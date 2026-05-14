{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings   #-}
-- | Four FRP-shaping primitives over typed servant streaming calls.
--
-- Each function takes a 'ClientEnv' and a 'ClientM (SourceIO v)' — the
-- exact shape produced by 'client (Proxy @api)' for a streaming
-- endpoint — and bridges its values into a Reflex shape:
--
--   * 'performStreamEvent'         — 'Event t v', one firing per chunk
--   * 'performStreamProgress'      — 'Dynamic t (StreamObjectProgress v)'
--
-- All four delegate framing + decoding to servant — the API type
-- (its 'FramingUnrender' + 'MimeUnrender' instances) determines how
-- bytes become @v@s. The Reflex bridge stays out of the parsing.
--
-- Implementation: each trigger fork a thread that calls 'runClientM'
-- (which, via servant-jsaddle's progressive @RunStreamingClient ClientM@
-- instance, returns a 'SourceIO v' backed by a bounded queue of XHR
-- chunks). The thread drains the source, firing per-Yield callbacks
-- into Reflex via 'newTriggerEvent'. Stays off the widget event loop.
module Reflex.Dom.JSaddleStreaming
  ( -- * State types (used by 'performStreamProgress')
    StreamObjectProgress (..)
  , StreamStatus (..)
  , StreamMsg(..)
  , emptyProgress

    -- * Patterns
  , performStreamEvent
  , performStreamProgress

    -- * Lower-level primitive
  , withStream
  ) where

import qualified Data.Text                 as T
import Control.Concurrent          (forkIO)
import Control.Monad               (void)
import Control.Monad.Fix           (MonadFix)
import Control.Monad.IO.Class      (liftIO)
import Data.Text                   (Text)
import qualified GHCJS.DOM.Types   as JS
import Language.Javascript.JSaddle (MonadJSM, liftJSM)
import Reflex
import Servant.Client.JSaddle      (ClientEnv, ClientM, runClientM)
-- Re-imported for its orphan 'RunStreamingClient ClientM' instance —
-- without this import the typed streaming client falls back to no
-- instance and fails to typecheck downstream.
import Servant.Client.JSaddle.Streaming ()
import qualified Servant.Types.SourceT as ST

-- ────────────────────────────────────────────────────────────────────
-- Accumulated-state types
-- ────────────────────────────────────────────────────────────────────

data StreamStatus
  = Loading                    -- ^ Stream open, values arriving.
  | Done                       -- ^ Stream completed cleanly.
  | Errored !Text              -- ^ Network / decode error.
  deriving (Show, Eq)

data StreamObjectProgress v = StreamObjectProgress
  { _sop_messages :: ![v]
  , _sop_status   :: !StreamStatus
  } deriving (Show, Eq)

emptyProgress :: StreamObjectProgress v
emptyProgress = StreamObjectProgress [] Loading

-- ────────────────────────────────────────────────────────────────────
-- Internal: drain a typed servant stream into a callback
-- ────────────────────────────────────────────────────────────────────

-- | Lifecycle events delivered by 'withStream' to its callback.
data StreamMsg v
  = StreamVal !v        -- ^ One value from the stream.
  | StreamFin           -- ^ Stream completed cleanly.
  | StreamErr !Text     -- ^ Error from servant client or mid-stream.
  deriving Show
  
-- | Fork a thread that runs a typed servant streaming action and
-- drains the resulting 'SourceIO', invoking the callback once per
-- 'Yield' plus a final 'StreamFin' or 'StreamErr'.
--
-- The callback runs in IO on the forked thread; use it to fire
-- Reflex 'newTriggerEvent' callbacks or write to refs.
withStream
  :: ClientEnv
  -> ClientM (ST.SourceT IO v)
  -> (StreamMsg v -> IO ())
  -> JS.DOM ()
withStream env action fire = do
  domc <- JS.askDOM
  liftIO . void . forkIO $ do
    result <- flip JS.runDOM domc $ runClientM action env
    case result of
      Left err     -> fire (StreamErr (T.pack (show err)))
      Right source -> drainSource fire source

-- | Step through a 'SourceT IO' and fire the callback per step:
-- 'StreamVal' per 'Yield', 'StreamFin' on clean 'Stop', 'StreamErr'
-- on 'Error'. Skip / Effect steps are pumped silently.
drainSource :: (StreamMsg v -> IO ()) -> ST.SourceT IO v -> IO ()
drainSource fire (ST.SourceT k) = k go
  where
    go = \case
      ST.Stop        -> fire StreamFin
      ST.Error e     -> fire (StreamErr (T.pack e))
      ST.Skip s      -> go s
      ST.Effect ms   -> ms >>= go
      ST.Yield v s   -> fire (StreamVal v) >> go s

-- ────────────────────────────────────────────────────────────────────
-- Pattern 1 — per-value Event
-- ────────────────────────────────────────────────────────────────────

-- | Fire one Reflex 'Event' per 'StreamMsg' the stream produces:
-- 'StreamVal' per 'Yield', 'StreamErr' on client-side failure, and
-- 'StreamFin' once the source has been fully drained.
performStreamEvent
  :: ( PerformEvent t m
     , TriggerEvent t m
     , MonadJSM (Performable m)
     )
  => ClientEnv
  -> ClientM (ST.SourceT IO v)
  -- ^ typed servant streaming call
  -> Event t a                 -- ^ trigger
  -> m (Event t (StreamMsg v))
performStreamEvent env action trigE = do
  (vE, fireV) <- newTriggerEvent
  -- The drain is forked off Reflex's propagation thread: 'drainSource'
  -- blocks on each chunk pull, and if we ran it inside 'performEvent_'
  -- directly Reflex couldn't process the per-Yield 'fireV' triggers
  -- until the whole stream completed — DOM updates would batch at end.
  -- The DOMContext is captured at fire time so the forked thread can
  -- re-enter JSM to run the typed client and the source's pulls.
  performEvent_ $ ffor trigE $ \_ -> do
    domc <- liftJSM JS.askDOM
    liftIO . void . forkIO $ do
      result <- flip JS.runDOM domc $ runClientM action env
      case result of
        Left err     -> fireV (StreamErr (T.pack (show err)))
        Right source -> drainSource fireV source
  pure vE

-- ────────────────────────────────────────────────────────────────────
-- Pattern 3 — live Dynamic of accumulated state
-- ────────────────────────────────────────────────────────────────────

-- | Fold a 'performStreamEvent' into a live 'StreamObjectProgress':
-- values accumulate into '_sop_messages' as they arrive, and
-- '_sop_status' tracks Loading → Done / Errored over the lifecycle.
performStreamProgress
  :: ( PerformEvent t m
     , TriggerEvent t m
     , MonadHold t m
     , MonadFix m
     , MonadJSM (Performable m)
     )
  => ClientEnv
  -> ClientM (ST.SourceT IO v)
  -> Event t a
  -> m (Dynamic t (StreamObjectProgress v))
performStreamProgress env action trigE = do
  msgE <- performStreamEvent env action trigE
  foldDyn step emptyProgress msgE
  where
    step (StreamVal v) sop = sop { _sop_messages = _sop_messages sop ++ [v] }
    step StreamFin     sop = sop { _sop_status   = Done }
    step (StreamErr e) sop = sop { _sop_status   = Errored e }

