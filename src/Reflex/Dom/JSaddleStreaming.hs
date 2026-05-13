{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE ScopedTypeVariables #-}
-- | Four FRP-shaping primitives over typed servant streaming calls.
--
-- Each function takes a 'ClientEnv' and a 'ClientM (SourceIO v)' — the
-- exact shape produced by 'client (Proxy @api)' for a streaming
-- endpoint — and bridges its values into a Reflex shape:
--
--   * 'performStreamEvent'         — 'Event t v', one firing per chunk
--   * 'performStreamCollected'     — 'Event t (Maybe [v])', once at end
--   * 'performStreamProgress'      — 'Dynamic t (StreamObjectProgress v)'
--   * 'performStreamAccumulating'  — 'Dynamic t [v]'
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
  , emptyProgress

    -- * Patterns
  , performStreamEvent
  , performStreamCollected
  , performStreamProgress
  , performStreamAccumulating
  ) where

import Control.Concurrent          (forkIO)
import Control.Monad               (void)
import Control.Monad.Fix           (MonadFix)
import Control.Monad.IO.Class      (liftIO)
import Data.IORef                  (modifyIORef', newIORef, readIORef)
import Data.Text                   (Text)
import qualified Data.Text         as T
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
      Right source -> drainSource fire source `andThen` fire StreamFin
  where
    andThen io after = io >> after

-- | Step through a 'SourceT IO' once and fire the callback per Yield.
-- 'Error' steps end the drain; 'Stop' ends it cleanly. The caller is
-- responsible for emitting 'StreamFin' / 'StreamErr' around this.
drainSource :: (StreamMsg v -> IO ()) -> ST.SourceT IO v -> IO ()
drainSource fire (ST.SourceT k) = k go
  where
    go = \case
      ST.Stop        -> pure ()
      ST.Error e     -> fire (StreamErr (T.pack e))
      ST.Skip s      -> go s
      ST.Effect ms   -> ms >>= go
      ST.Yield v s   -> fire (StreamVal v) >> go s

-- ────────────────────────────────────────────────────────────────────
-- Pattern 1 — per-value Event
-- ────────────────────────────────────────────────────────────────────

-- | Fire one Reflex 'Event' per value the stream produces.
-- Lifecycle events (done / error) are silently dropped — reach for
-- 'performStreamProgress' if you care about them.
performStreamEvent
  :: ( PerformEvent t m
     , TriggerEvent t m
     , MonadJSM (Performable m)
     )
  => ClientEnv
  -> ClientM (ST.SourceT IO v)   -- ^ typed servant streaming call
  -> Event t a                 -- ^ trigger
  -> m (Event t v)
performStreamEvent env action trigE = do
  (vE, fireV) <- newTriggerEvent
  performEvent_ $ ffor trigE $ \_ -> liftJSM $
    withStream env action $ \case
      StreamVal v -> fireV v
      _           -> pure ()
  pure vE

-- ────────────────────────────────────────────────────────────────────
-- Pattern 2 — one-shot complete-list Event
-- ────────────────────────────────────────────────────────────────────

-- | Buffer values until the stream completes, then fire exactly one
-- Reflex 'Event' with the full list. 'Nothing' indicates an error.
performStreamCollected
  :: ( PerformEvent t m
     , TriggerEvent t m
     , MonadJSM (Performable m)
     )
  => ClientEnv
  -> ClientM (ST.SourceT IO v)
  -> Event t a
  -> m (Event t (Maybe [v]))
performStreamCollected env action trigE =
  performEventAsync $ ffor trigE $ \_ fire -> liftJSM $ do
    bufRef <- liftIO (newIORef [])
    withStream env action $ \case
      StreamVal v -> modifyIORef' bufRef (v:)
      StreamFin   -> do
        buf <- readIORef bufRef
        fire (Just (reverse buf))
      StreamErr _ -> fire Nothing

-- ────────────────────────────────────────────────────────────────────
-- Pattern 3 — live Dynamic of accumulated state
-- ────────────────────────────────────────────────────────────────────

-- Internal: fold a stream of StreamMsg into the progress record.
data ProgressEv v
  = PEVal !v
  | PEFin
  | PEErr !Text

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
  (evE, fire) <- newTriggerEvent
  performEvent_ $ ffor trigE $ \_ -> liftJSM $
    withStream env action $ \case
      StreamVal v -> fire (PEVal v)
      StreamFin   -> fire PEFin
      StreamErr e -> fire (PEErr e)
  let step (PEVal v) sop = sop { _sop_messages = _sop_messages sop ++ [v] }
      step PEFin     sop = sop { _sop_status = Done }
      step (PEErr e) sop = sop { _sop_status = Errored e }
  foldDyn step emptyProgress evE

-- ────────────────────────────────────────────────────────────────────
-- Pattern 4 — accumulating Dynamic [v]
-- ────────────────────────────────────────────────────────────────────

-- | Cumulative list of values. Lifecycle is ignored; use
-- 'performStreamProgress' if you need it.
performStreamAccumulating
  :: ( PerformEvent t m
     , TriggerEvent t m
     , MonadHold t m
     , MonadFix m
     , MonadJSM (Performable m)
     )
  => ClientEnv
  -> ClientM (ST.SourceT IO v)
  -> Event t a
  -> m (Dynamic t [v])
performStreamAccumulating env action trigE = do
  vE <- performStreamEvent env action trigE
  foldDyn (\v acc -> acc ++ [v]) [] vE
