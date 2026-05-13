{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE ScopedTypeVariables #-}
-- | Four FRP-shaping primitives over
-- 'Servant.Client.JSaddle.Streaming.sendStreamingRequest'. Each takes
-- a 'ClientEnv', a servant 'Request', a per-line parser
-- @(ByteString -> Maybe v)@, and a trigger 'Event'. They share one
-- assumption: the wire is newline-framed (matches servant's
-- 'NewlineFraming'). Each complete @\\n@-terminated line is fed to the
-- parser; the typed @v@ then flows through whichever Reflex shape the
-- pattern names.
--
-- The four patterns:
--
--   * 'performMessageEvent'      — 'Event t v', one firing per message.
--   * 'performCollectedMessages' — 'Event t (Maybe [v])', one firing
--                                  at stream end with the full list.
--   * 'performMessageProgress'   — 'Dynamic t (StreamObjectProgress v)',
--                                  cumulative state + lifecycle.
--   * 'performParsedStream'      — 'Dynamic t [v]', cumulative list.
--
-- Use 'Just' as the parser to get raw lines as 'ByteString'.
module Reflex.Dom.JSaddleStreaming
  ( -- * Accumulated-state types (used by 'performMessageProgress')
    StreamObjectProgress (..)
  , StreamStatus (..)
  , emptyProgress

    -- * Patterns
  , performMessageEvent
  , performCollectedMessages
  , performMessageProgress
  , performParsedStream
  ) where

import Control.Monad (forM_, unless)
import Control.Monad.Fix (MonadFix)
import Control.Monad.IO.Class (liftIO)
import qualified Data.ByteString as BS
import Data.IORef (modifyIORef', newIORef, readIORef, writeIORef)
import qualified Data.Maybe as Maybe
import Data.Text (Text)
import Language.Javascript.JSaddle (MonadJSM, liftJSM)
import Reflex hiding (Request)
import Servant.Client.Core (Request)
import Servant.Client.JSaddle (ClientEnv)
import Servant.Client.JSaddle.Streaming
  (StreamEvent (..), sendStreamingRequest)

-- ────────────────────────────────────────────────────────────────────
-- Accumulated-state types
-- ────────────────────────────────────────────────────────────────────

data StreamStatus
  = Loading
  | Done
  | Errored !Text
  deriving (Show, Eq)

data StreamObjectProgress v = StreamObjectProgress
  { _sop_messages   :: ![v]
  , _sop_httpStatus :: !(Maybe Word)
  , _sop_status     :: !StreamStatus
  } deriving (Show, Eq)

emptyProgress :: StreamObjectProgress v
emptyProgress = StreamObjectProgress [] Nothing Loading

-- ────────────────────────────────────────────────────────────────────
-- Pattern 1 — per-message Event
-- ────────────────────────────────────────────────────────────────────

-- | Fire one Reflex 'Event' per parsed message. Lines that fail to
-- parse are silently dropped. Lifecycle events (done / error) are not
-- exposed — reach for 'performMessageProgress' if you care about them.
performMessageEvent
  :: ( PerformEvent t m
     , TriggerEvent t m
     , MonadJSM (Performable m)
     )
  => ClientEnv
  -> Request
  -> (BS.ByteString -> Maybe v)  -- ^ per-line parser
  -> Event t a                   -- ^ trigger
  -> m (Event t v)
performMessageEvent env req parser trigE = do
  (vE, fireV) <- newTriggerEvent
  performEvent_ $ ffor trigE $ \_ -> liftJSM $ do
    bufRef <- liftIO (newIORef BS.empty)
    sendStreamingRequest env req $ \case
      StreamChunk c -> liftIO $ do
        buf <- readIORef bufRef
        let (newBuf, lines_) = newlineSplit buf c
        writeIORef bufRef newBuf
        forM_ lines_ $ \line -> case parser line of
          Just v  -> fireV v
          Nothing -> pure ()
      _ -> pure ()
  pure vE

-- ────────────────────────────────────────────────────────────────────
-- Pattern 2 — one-shot complete list via performEventAsync
-- ────────────────────────────────────────────────────────────────────

-- | Buffer messages until the stream completes, then fire exactly one
-- Reflex 'Event' with the full list. 'Nothing' means network error.
performCollectedMessages
  :: ( PerformEvent t m
     , TriggerEvent t m
     , MonadJSM (Performable m)
     )
  => ClientEnv
  -> Request
  -> (BS.ByteString -> Maybe v)
  -> Event t a
  -> m (Event t (Maybe [v]))
performCollectedMessages env req parser trigE =
  performEventAsync $ ffor trigE $ \_ fire -> liftJSM $ do
    bufRef  <- liftIO (newIORef BS.empty)
    msgsRef <- liftIO (newIORef ([] :: [v]))
    sendStreamingRequest env req $ \case
      StreamChunk c -> liftIO $ do
        buf <- readIORef bufRef
        let (newBuf, lines_) = newlineSplit buf c
            parsed           = Maybe.mapMaybe parser lines_
        writeIORef bufRef newBuf
        modifyIORef' msgsRef (<> parsed)
      StreamDone _ -> liftIO $ do
        msgs <- readIORef msgsRef
        fire (Just msgs)
      StreamFail _ -> liftIO (fire Nothing)

-- ────────────────────────────────────────────────────────────────────
-- Pattern 3 — live Dynamic of accumulated state
-- ────────────────────────────────────────────────────────────────────

-- Internal: split the lifecycle into a single sum so we can foldDyn
-- over it cleanly.
data ProgressEvent v
  = PEMessages ![v]
  | PEDone     !Word
  | PEFailed   !Text

-- | foldDyn over StreamEvents into a 'StreamObjectProgress v'. The
-- Dynamic ticks once per parsed message and once per lifecycle
-- transition.
performMessageProgress
  :: ( PerformEvent t m
     , TriggerEvent t m
     , MonadHold t m
     , MonadFix m
     , MonadJSM (Performable m)
     )
  => ClientEnv
  -> Request
  -> (BS.ByteString -> Maybe v)
  -> Event t a
  -> m (Dynamic t (StreamObjectProgress v))
performMessageProgress env req parser trigE = do
  (evtE, fire) <- newTriggerEvent
  performEvent_ $ ffor trigE $ \_ -> liftJSM $ do
    bufRef <- liftIO (newIORef BS.empty)
    sendStreamingRequest env req $ \case
      StreamChunk c -> liftIO $ do
        buf <- readIORef bufRef
        let (newBuf, lines_) = newlineSplit buf c
            parsed           = Maybe.mapMaybe parser lines_
        writeIORef bufRef newBuf
        unless (null parsed) $ fire (PEMessages parsed)
      StreamDone s -> liftIO (fire (PEDone (fromIntegral s)))
      StreamFail e -> liftIO (fire (PEFailed e))
  let step (PEMessages msgs) sop =
        sop { _sop_messages = _sop_messages sop <> msgs }
      step (PEDone s) sop =
        sop { _sop_httpStatus = Just s, _sop_status = Done }
      step (PEFailed e) sop =
        sop { _sop_status = Errored e }
  foldDyn step emptyProgress evtE

-- ────────────────────────────────────────────────────────────────────
-- Pattern 4 — accumulating Dynamic [v]
-- ────────────────────────────────────────────────────────────────────

-- | Cumulative list of parsed messages. Equivalent to
-- @fmap _sop_messages . performMessageProgress@ for callers that don't
-- need the lifecycle status.
performParsedStream
  :: forall t m v a.
     ( PerformEvent t m
     , TriggerEvent t m
     , MonadHold t m
     , MonadFix m
     , MonadJSM (Performable m)
     )
  => ClientEnv
  -> Request
  -> (BS.ByteString -> Maybe v)
  -> Event t a
  -> m (Dynamic t [v])
performParsedStream env req parser trigE = do
  msgE <- performMessageEvent env req parser trigE
  foldDyn (\v acc -> acc ++ [v]) [] msgE

-- ────────────────────────────────────────────────────────────────────
-- Internal helper
-- ────────────────────────────────────────────────────────────────────

-- | Append @chunk@ to @buf@, split off complete (newline-terminated)
-- lines, return @(remaining-partial-line, complete-lines)@.
newlineSplit :: BS.ByteString -> BS.ByteString -> (BS.ByteString, [BS.ByteString])
newlineSplit buf chunk =
  let combined = buf <> chunk
      pieces   = BS.split 10 combined
  in case reverse pieces of
       []       -> (BS.empty, [])
       lst:rev  -> (lst, reverse rev)
