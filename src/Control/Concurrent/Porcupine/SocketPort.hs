-- Copyright (c) 2018, Travis Bemann
-- All rights reserved.
-- 
-- Redistribution and use in source and binary forms, with or without
-- modification, are permitted provided that the following conditions are met:
-- 
-- o Redistributions of source code must retain the above copyright notice, this
--   list of conditions and the following disclaimer.
-- 
-- o Redistributions in binary form must reproduce the above copyright notice,
--   this list of conditions and the following disclaimer in the documentation
--   and/or other materials provided with the distribution.
-- 
-- o Neither the name of the copyright holder nor the names of its
--   contributors may be used to endorse or promote products derived from
--   this software without specific prior written permission.
-- 
-- THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
-- AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
-- IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
-- ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
-- LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
-- CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
-- SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
-- INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
-- CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
-- ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
-- POSSIBILITY OF SUCH DAMAGE.

{-# LANGUAGE OverloadedStrings, OverloadedLists, RecordWildCards,
             DeriveGeneric, MultiParamTypeClasses,
             GeneralizedNewtypeDeriving #-}

module Control.Concurrent.Porcupine.SocketPort

  (SocketPort,
   SocketListener,
   Key,
   connect,
   disconnect,
   startListen,
   stopListen,
   registerPort,
   unregisterPort,
   registerListener,
   unregisterListener,
   subscribe,
   unsubscribe,
   assign,
   unassign,
   listenEnd,
   unlistenEnd)

where

import qualified Control.Concurrent.Porcupine.Process as P
import qualified Control.Concurrent.Porcupine.Utility as U
import qualified Data.Sequence as S
import qualified Data.Text as T
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.Binary as B
import qualified Data.HashMap.Strict as M
import qualified Network.Socket as NS
import qualified Network.Socket.ByteString as NSB
import Control.Monad (forM_)
import Control.Monad.IO.Class (MonadIO (..))
import Control.Exception.Base (SomeException,
                               Exception (..),
                               AsyncException (..),
                               IOException (..),
                               catch)
import Control.Concurrent.Porcupine.Process (Key)
import Data.Word (Word32,
                  Word64)
import Data.Functor ((<$>))
import Data.Monoid ((<>),
                    mconcat)
import Text.Printf (printf)

-- | Socket port type
newtyle SocketPort = SocketPort P.ProcessId
  deriving (Eq, Ord, B.Binary)

-- | Socket port type Show instance
instance Show SocketPort where
  show (SocketPort pid) = printf "socketPort:%s" $ show pid

-- | Socket listener type
newtyle SocketListener = SocketListener P.ProcessId
  deriving (Eq, Ord, B.Binary)

-- | Socket listener type Show instance
instance Show SocketListen where
  show (SocketListen pid) = printf "socketListener:%s" $ show pid

-- | Socket port state
data SocketPortState =
  SocketPortState { spsSendPid :: P.ProcessId,
                    spsReceivePid :: P.ProcessId,
                    spsSocket :: NS.Socket,
                    spsSendStopped :: Bool,
                    spsReceiveStopped :: Bool,
                    spsRegistered :: M.HashMap P.DestId Integer }

-- | Socket listener state
data SocketListenerState =
  SocketListenerState { slsSocket :: NS.Socket,
                        slsListenPid :: P.ProcessId,
                        slsRegistered :: M.HashMap P.DestId Integer }

-- | The magic value
magicValue :: Word32
magicValue = 0xF000BAA4

-- | Connect to a socket listener.
connect :: NS.SockAddr -> P.Key -> P.Process SocketPort
connect sockAddr key =
  SocketPort <$> (P.spawn' $ startSocketPortWithoutSocket sockAddr key)

-- | Start a socket port without an existing connection
startSocketPortWithoutSocket :: NS.SockAddr -> P.Key -> P.Process ()
startSocketPortWithoutSocket sockAddr key = do
  socket <- liftIO $ NS.socket (familyOfSockAddr sockAddr) NS.Stream
            NS.defaultProtocol
  withSocket socket . liftIO $ NS.connect socket sockAddr
  startSocketPort socket key

-- | Actually start a socket port
startSocketPort :: NS.Socket -> P.Key -> P.Process ()
startSocketPort socket key = do
  myPid <- P.myProcessId
  sendPid <- P.spawnListenEnd' $ startSendProcess socket key
  receivePid <- P.spawnListenEnd' $ startReceiveProcess myPid socket key
  runSocketPort $ SocketPortState { spsSendPid = sendPid,
                                    spsReceivePid = receivePid,
                                    spsSocket = socket,
                                    spsSendStopped = False,
                                    spsReceiveStopped = False,
                                    spsRegistered = S.empty }

-- | Run the main loop of a socket port's main process
runSocketPort :: SocketPortState -> P.Proces ()
runSocketPort state =
  state <- P.receive [handleSendEnd state, handleReceiveEnd state,
                      handleRegister state, handleUnregister state,
                      handleIncoming state, handleOutgoing state]
  runSocketPort state

-- | Handle send process end
handleSendEnd :: SocketPortState -> P.SourceId -> P.DestId -> P.Header ->
                 P.Payload -> Maybe (P.Process SocketPortState)
handleSendEnd state sid _ header _
  | U.processIdOfSourceId sid == (Just $ spsSendPid state)
    && U.isEnd header =
      Just $ do
        if not $ receiveStopped state
          then do
            NS.shutdown (spsSocket state) NS.ShutdownBoth
            P.kill' $ spsReceivePid state
            return $ state { sendStopped = True }
          else do
            NS.close $ spsSocket state
            P.quit'
            return state
  | True = Nothing

-- | Handle receive process end
handleReceiveEnd :: SocketPortState -> P.SourceId -> P.DestId -> P.Header ->
                    P.Payload -> Maybe (P.Process SocketPortState)
handleReceiveEnd state sid _ header _
  | U.processIdOfSourceId sid == (Just $ spsReceivePid state)
    && U.isEnd header =
      Just $ do
        if not $ sendStopped state
          then do
            NS.shutdown (spsSocket state) NS.ShutdownBoth
            P.kill' $ spsSendPid state
            return $ state { receiveStopped = True }
          else do
            NS.close $ spsSocket state
            P.quit'
            return state
  | True = Nothing

-- | Handle socket port registration
handleRegister :: SocketPortState -> P.SourceId -> P.DestId -> P.Header ->
                  P.Payload -> Maybe (P.Process SocketPortState)
handleRegister state _ _ header payload
  | header == registerHeader =
      Just $ do
        case U.tryDecode payload of
          Right did ->
            case M.lookup did $ spsRegistered state of
              Nothing ->
                return $ state { spsRegistered =
                                   M.insert did 1 $ spsRegistered state }
              Just _ -> do
                return $
                  state { spsRegistered =
                            M.adjust (+1) did $ spsRegistered state }
          Left _ -> return state
  | True = Nothing

-- | Handle socket port unregistration
handleUnRegister :: SocketPortState -> P.SourceId -> P.DestId -> P.Header ->
                    P.Payload -> Maybe (P.Process SocketPortState)
handleUnregister state _ _ header payload
  | header == unregisterHeader =
      Just $ do
        case U.tryDecode payload of
          Right did ->
            case M.lookup did $ spsRegistered state of
              Just 1 -> do
                return $ state { spsRegistered =
                                 M.delete did $ spsRegistered state}
              Just _ -> do
                return $
                  state { spsRegistered =
                            M.adjust (subtract 1) $ spsRegistered state }
              Nothing -> return state
          Left _ -> return state
  | True = Nothing

-- | Handle incoming messages
handleIncoming :: SocketPortState -> P.SourceId -> P.DestId -> P.Header ->
                  P.Payload -> Maybe (P.Process SocketPortState)
handleIncoming sid _ header payload =
  | U.processIdOfSourceId sid == (Just $ spsReceivePid state)
    && header == receiveRemoteHeader =
      Just $ do
        case U.tryDecode payload of
          Right container -> do
            forM_ (M.keys $ spsRegistered state) $ \did -> do
              P.send did (P.mcontHeader container) (P.mcontPayload container)
            return state
          Left _ -> return state
  | True = Nothing

-- | Handle outgoing messages
handleOutgoing :: SocketPortState -> P.SourceId -> P.DestId -> P.Header ->
                  P.Payload -> Maybe (P.Process SocketPortState)
handleOutgoing state sid _ header payload =
  | U.processIdOfSourceId sid /= (Just $ spsSendPid state)
    && U.processIdOfSourceId sid /= (Just $ spsReceivePid state) =
      Just $ do
        if not $ spsSendStopped state
          then do
            let payload' = U.encode $ P.MessageContainer header payload
            P.send' (P.ProcessDest $ spsSendPid state) sendRemoteHeader
              payload'
          else return ()
        return state
  | True = Nothing

-- | Start send process
startSendProcess :: NS.Socket -> P.Key -> P.Process ()
startSendProcess socket key = do
  let key' = encode key
      bytes = mconcat [U.encode magicValue,
                       U.encode $ (fromIntegral $ BS.length key' :: Word64),
                       key']
  P.handle (\e -> const P.quit' (e :: IOException)) $ do
    liftIO $ NSB.sendAll socket bytes
  runSendProcess socket

-- | Run send process
runSendProcess :: NS.Socket -> P.Process ()
runSendProcess socket = do
  receive $ handleSendRemote socket
  runSendProcess

-- | Handle send remote message
handleSendRemote :: NS.Socket -> P.SourceId -> P.DestId -> P.Header ->
                    P.Payload -> Maybe (P.Process ())
handleSendRemote socket sid _ header payload
  | U.processIdOfSourceId sid == Just parentPid
    && header == sendRemoteHeader =
      Just $ do
        case U.tryDecode payload of
          Right container -> do
            let bytes = U.encode (container :: P.MessageContainer)
                lengthField =
                  U.encode (fromIntegral $ BS.length bytes :: Word64)
                bytes' = lengthField <> bytes
            P.handle (\e -> const P.quit' (e :: IOException))
              (liftIO $ NSB.sendAll socket bytes')
          Left -> return ()
  | True = return ()

-- | Start receive process
startReceiveProcess :: P.ProcessId -> NS.Socket -> P.Key -> P.Process ()
startReceiveProcess parentPid socket key = do
  (magicField, rest) <- receiveBytes socket 4 BS.empty
  case U.tryDecode magicField :: Either T.Text Word32 of
    Left _ -> P.quit'
    Right magicField
      | magicField /= magicValue -> P.quit'
      | True -> do
          (keyLength, rest) <- receiveBytes socket 8 rest
          case U.tryDecode keyLength :: Either T.Text Word64 of
            Left _ -> P.quit'
            Right keyLength -> do
              (incomingKey, rest) <- receiveBytes socket keyLength rest
              case U.tryDecode incomingKey :: Either T.Text P.Key of
                Left _ -> P.quit'
                Right incomingKey
                  | U.decode incomingKey /= key -> P.quit'
                  | True -> runReceiveProcess parentPid socket rest

-- | Run receive proces
runReceiveProcess :: P.ProcessId -> NS.Socket -> BS.ByteString -> P.Process ()
runReceiveProcess parentPid socket buffer = do
  (messageLength, rest) <- receiveBytes socket 8 buffer
  case U.tryDecode messageLength :: Either T.Text Word64 of
    Left _ -> P.quit'
    Right messageLength ->
      (message, rest) <- receiveBytes socket messageLength rest
      case U.tryDecode message :: Either T.Text P.MessageContainer of
        Left _ -> P.quit'
        Right _ -> do
          P.send (ProcessDest parentPid) receiveRemoteHeader message
          runReceiveProcess parentPid socket rest
  
-- | Receive a minimum number of bytes
receiveBytes :: NS.Socket -> Word64 -> BS.ByteString ->
                P.Process (BS.ByteString, BS.ByteString)
receiveBytes socket length bytes =
  if BS.length >= fromIntegral length
  then return $ BS.splitAt (fromIntegral length) bytes
  else do
    newBytes <- P.catch (liftIO $ NSB.recv socket 4096)
                (\e -> const (do { P.quit'; return BS.empty })
                       (e :: IOException))
    if BS.length newBytes > 0
    then receiveBytes socket length $ bytes <> newBytes
    else do
      P.quit'
      return BS.empty
    
-- | Register header
registerHeader :: P.Header
registerHeader = encode ("socketPortRegister" :: T.Text)

-- | Unregister header
unregisterHeader :: P.Header
unregisterHeader = encode ("socketPortUnregister" :: T.Text)

-- | Send remote header
sendRemoteHeader :: P.Header
sendRemoteHeader = encode ("sendRemote" :: T.Text)

-- | Receive remote header
receiveRemoteHeader :: P.Header
receiveRemoteHeader = encode ("receiveRemote" :: T.Text)

-- | Handle closing a socket after handling an exception
withSocket :: NS.Socket -> P.Process a -> P.Process a
withSocket socket action = do
  P.catch action $ \e -> do
    liftIO $ NS.close socket
    E.throw e

-- | Get family of SockAddr
familyOfSockAddr :: NS.SockAddr -> NS.Family
familyOfSockAddr (NS.SockAddrInet _ _) = NS.AF_INET
familyOfSockAddr (NS.SockAddrInet6 _ _ _ _) = NS.AF_INET6
familyOfSockAddr (NS.SockAddrUnix _) = NS.AF_UNIX
familyOfSockAddr _ = error "not supported"
