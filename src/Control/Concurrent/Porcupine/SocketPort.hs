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
import qualified Data.Sequence as S
import qualified Data.Text as T
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.Binary as B
import qualified Network.Socket as NS
import qualified Network.Socket.ByteString as NSB
import Control.Monad.IO.Class (MonadIO (..))
import Control.Exception.Base (SomeException,
                               Exception (..),
                               AsyncException (..),
                               IOException (..),
                               catch)
import Control.Concurrent.Porcupine.Process (Key)
import Data.Word (Word32)
import Data.Functor ((<$>))
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
                    spsRegistered :: S.Seq P.DestId }

-- | Socket listener state
data SocketListenerState =
  SocketListenerState { slsListening :: Bool,
                        slsSocket :: NS.Socket,
                        slsListenPid :: P.ProcessId,
                        slsSockAddr :: NS.SockAddr,
                        sksKey :: P.Key }

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
  sendPid <- P.spawnListenEnd' $ startSendProcess myPid socket key
  receivePid <- P.spawnListenEnd' $ startReceiveProcess myPid socket key
  runSocketPort $ SocketPortState { spsSendPid = sendPid,
                                    spsReceivePid = receivePid,
                                    spsRegistered = S.empty }

-- | Run the main loop of a socket port's main process
runSocketPort :: SocketPortState -> P.Proces ()
runSocketPort state =
  state <- P.receive [handleSendEnd, handleReceiveEnd,
                      handleRegister, handleUnregister,
                      handleIncoming, handleOutgoing]
  runSocketPort state
  where handleSendEnd

-- | Handle closing a socket after handling an exception
withSocket :: NS.Socket -> P.Process a -> P.Process a
withSocket socket action = do
  P.catch action $ \e -> do
    liftIO $ NS.close socket
    E.throw e

-- | Decode data from a strict ByteString.
decode :: B.Binary a => BS.ByteString -> a
decode = B.decode . BSL.fromStrict

-- | Encode data to a strict ByteString.
encode :: B.Binary a => a -> BS.ByteString
encode = BSL.toStrict . B.encode

-- | Get family of SockAddr
familyOfSockAddr :: NS.SockAddr -> NS.Family
familyOfSockAddr (NS.SockAddrInet _ _) = NS.AF_INET
familyOfSockAddr (NS.SockAddrInet6 _ _ _ _) = NS.AF_INET6
familyOfSockAddr (NS.SockAddrUnix _) = NS.AF_UNIX
familyOfSockAddr _ = error "not supported"
