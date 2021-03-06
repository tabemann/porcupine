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

module Control.Concurrent.Porcupine.FreeSocketPort

  (SocketPort,
   SocketListener,
   Key,
   connect,
   disconnect,
   listen,
   unlisten,
   registerListener,
   unregisterListener,
   addAutoEndListener,
   removeAutoEndListener,
   send,
   sendWithUniqueId,
   lookup,
   tryLookup,
   subscribe,
   unsubscribe,
   assign,
   unassign,
   listenEnd,
   listenEndAsProxy,
   unlistenEnd,
   unlistenEndAsProxy,
   lookupRemote,
   tryLookupRemote,
   asyncLookupRemote,
   asyncTryLookupRemote,
   isEnd,
   isNormalEnd,
   isFail,
   accept,
   handleAsyncLookupRemoteResponse,
   listenerLookup,
   listenerTryLookup,
   listenerSubscribe,
   listenerUnsubscribe,
   listenerAssign,
   listenerUnassign,
   listenerListenEnd,
   listenerListenEndAsProxy,
   listenerUnlistenEnd,
   listenerUnlistenEndAsProxy,
   listenerIsEnd,
   listenerIsNormalEnd,
   listenerIsFail)

where

import qualified Control.Concurrent.Porcupine.Process as P
import qualified Control.Concurrent.Porcupine.Utility as U
import qualified Data.Sequence as S
import qualified Data.Text as T
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.Binary as B
import Data.Binary.Get (Get)
import Data.Binary.Put (Put)
import qualified Data.HashMap.Strict as M
import qualified Network.Socket as NS
import qualified Network.Socket.ByteString as NSB
import Data.Sequence ((|>))
import Data.Foldable (foldl')
import Control.Monad (forM,
                      forM_,
                      when)
import Control.Monad.Fail (fail)
import Control.Monad.IO.Class (MonadIO (..))
import Control.Exception.Base (SomeException,
                               Exception (..),
                               AsyncException (..),
                               IOException (..),
                               catch,
                               throw)
import Control.Concurrent.Porcupine.Process (Key)
import Data.Hashable (Hashable(..))
import Data.Word (Word8,
                  Word32,
                  Word64)
import Data.Functor ((<$>))
import Data.Foldable (toList)
import Data.Monoid ((<>),
                    mconcat)
import Text.Printf (printf)
import Prelude hiding (lookup,
                       fail)
import Debug.Trace (trace)

-- | Socket port type
newtype SocketPort = SocketPort P.ProcessId
                   deriving (Eq, Ord, B.Binary, Hashable)

-- | Socket port type Show instance
instance Show SocketPort where
  show (SocketPort pid) = printf "freeSocketPort:%s" $ show pid

-- | Socket listener type
newtype SocketListener = SocketListener P.ProcessId
                       deriving (Eq, Ord, B.Binary, Hashable)

-- | Socket listener type Show instance
instance Show SocketListener where
  show (SocketListener pid) = printf "freeSocketListener:%s" $ show pid

-- | Asynchronous remote lookup type
newtype AsyncLookupRemote = AsyncLookupRemote P.UniqueId
                          deriving (Eq, Ord, B.Binary, Hashable)

-- | Socket port state
data SocketPortState =
  SocketPortState { spsSendPid :: P.ProcessId,
                    spsReceivePid :: P.ProcessId,
                    spsSocket :: NS.Socket,
                    spsSendStopped :: Bool,
                    spsReceiveStopped :: Bool,
                    spsAssignment :: M.HashMap P.Name P.DestId,
                    spsWaitAssignment :: S.Seq (P.Name, P.DestId, P.UniqueId) }

-- | Socket listener state
data SocketListenerState =
  SocketListenerState { slsSocket :: NS.Socket,
                        slsListenPid :: P.ProcessId,
                        slsRegistered :: M.HashMap P.DestId Integer,
                        slsAutoEndListeners :: S.Seq P.DestId }

-- | Auto setup query response type
data AutoSetupResponse =
  AutoSetupResponse { asrAutoEndListeners :: S.Seq P.DestId }
  deriving (Eq, Ord)

-- | Auto setup query response type Binary instance
instance B.Binary AutoSetupResponse where
  put AutoSetupResponse{..} = do B.put asrAutoEndListeners
  get = do autoEndListeners <- B.get
           return $ AutoSetupResponse { asrAutoEndListeners =
                                          autoEndListeners }

-- | Free socket port message type
data SocketMessage = SocketUserMessage { sumMessage :: P.MessageContainer }
                   | SocketAssign { sassName :: P.Name,
                                    sassDestId :: P.DestId }
                   | SocketUnassign { suassName :: P.Name,
                                      suassDestId :: P.DestId }
                   deriving (Eq, Ord)

-- | Free socket port message type Binary instance
instance B.Binary SocketMessage where
  put SocketUserMessage{..} = do B.put (0 :: Word8)
                                 B.put sumMessage
  put SocketAssign{..} = do B.put (1 :: Word8)
                            B.put sassName
                            B.put sassDestId
  put SocketUnassign{..} = do B.put (2 :: Word8)
                              B.put suassName
                              B.put suassDestId
  get = do select <- B.get :: Get Word8
           case select of
             0 -> do message <- B.get
                     return $ SocketUserMessage { sumMessage = message }
             1 -> do name <- B.get
                     destId <- B.get
                     return $ SocketAssign { sassName = name,
                                             sassDestId = destId }
             2 -> do name <- B.get
                     destId <- B.get
                     return $ SocketUnassign { suassName = name,
                                               suassDestId = destId }
             _ -> fail "improper socket message encoding"

-- | The magic value
magicValue :: Word32
magicValue = 0xF000BAA5

-- | Do log or not
logActive :: Bool
logActive = False

-- | Log a message.
logMessage :: MonadIO a => String -> a ()
logMessage string = when logActive . liftIO $ putStr string

-- | Connect to a socket listener.
connect :: NS.SockAddr -> P.Key -> S.Seq P.DestId -> P.Process SocketPort
connect sockAddr key endListeners = do
  SocketPort <$> (P.spawnListenEnd' (startSocketPortWithoutSocket sockAddr key)
                  endListeners)

-- | Disconnect from a socket listener.
disconnect :: SocketPort -> P.Process ()
disconnect (SocketPort pid) = P.kill' $ P.ProcessDest pid

-- | Listen on a port.
listen :: NS.SockAddr -> P.Key -> S.Seq P.DestId -> S.Seq P.DestId ->
          S.Seq P.DestId -> P.Process SocketListener
listen sockAddr key registered endListeners autoEndListeners =
  SocketListener <$> (P.spawnListenEnd' (startListener sockAddr key registered
                                         autoEndListeners)
                      endListeners)

-- | Stop listening on a port.
unlisten :: SocketListener -> P.Process ()
unlisten (SocketListener pid) = P.kill' $ P.ProcessDest pid

-- | Register on a socket listener.
registerListener :: SocketListener -> P.DestId -> P.Process ()
registerListener (SocketListener pid) did =
  P.send pid socketListenerRegisterHeader did

-- | Unregister on a socket listener.
unregisterListener :: SocketListener -> P.DestId -> P.Process ()
unregisterListener (SocketListener pid) did =
  P.send pid socketListenerUnregisterHeader did

-- | Add auto-end listening for a socket listener.
addAutoEndListener :: SocketListener -> P.DestId -> P.Process ()
addAutoEndListener (SocketListener pid) did =
  P.send pid addAutoEndListenerHeader did

-- | Remove auto-end listening for a socket listener.
removeAutoEndListener :: SocketListener -> P.DestId -> P.Process ()
removeAutoEndListener (SocketListener pid) did =
  P.send pid removeAutoEndListenerHeader did

-- | Send a message to a socket port.
send :: (P.IsDest a, B.Binary b) => SocketPort -> a -> P.Header -> b ->
        P.Process ()
send (SocketPort pid) dest header payload =
  let annotation =
        [P.Annotation U.proxyDestIdTag . U.encode $ P.makeDestId dest]
  in P.sendAnnotated pid header payload annotation

-- | Send a message to a socket port with a unique Id.
sendWithUniqueId :: (P.IsDest a, B.Binary b) => SocketPort -> a -> P.UniqueId ->
                    P.Header -> b -> P.Process ()
sendWithUniqueId (SocketPort pid) dest uid header payload =
  let annotation =
        [P.Annotation U.proxyDestIdTag . U.encode $ P.makeDestId dest,
         P.Annotation U.uniqueIdTag $ U.encode uid]
  in P.sendAnnotated pid header payload annotation

-- | Look up a socket port.
lookup :: P.Name -> P.Process (Maybe SocketPort)
lookup name = do
  did <- P.lookup name
  case did of
    P.ProcessDest pid -> return . Just $ SocketPort pid
    P.GroupDest _ -> return Nothing

-- | Try to lookup a socket port.
tryLookup :: P.Name -> P.Process (Maybe SocketPort)
tryLookup name = do
  did <- P.tryLookup name
  case did of
    Just (P.ProcessDest pid) -> return . Just $ SocketPort pid
    _ -> return Nothing

-- | Subscribe a socket port to a group.
subscribe :: P.GroupId -> SocketPort -> P.Process ()
subscribe gid (SocketPort pid) = P.subscribeAsProxy gid pid

-- | Unsubscribe a socket port from a group.
unsubscribe :: P.GroupId -> SocketPort -> P.Process ()
unsubscribe gid (SocketPort pid) = P.unsubscribeAsProxy gid pid

-- | Assign a socket port to a name.
assign :: P.Name -> SocketPort -> P.Process ()
assign name (SocketPort pid) = P.assign name $ P.ProcessDest pid

-- | Unassign a socket port from a name.
unassign :: P.Name -> SocketPort -> P.Process ()
unassign name (SocketPort pid) = P.unassign name $ P.ProcessDest pid

-- | Listen for socket port end.
listenEnd :: SocketPort -> P.Process ()
listenEnd (SocketPort pid) = P.listenEnd $ P.ProcessDest pid

-- | Set listening for socket port end as a proxy.
listenEndAsProxy :: SocketPort -> P.DestId -> P.Process ()
listenEndAsProxy (SocketPort pid) did =
  P.listenEndAsProxy (P.ProcessDest pid) did

-- | Stop listening for socket port end.
unlistenEnd :: SocketPort -> P.Process ()
unlistenEnd (SocketPort pid) = P.unlistenEnd $ P.ProcessDest pid

-- | Stop listening for socket port end as a proxy.
unlistenEndAsProxy :: SocketPort -> P.DestId -> P.Process ()
unlistenEndAsProxy (SocketPort pid) did =
  P.unlistenEndAsProxy (P.ProcessDest pid) did

-- | Look up a remote name with a timeout (in microseconds).
lookupRemote :: SocketPort -> P.Name -> Int ->
                P.Process (Either T.Text P.DestId)
lookupRemote (SocketPort pid) name timeout = do
  uid <- P.newUniqueId
  U.sendWithUniqueId pid uid lookupRemoteHeader name
  maybeDid <- P.receiveWithTimeout timeout
    [\msg -> if U.matchHeaderAndUniqueId msg remoteAssignmentHeader uid
             then case U.getPayload msg of
                    Right assignment ->
                      Just . return $ P.usasDestId assignment
                    Left _ -> Nothing
             else Nothing]
  case maybeDid of
    Just did -> return $ Right did
    Nothing -> return $ Left "remote lookup timed out"

-- | Attempt to look up a remote name with a timeout (in microseconds).
tryLookupRemote :: SocketPort -> P.Name -> Int ->
                   P.Process (Either T.Text (Maybe P.DestId))
tryLookupRemote (SocketPort pid) name timeout = do
  uid <- P.newUniqueId
  U.sendWithUniqueId pid uid tryLookupRemoteHeader name
  maybeDid <- P.receiveWithTimeout timeout
    [\msg -> if U.matchHeaderAndUniqueId msg remoteAssignmentHeader uid
             then case U.getPayload msg of
                    Right assignment ->
                      Just . return . Just $ P.usasDestId assignment
                    Left _ -> Nothing
             else Nothing,
     \msg -> if U.matchHeaderAndUniqueId msg noRemoteAssignmentHeader uid
             then case U.getPayload msg :: Either T.Text P.Name of
                    Right _ -> Just $ return Nothing
                    Left _ -> Nothing
             else Nothing ]
  case maybeDid of
    Just maybeDid' -> return $ Right maybeDid'
    Nothing -> return $ Left "remote lookup timed out"

-- | Look up a remote name asynchronously.
asyncLookupRemote :: SocketPort -> P.Name -> P.Process AsyncLookupRemote
asyncLookupRemote (SocketPort pid) name = do
  uid <- P.newUniqueId
  U.sendWithUniqueId pid uid lookupRemoteHeader name
  return $ AsyncLookupRemote uid

-- | Attempt to look up a remote name asynchronously.
asyncTryLookupRemote :: SocketPort -> P.Name -> P.Process AsyncLookupRemote
asyncTryLookupRemote (SocketPort pid) name = do
  uid <- P.newUniqueId
  U.sendWithUniqueId pid uid tryLookupRemoteHeader name
  return $ AsyncLookupRemote uid

-- | Get whether a socket port has ended.
isEnd :: SocketPort -> P.Message -> Bool
isEnd (SocketPort pid) msg = U.isEndForProcessId msg pid

-- | Get whether a socket port has ended normally.
isNormalEnd :: SocketPort -> P.Message -> Bool
isNormalEnd (SocketPort pid) msg = U.isNormalEndForProcessId msg pid

-- | Get whether a socket port has failed.
isFail :: SocketPort -> P.Message -> Bool
isFail (SocketPort pid) msg = U.isFailForProcessId msg pid

-- | Handle an asychronous remote lookup response.
handleAsyncLookupRemoteResponse :: AsyncLookupRemote -> P.Message ->
                                   Maybe (Maybe P.DestId)
handleAsyncLookupRemoteResponse (AsyncLookupRemote uid) msg
  | U.matchHeaderAndUniqueId msg remoteAssignmentHeader uid =
    case U.getPayload msg of
      Right assignment -> Just . Just $ P.usasDestId assignment
      Left _ -> Nothing
  | U.matchHeaderAndUniqueId msg noRemoteAssignmentHeader uid = Just $ Nothing
  | True = Nothing

-- | Get whether a socket port has been accepted, and if it has, return the
-- socket port.
accept :: SocketListener -> P.Message -> Maybe SocketPort
accept (SocketListener listenerPid) msg
  | U.matchHeaderAndProcessId msg acceptedHeader listenerPid =
    case U.getPayload msg :: Either T.Text P.ProcessId of
      Right pid -> Just $ SocketPort pid
      Left _ -> Nothing
  | True = Nothing

-- | Look up a socket listener.
listenerLookup :: P.Name -> P.Process (Maybe SocketListener)
listenerLookup name = do
  did <- P.lookup name
  case did of
    P.ProcessDest pid -> return . Just $ SocketListener pid
    P.GroupDest _ -> return Nothing

-- | Try to lookup a socket listener.
listenerTryLookup :: P.Name -> P.Process (Maybe SocketListener)
listenerTryLookup name = do
  did <- P.tryLookup name
  case did of
    Just (P.ProcessDest pid) -> return . Just $ SocketListener pid
    _ -> return Nothing

-- | Subscribe a socket listener to a group.
listenerSubscribe :: P.GroupId -> SocketListener -> P.Process ()
listenerSubscribe gid (SocketListener pid) = P.subscribeAsProxy gid pid

-- | Unsubscribe a socket listener from a group.
listenerUnsubscribe :: P.GroupId -> SocketListener -> P.Process ()
listenerUnsubscribe gid (SocketListener pid) = P.unsubscribeAsProxy gid pid

-- | Assign a socket listener to a name.
listenerAssign :: P.Name -> SocketListener -> P.Process ()
listenerAssign name (SocketListener pid) = P.assign name $ P.ProcessDest pid

-- | Unassign a socket listener from a name.
listenerUnassign :: P.Name -> SocketListener -> P.Process ()
listenerUnassign name (SocketListener pid) = P.unassign name $ P.ProcessDest pid

-- | Listen for socket listener end.
listenerListenEnd :: SocketListener -> P.Process ()
listenerListenEnd (SocketListener pid) = P.listenEnd $ P.ProcessDest pid

-- | Set listening for socket listener end as a proxy.
listenerListenEndAsProxy :: SocketListener -> P.DestId -> P.Process ()
listenerListenEndAsProxy (SocketListener pid) did =
  P.listenEndAsProxy (P.ProcessDest pid) did

-- | Stop listening for socket listener end.
listenerUnlistenEnd :: SocketListener -> P.Process ()
listenerUnlistenEnd (SocketListener pid) = P.unlistenEnd $ P.ProcessDest pid

-- | Stop listening for socket listener end as a proxy.
listenerUnlistenEndAsProxy :: SocketListener -> P.DestId -> P.Process ()
listenerUnlistenEndAsProxy (SocketListener pid) did =
  P.unlistenEndAsProxy (P.ProcessDest pid) did

-- | Get whether a socket listener has ended.
listenerIsEnd :: SocketListener -> P.Message -> Bool
listenerIsEnd (SocketListener pid) msg = U.isEndForProcessId msg pid

-- | Get whether a socket listener has ended normally.
listenerIsNormalEnd :: SocketListener -> P.Message -> Bool
listenerIsNormalEnd (SocketListener pid) msg = U.isNormalEndForProcessId msg pid

-- | Get whether a socket listener has failed.
listenerIsFail :: SocketListener -> P.Message -> Bool
listenerIsFail (SocketListener pid) msg = U.isFailForProcessId msg pid

-- | Start a listener.
startListener :: NS.SockAddr -> P.Key -> S.Seq P.DestId -> S.Seq P.DestId ->
                 P.Process ()
startListener sockAddr key registered autoEndListeners = do
  socket <- liftIO $ NS.socket (familyOfSockAddr sockAddr) NS.Stream
            NS.defaultProtocol
  handleSocket socket $ do
    liftIO $ NS.setSocketOption socket NS.ReuseAddr 1
    liftIO $ NS.bind socket sockAddr
    liftIO $ NS.listen socket 5
    myPid <- P.myProcessId
    listenPid <- P.spawnListenEnd' (startListenProcess myPid socket key)
                 [P.ProcessDest myPid]
    P.handle (\e -> do P.kill' $ P.ProcessDest listenPid
                       liftIO $ throw (e :: SomeException)) $ do
      runParentListener $ SocketListenerState { slsSocket = socket,
                                                slsListenPid = listenPid,
                                                slsRegistered =
                                                  normalize registered,
                                                slsAutoEndListeners =
                                                  autoEndListeners }

-- | Start a listening process.
startListenProcess :: P.ProcessId -> NS.Socket -> P.Key -> P.Process ()
startListenProcess parentPid socket key = do
  P.handle (\e -> const P.quit' (e :: IOException)) $ do
    runListenProcess parentPid socket key

-- | Run a listening process.
runListenProcess :: P.ProcessId -> NS.Socket -> P.Key -> P.Process ()
runListenProcess parentPid socket key = do
  (socket', _) <- liftIO $ NS.accept socket
  logMessage "Got incoming socket\n"
  P.send parentPid autoSetupRequestHeader BS.empty
  logMessage "Sent auto setup request to parent process\n"
  response <- P.receive
    [\msg ->
        if U.matchHeaderAndProcessId msg autoSetupResponseHeader parentPid
        then case U.getPayload msg of
               Right response -> Just $ return response
               Left _ -> Nothing
        else Nothing]
  logMessage "Got auto setup response from parent process\n"
  childPid <- P.spawnListenEnd' (startSocketPort socket' key)
              (asrAutoEndListeners response)
  P.send parentPid acceptedHeader childPid
  runListenProcess parentPid socket key

-- | Run the parent listener process.
runParentListener :: SocketListenerState -> P.Process ()
runParentListener state = do
  state <- P.receive [handleListenerRegister state,
                      handleListenerUnregister state,
                      handleAddAutoEndListener state,
                      handleRemoveAutoEndListener state,
                      handleAutoSetupRequest state,
                      handleAccepted state]
  runParentListener state

-- | Handle listener registration.
handleListenerRegister :: SocketListenerState -> P.Message ->
                          Maybe (P.Process SocketListenerState)
handleListenerRegister state msg
  | U.matchHeader msg socketListenerRegisterHeader =
    case U.getPayload msg :: Either T.Text P.DestId of
      Right did ->
        case M.lookup did $ slsRegistered state of
          Just count ->
            Just . return $ state { slsRegistered = M.insert did (count + 1) $
                                                    slsRegistered state }
          Nothing ->
            Just . return $ state { slsRegistered = M.insert did 1 $
                                                    slsRegistered state }
      Left _ -> Just $ return state
  | True = Nothing

-- | Handle listener unregistration.
handleListenerUnregister :: SocketListenerState -> P.Message ->
                            Maybe (P.Process SocketListenerState)
handleListenerUnregister state msg
  | U.matchHeader msg socketListenerUnregisterHeader =
    case U.getPayload msg :: Either T.Text P.DestId of
      Right did ->
        case M.lookup did $ slsRegistered state of
          Just 1 ->
            Just . return $ state { slsRegistered = M.delete did $
                                                    slsRegistered state }
          Just count ->
            Just . return $ state { slsRegistered = M.insert did (count - 1) $
                                                    slsRegistered state }
          Nothing -> Just $ return state
      Left _ -> Just $ return state
  | True = Nothing

-- | Handle add an auto end listener.
handleAddAutoEndListener :: SocketListenerState -> P.Message ->
                            Maybe (P.Process SocketListenerState)
handleAddAutoEndListener state msg
  | U.matchHeader msg addAutoEndListenerHeader =
    case U.getPayload msg :: Either T.Text P.DestId of
      Right did ->
        Just . return $ state { slsAutoEndListeners =
                                  slsAutoEndListeners state |> did }
      Left _ -> Just $ return state
  | True = Nothing

-- | Handle remove an auto end listener.
handleRemoveAutoEndListener :: SocketListenerState -> P.Message ->
                               Maybe (P.Process SocketListenerState)
handleRemoveAutoEndListener state msg
  | U.matchHeader msg removeAutoEndListenerHeader =
    case U.getPayload msg :: Either T.Text P.DestId of
      Right did ->
        case S.findIndexL (== did) $ slsAutoEndListeners state of
          Just index ->
            Just . return $ state { slsAutoEndListeners =
                                      S.deleteAt index $
                                      slsAutoEndListeners state }
          Nothing -> Just $ return state
      Left _ -> Just $ return state
  | True = Nothing

-- | Handle an auto setup request.
handleAutoSetupRequest :: SocketListenerState -> P.Message ->
                          Maybe (P.Process SocketListenerState)
handleAutoSetupRequest state msg
  | U.matchHeaderAndProcessId msg autoSetupRequestHeader $ slsListenPid state =
    Just $ do
      let response = AutoSetupResponse { asrAutoEndListeners =
                                           slsAutoEndListeners state }
      U.reply msg autoSetupResponseHeader response
      logMessage "Sent auto setup response to listen process\n"
      return state
  | True = Nothing

-- | Handle an accepted connection.
handleAccepted :: SocketListenerState -> P.Message ->
                  Maybe (P.Process SocketListenerState)
handleAccepted state msg
  | U.matchHeaderAndProcessId msg acceptedHeader $ slsListenPid state =
    case U.getPayload msg :: Either T.Text P.ProcessId of
      Right _ ->
        Just $ do
          forM_ (M.keys $ slsRegistered state) $ \did ->
            P.sendRaw did acceptedHeader $ P.messagePayload msg
          return state
      Left _ -> Just $ return state
  | True = Nothing

-- | Start a socket port without an existing connection.
startSocketPortWithoutSocket :: NS.SockAddr -> P.Key -> P.Process ()
startSocketPortWithoutSocket sockAddr key = do
  socket <- liftIO $ NS.socket (familyOfSockAddr sockAddr) NS.Stream
            NS.defaultProtocol
  handleSocket socket $ do liftIO $ NS.connect socket sockAddr
                           startSocketPort socket key

-- | Actually start a socket port.
startSocketPort :: NS.Socket -> P.Key -> P.Process ()
startSocketPort socket key = do
  myPid <- P.myProcessId
  sendPid <- P.spawnListenEnd' (startSendProcess myPid socket key)
             [P.ProcessDest myPid]
  P.handle (\e -> do P.kill' sendPid
                     liftIO $ throw (e :: SomeException)) $ do
    receivePid <- P.spawnListenEnd' (startReceiveProcess myPid socket key)
                  [P.ProcessDest myPid]
    P.handle (\e -> do P.kill' receivePid
                       liftIO $ throw (e :: SomeException)) $ do
      runSocketPort $ SocketPortState { spsSendPid = sendPid,
                                        spsReceivePid = receivePid,
                                        spsSocket = socket,
                                        spsSendStopped = False,
                                        spsReceiveStopped = False,
                                        spsAssignment = M.empty,
                                        spsWaitAssignment = S.empty }

-- | Run the main loop of a socket port's main process
runSocketPort :: SocketPortState -> P.Process ()
runSocketPort state = do
  state <- P.receive [handleSendEnd state, handleReceiveEnd state,
                      handleLookupRemote state, handleTryLookupRemote state,
                      handleIncoming state, handleOutgoing state]
  runSocketPort state

-- | Handle send process end
handleSendEnd :: SocketPortState -> P.Message ->
                 Maybe (P.Process SocketPortState)
handleSendEnd state msg
  | U.isEndForProcessId msg $ spsSendPid state = Just $ P.quit' >> return state
  | True = Nothing

-- | Handle receive process end
handleReceiveEnd :: SocketPortState -> P.Message ->
                    Maybe (P.Process SocketPortState)
handleReceiveEnd state msg
  | U.isEndForProcessId msg $ spsReceivePid state =
    Just $ P.quit' >> return state
  | True = Nothing

-- | Handle remote lookup
handleLookupRemote :: SocketPortState -> P.Message ->
                      Maybe (P.Process SocketPortState)
handleLookupRemote state msg
  | U.matchHeader msg lookupRemoteHeader =
    Just $ do
      case U.getPayload msg of
        Right name ->
          case M.lookup name $ spsAssignment state of
            Just did -> do
              let payload = P.UserAssignment { usasName = name,
                                               usasDestId = did }
              U.reply msg remoteAssignmentHeader payload
              return state
            Nothing ->
              case U.getUniqueId msg of
                Right (Just uid) -> do
                  logMessage . printf "Deferring remote lookup for %s\n" $
                    show uid
                  case U.processIdOfSourceId $ P.messageSourceId msg of
                    Just pid ->
                      let entry = (name, P.ProcessDest pid, uid)
                      in return $ state { spsWaitAssignment =
                                          spsWaitAssignment state |> entry }
                    Nothing -> return state
                _ -> return state
        Left _ -> return state
  | True = Nothing

-- | Handle attempted remote lookup
handleTryLookupRemote :: SocketPortState -> P.Message ->
                         Maybe (P.Process SocketPortState)
handleTryLookupRemote state msg
  | U.matchHeader msg tryLookupRemoteHeader =
    Just $ do
      case U.getPayload msg of
        Right name ->
          case M.lookup name $ spsAssignment state of
            Just did -> do
              let payload = P.UserAssignment { usasName = name,
                                               usasDestId = did }
              U.reply msg remoteAssignmentHeader payload
              return state
            Nothing -> do
              U.reply msg noRemoteAssignmentHeader name
              return state
        Left _ -> return state
  | True = Nothing

-- | Handle incoming messages
handleIncoming :: SocketPortState -> P.Message ->
                  Maybe (P.Process SocketPortState)
handleIncoming state msg
  | U.matchHeaderAndProcessId msg receiveRemoteHeader $ spsReceivePid state =
    Just $ do
      case U.getPayload msg of
        Right (SocketUserMessage container) -> do
          logMessage . printf "Received user message with %d annotations\n" .
            S.length $ P.mcontAnnotations container
          case getAnnotation (P.mcontAnnotations container) U.proxyDestIdTag of
            Just did ->
              case U.tryDecode did :: Either T.Text P.DestId of
                Right did ->
                  let newAnnotations =
                        unsetAnnotation (P.mcontAnnotations container)
                        U.proxyDestIdTag
                  in P.sendRawAnnotated did (P.mcontHeader container)
                     (P.mcontPayload container) newAnnotations
                Left _ -> logMessage "Failed to decode proxy destination Id\n"
            Nothing -> logMessage "Failed to find proxy destination Id\n"
          return state
        Right (SocketAssign name destId) -> do
          logMessage "Received assign message\n"
          waiting <-
            forM (toList $ spsWaitAssignment state) $
            \item@(name', destId', uid) ->
              if name == name'
              then do
                logMessage . printf "Fulfilling remote lookup for %s\n" $
                  show uid
                let payload = P.UserAssignment name destId
                U.sendWithUniqueId destId' uid remoteAssignmentHeader payload
                return S.empty
              else return $ S.singleton item
          return $ state { spsAssignment =
                             M.insert name destId $ spsAssignment state ,
                           spsWaitAssignment = mconcat waiting }
        Right (SocketUnassign name destId) -> do
          logMessage "Received unassign message\n"
          return $ state { spsAssignment =
                             M.delete name $ spsAssignment state }
        Left _ -> do
          logMessage "Failed to decode incoming message\n"
          return state
  | True = Nothing

-- | Get an annotation from an annotation sequence
getAnnotation :: S.Seq P.Annotation -> P.AnnotationTag ->
                 Maybe P.AnnotationValue
getAnnotation annotations tag =
  case S.findIndexL (\(P.Annotation tag' _) -> tag == tag') annotations of
    Just index ->
      case S.lookup index annotations of
        Just (P.Annotation _ value) -> Just value
        Nothing -> error "impossible"
    Nothing -> Nothing

-- | Remove an annotation from an annotation sequence
unsetAnnotation :: S.Seq P.Annotation -> P.AnnotationTag -> S.Seq P.Annotation
unsetAnnotation annotations tag =
  S.filter (\(P.Annotation tag' _) -> tag /= tag') annotations

-- | Set an annotation in an annotation sequence
setAnnotation :: S.Seq P.Annotation -> P.AnnotationTag -> P.AnnotationValue ->
                 S.Seq P.Annotation
setAnnotation annotations tag value =
  unsetAnnotation annotations tag |> P.Annotation tag value

-- | Handle outgoing messages
handleOutgoing :: SocketPortState -> P.Message ->
                  Maybe (P.Process SocketPortState)
handleOutgoing state msg
  | U.excludeProcessId msg (spsSendPid state)
    && U.excludeProcessId msg (spsReceivePid state) =
      Just $ do
        if not $ spsSendStopped state
          then case getAnnotation (P.messageAnnotations msg)
                    U.proxyDestIdTag of
                 Just _ -> do
                   let annotations =
                         setAnnotation (P.messageAnnotations msg)
                         U.proxySourceIdTag (U.encode $ P.messageSourceId msg)
                       payload =
                         P.MessageContainer (P.messageHeader msg)
                         (P.messagePayload msg) annotations
                   P.send (spsSendPid state) sendRemoteHeader payload
                 Nothing -> return ()
          else return ()
        return state
  | True = Nothing

-- | Start send process
startSendProcess :: P.ProcessId -> NS.Socket -> P.Key -> P.Process ()
startSendProcess parentPid socket key = do
  P.listenAssign
  allNames <- P.lookupAll
  let key' = U.encode key
      assignParts = map formatAssign $ M.toList allNames
      bytes = mconcat (U.encode magicValue :
                       U.encode (fromIntegral $ BS.length key' :: Word64) :
                       key' : assignParts)
  P.handle (\e -> const P.quit' (e :: IOException)) $ do
    liftIO $ NSB.sendAll socket bytes
  runSendProcess parentPid socket
    where formatAssign (name, destId) =
            let assignBytes = U.encode $ SocketAssign name destId
                lengthBytes =
                  U.encode (fromIntegral $ BS.length assignBytes :: Word64)
            in lengthBytes <> assignBytes

-- | Run send process
runSendProcess :: P.ProcessId -> NS.Socket -> P.Process ()
runSendProcess parentPid socket = do
  P.receive [handleSendRemote parentPid socket,
             handleAssigned socket,
             handleUnassigned socket]
  runSendProcess parentPid socket

-- | Handle send remote message
handleSendRemote :: P.ProcessId -> NS.Socket -> P.Message ->
                    Maybe (P.Process ())
handleSendRemote parentPid socket msg
  | U.matchHeaderAndProcessId msg sendRemoteHeader parentPid =
    Just $ do
      case U.getPayload msg of
        Right container ->
          sendSocketMessage socket $ SocketUserMessage container
        Left _ -> return ()
  | True = Nothing

-- | Handle assigned message
handleAssigned :: NS.Socket -> P.Message -> Maybe (P.Process ())
handleAssigned socket msg
  | U.matchHeader msg U.assignedHeader =
    Just $ do
      case U.getPayload msg of
        Right P.UserAssignment{..} -> do
          sendSocketMessage socket $ SocketAssign usasName usasDestId
        Left _ -> return ()
  | True = Nothing  

-- | Handle unassigned message
handleUnassigned :: NS.Socket -> P.Message -> Maybe (P.Process ())
handleUnassigned socket msg
  | U.matchHeader msg U.unassignedHeader =
    Just $ do
      case U.getPayload msg of
        Right P.UserAssignment{..} ->
          sendSocketMessage socket $ SocketUnassign usasName usasDestId
        Left _ -> return ()
  | True = Nothing  

-- | Send socket message
sendSocketMessage :: NS.Socket -> SocketMessage -> P.Process ()
sendSocketMessage socket msg = do
  let bytes = U.encode msg
      lengthField = U.encode (fromIntegral $ BS.length bytes :: Word64)
      bytes' = lengthField <> bytes
  P.handle (\e -> const P.quit' (e :: IOException))
    (liftIO $ NSB.sendAll socket bytes')

-- | Start receive process
startReceiveProcess :: P.ProcessId -> NS.Socket -> P.Key -> P.Process ()
startReceiveProcess parentPid socket key = do
  (magicField, rest) <- receiveBytes socket word32Size BS.empty
  case U.tryDecode magicField :: Either T.Text Word32 of
    Left _ -> do
      logMessage "Failed to decode magicField\n"
      P.quit'
    Right magicField
      | magicField /= magicValue -> do
          logMessage "magicField does not match magicValue\n"
          P.quit'
      | True -> do
          (keyLength, rest) <- receiveBytes socket word64Size rest
          case U.tryDecode keyLength :: Either T.Text Word64 of
            Left _ -> do
              logMessage "Failed to decode keyLength\n"
              P.quit'
            Right keyLength -> do
              (incomingKey, rest) <- receiveBytes socket keyLength rest
              case U.tryDecode incomingKey :: Either T.Text P.Key of
                Left _ -> do
                  logMessage "Failed to decode incomingKey\n"
                  P.quit'
                Right incomingKey
                  | incomingKey /= key -> do
                      logMessage "incomingKey does not match key\n"
                      P.quit'
                  | True -> runReceiveProcess parentPid socket rest

-- | Run receive process
runReceiveProcess :: P.ProcessId -> NS.Socket -> BS.ByteString -> P.Process ()
runReceiveProcess parentPid socket buffer = do
  (messageLength, rest) <- receiveBytes socket word64Size buffer
  case U.tryDecode messageLength :: Either T.Text Word64 of
    Left _ -> do
      logMessage "Failed to decode messageLength\n"
      P.quit'
    Right messageLength -> do
      (message, rest) <- receiveBytes socket messageLength rest
      case U.tryDecode message :: Either T.Text SocketMessage of
        Left _ -> do
          logMessage "Failed to decode socket message\n"
          P.quit'
        Right message -> do
          P.send parentPid receiveRemoteHeader message
          runReceiveProcess parentPid socket rest
  
-- | Receive a minimum number of bytes
receiveBytes :: NS.Socket -> Word64 -> BS.ByteString ->
                P.Process (BS.ByteString, BS.ByteString)
receiveBytes socket length bytes =
  if BS.length bytes >= fromIntegral length
  then return $ BS.splitAt (fromIntegral length) bytes
  else do
    newBytes <- P.catch (liftIO $ NSB.recv socket 4096)
                (\e -> const (do P.quit'
                                 return BS.empty)
                       (e :: IOException))
    if BS.length newBytes > 0
      then receiveBytes socket length $ bytes <> newBytes
      else do
        P.quit'
        return (BS.empty, BS.empty)

-- | Normalize a registered sequence into a registered map.
normalize :: S.Seq P.DestId -> M.HashMap P.DestId Integer
normalize dids =
  foldl' (\normalized did ->
            case M.lookup did normalized of
              Just count -> M.insert did (count + 1) normalized
              Nothing -> M.insert did 1 normalized)
    M.empty dids

-- | Size of an encoded Word32
word32Size :: Word64
word32Size = fromIntegral . BS.length $ U.encode (0 :: Word32)

-- | Size of an encoded Word64
word64Size :: Word64
word64Size = fromIntegral . BS.length $ U.encode (0 :: Word64)

-- | Socket port register header
socketPortRegisterHeader :: P.Header
socketPortRegisterHeader = P.makeHeader ("socketPortRegister" :: T.Text)

-- | Socket port unregister header
socketPortUnregisterHeader :: P.Header
socketPortUnregisterHeader = P.makeHeader ("socketPortUnregister" :: T.Text)

-- | Socket listener register header
socketListenerRegisterHeader :: P.Header
socketListenerRegisterHeader = P.makeHeader ("socketListenerRegister" :: T.Text)

-- | Socket listener unregister header
socketListenerUnregisterHeader :: P.Header
socketListenerUnregisterHeader =
  P.makeHeader ("socketListenerUnregister" :: T.Text)

-- | Socket listener add auto register
addAutoRegisterHeader :: P.Header
addAutoRegisterHeader = P.makeHeader ("addAutoRegister" :: T.Text)

-- | Socket listener remove auto register
removeAutoRegisterHeader :: P.Header
removeAutoRegisterHeader = P.makeHeader ("removeAutoRegister" :: T.Text)

-- | Socket listener add auto end listener
addAutoEndListenerHeader :: P.Header
addAutoEndListenerHeader = P.makeHeader ("addAutoEndListener" :: T.Text)

-- | Socket listener remove auto end listener
removeAutoEndListenerHeader :: P.Header
removeAutoEndListenerHeader = P.makeHeader ("removeAutoEndListener" :: T.Text)

-- | Send remote header
sendRemoteHeader :: P.Header
sendRemoteHeader = P.makeHeader ("sendRemote" :: T.Text)

-- | Receive remote header
receiveRemoteHeader :: P.Header
receiveRemoteHeader = P.makeHeader ("receiveRemote" :: T.Text)

-- | Auto setup request header
autoSetupRequestHeader :: P.Header
autoSetupRequestHeader = P.makeHeader ("autoSetupRequest" :: T.Text)

-- | Auto setup response header
autoSetupResponseHeader :: P.Header
autoSetupResponseHeader = P.makeHeader ("autoSetupResponse" :: T.Text)

-- | Accepted connection header
acceptedHeader :: P.Header
acceptedHeader = P.makeHeader ("accepted" :: T.Text)

-- | Look up a remote assignment header
lookupRemoteHeader :: P.Header
lookupRemoteHeader = P.makeHeader ("lookupRemote" :: T.Text)

-- | Try to look up a remote assignment header
tryLookupRemoteHeader :: P.Header
tryLookupRemoteHeader = P.makeHeader ("tryLookupRemote" :: T.Text)

-- | Found remote assignment header
remoteAssignmentHeader :: P.Header
remoteAssignmentHeader = P.makeHeader ("remoteAssignment" :: T.Text)

-- | Failed to find remote assignment header
noRemoteAssignmentHeader :: P.Header
noRemoteAssignmentHeader = P.makeHeader ("noRemoteAssignment" :: T.Text)

-- | Handle closing a socket after handling an exception
handleSocket :: NS.Socket -> P.Process a -> P.Process a
handleSocket socket action = do
  P.catch action $ \e -> do
    liftIO $ NS.shutdown socket NS.ShutdownBoth
    liftIO $ NS.close socket
    logMessage . printf "GOT EXCEPTION: %s\n" $ show e
    liftIO $ throw (e :: SomeException)

-- | Get family of SockAddr
familyOfSockAddr :: NS.SockAddr -> NS.Family
familyOfSockAddr (NS.SockAddrInet _ _) = NS.AF_INET
familyOfSockAddr (NS.SockAddrInet6 _ _ _ _) = NS.AF_INET6
familyOfSockAddr (NS.SockAddrUnix _) = NS.AF_UNIX
familyOfSockAddr _ = error "not supported"
