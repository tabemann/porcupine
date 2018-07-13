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

module Control.Concurrent.Porcupine.Process

  (Process,
   Message,
   Handler,
   Entry,
   Header,
   Payload,
   Name,
   Key,
   AnnotationTag,
   AnnotationValue,
   Annotation (..),
   ProcessId,
   NodeId,
   PartialNodeId,
   GroupId,
   UniqueId,
   SourceId (..),
   DestId (..),
   UserRemoteConnectFailed (..),
   UserRemoteDisconnected (..),
   MessageContainer (..),
   UserAssignment (..),
   myProcessId,
   myNodeId,
   nodeIdOfProcessId,
   messageSourceId,
   messageDestId,
   messageHeader,
   messagePayload,
   messageAnnotations,
   makeHeader,
   makeName,
   makeKey,
   makeTag,
   spawnInit,
   spawnInitAnnotated,
   spawnInit',
   spawn,
   spawnAnnotated,
   spawn',
   spawnAsProxy,
   spawnAnnotatedAsProxy,
   spawnAsProxy',
   spawnListenEnd,
   spawnListenEndAnnotated,
   spawnListenEnd',
   spawnListenEndAsProxy,
   spawnListenEndAnnotatedAsProxy,
   spawnListenEndAsProxy',
   quit,
   quit',
   kill,
   kill',
   killAsProxy,
   killAsProxy',
   shutdown,
   shutdown',
   shutdownAsProxy,
   shutdownAsProxy',
   send,
   sendAnnotated,
   sendAsProxy,
   sendAnnotatedAsProxy,
   sendRaw,
   sendRawAnnotated,
   sendRawAsProxy,
   sendRawAnnotatedAsProxy,
   receive,
   tryReceive,
   receiveWithTimeout,
   subscribe,
   unsubscribe,
   subscribeAsProxy,
   unsubscribeAsProxy,
   assign,
   unassign,
   tryLookup,
   lookup,
   lookupAll,
   newGroupId,
   newUniqueId,
   connect,
   connectRemote,
   catch,
   catchJust,
   handle,
   listenEnd,
   unlistenEnd,
   listenEndAsProxy,
   unlistenEndAsProxy,
   listenAssign,
   unlistenAssign,
   listenAssignAsProxy,
   unlistenAssignAsProxy)

where

import Control.Concurrent.Porcupine.Private.Types
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.Binary as B
import qualified Data.Sequence as S
import qualified Data.Text as T
import qualified Data.HashMap.Strict as M
import qualified System.Random as R
import qualified Network.Socket as NS
import Data.Sequence (ViewL (..))
import Data.Sequence ((|>))
import Control.Concurrent (myThreadId,
                           threadDelay,
                           killThread)
import Control.Concurrent.Async (Async,
                                 async,
                                 cancel)
import Control.Concurrent.STM (STM,
                               atomically,
                               retry,
                               orElse,
                               TVar,
                               newTVar,
                               readTVar,
                               writeTVar)
import Control.Concurrent.STM.TQueue (TQueue,
                                      newTQueue,
                                      readTQueue,
                                      tryReadTQueue,
                                      writeTQueue)
import Control.Concurrent.STM.TMVar (TMVar,
                                     newEmptyTMVar,
                                     putTMVar,
                                     readTMVar)
import qualified Control.Monad.Trans.State.Strict as St
import qualified Control.Exception.Base as E
import Data.Functor ((<$>))
import Data.Foldable (foldl')
import Control.Monad ((=<<),
                      join)
import Control.Monad.IO.Class (MonadIO (..))
import Prelude hiding (lookup)

-- | Get the current process Id.
myProcessId :: Process ProcessId
myProcessId = Process $ procId <$> St.get

-- | Get the current node Id.
myNodeId :: Process NodeId
myNodeId = Process $ nodeId . procNode <$> St.get

-- | Get node Id of process Id
nodeIdOfProcessId :: ProcessId -> NodeId
nodeIdOfProcessId = pidNodeId

-- | Get source Id of a message.
messageSourceId :: Message -> SourceId
messageSourceId = msgSourceId

-- | Get destination Id of a message.
messageDestId :: Message -> DestId
messageDestId = msgDestId

-- | Get header of a message.
messageHeader :: Message -> Header
messageHeader = msgHeader

-- | Get payload of a message.
messagePayload :: Message -> Payload
messagePayload = msgPayload

-- | Get the annotations of a message.
messageAnnotations :: Message -> S.Seq Annotation
messageAnnotations = msgAnnotations

-- | Make a header
makeHeader :: B.Binary a => a -> Header
makeHeader = Header . encode

-- | Make a name
makeName :: B.Binary a => a -> Name
makeName = Name . encode

-- | Make a key
makeKey :: B.Binary a => a -> Key
makeKey = Key . encode

-- | Make an annotation tag
makeTag :: B.Binary a => a -> AnnotationTag
makeTag = AnnotationTag . encode

-- | Empty header
emptyHeader :: Header
emptyHeader = Header BS.empty

-- | Spawn a process on the local node without a preexisting process.
spawnInit :: Entry -> Node -> Header -> Payload -> IO ProcessId
spawnInit entry node header payload =
  spawnInitAnnotated entry node header payload S.empty

-- | Spawn a process on the local node without a preexisting process with
-- annotations.
spawnInitAnnotated :: Entry -> Node -> Header -> Payload -> S.Seq Annotation ->
                      IO ProcessId
spawnInitAnnotated entry node header payload annotations = do
  spawnedPid <- newProcessIdForNode node
  let message =
        SpawnMessage { spawnMessage =
                         Message { msgSourceId = NoSource,
                                   msgDestId = ProcessDest spawnedPid,
                                   msgHeader = header,
                                   msgPayload = payload,
                                   msgAnnotations = annotations },
                       spawnEntry = entry,
                       spawnProcessId = spawnedPid,
                       spawnEndListeners = S.empty } 
  atomically . writeTQueue (nodeQueue node) $
    LocalReceived { lrcvMessage = message }
  return spawnedPid

-- | Spawn a process on the local node without a preexisting process without
-- instantiation parameters (because they are normally only needed for remote
-- spawns).
spawnInit' :: Process () -> Node -> IO ProcessId
spawnInit' action node = spawnInit (\_ -> action) node emptyHeader BS.empty

-- | Spawn a process on the local node normally.
spawn :: Entry -> Header -> Payload -> Process ProcessId
spawn entry header payload = do
  spawnAnnotated entry header payload S.empty

-- | Spawn a process on the local node normally with annotations.
spawnAnnotated :: Entry -> Header -> Payload -> S.Seq Annotation ->
                  Process ProcessId
spawnAnnotated entry header payload annotations = do
  pid <- myProcessId
  spawnedPid <- newProcessId
  sendEvent $ SpawnMessage { spawnMessage =
                               Message { msgSourceId = NormalSource pid,
                                         msgDestId = ProcessDest spawnedPid,
                                         msgHeader = header,
                                         msgPayload = payload,
                                         msgAnnotations = annotations },
                             spawnEntry = entry,
                             spawnProcessId = spawnedPid,
                             spawnEndListeners = S.empty }
  return spawnedPid

-- | Spawn a process on the local node normally without instantiation parameters
-- (because they are normally only needed for remote spawns).
spawn' :: Process () -> Process ProcessId
spawn' action = spawn (\_ -> action) emptyHeader BS.empty

-- | Spawn a process on the local node normally for another process.
spawnAsProxy :: Entry -> ProcessId -> Header -> Payload -> Process ()
spawnAsProxy entry pid header payload =
  spawnAnnotatedAsProxy entry pid header payload S.empty

-- | Spawn a process on the local node normally with annotations for another
-- process.
spawnAnnotatedAsProxy :: Entry -> ProcessId -> Header -> Payload ->
                         S.Seq Annotation -> Process ()
spawnAnnotatedAsProxy entry pid header payload annotations = do
  spawnedPid <- newProcessId
  sendEvent $ SpawnMessage { spawnMessage =
                               Message { msgSourceId = NormalSource pid,
                                         msgDestId = ProcessDest spawnedPid,
                                         msgHeader = header,
                                         msgPayload = payload,
                                         msgAnnotations = annotations },
                             spawnEntry = entry,
                             spawnProcessId = spawnedPid,
                             spawnEndListeners = S.empty } 

-- | Spawn a process on the local node normally for another process without
-- instantiation parameters (because they are normally only needed for remote
-- spawns).
spawnAsProxy' :: Process () -> ProcessId -> Process ()
spawnAsProxy' action pid = spawnAsProxy (\_ -> action) pid emptyHeader BS.empty

-- | Spawn a process on the local node normally and atomically listen for end.
spawnListenEnd :: Entry -> Header -> Payload -> S.Seq DestId ->
                  Process ProcessId
spawnListenEnd entry header payload endListeners =
  spawnListenEndAnnotated entry header payload S.empty endListeners

-- | Spawn a process on the local node normally with annotation and atomically
-- listen for end.
spawnListenEndAnnotated :: Entry -> Header -> Payload -> S.Seq Annotation ->
                           S.Seq DestId -> Process ProcessId
spawnListenEndAnnotated entry header payload annotations endListeners = do
  pid <- myProcessId
  spawnedPid <- newProcessId
  sendEvent $ SpawnMessage { spawnMessage =
                               Message { msgSourceId = NormalSource pid,
                                         msgDestId = ProcessDest spawnedPid,
                                         msgHeader = header,
                                         msgPayload = payload,
                                         msgAnnotations = annotations },
                             spawnEntry = entry,
                             spawnProcessId = spawnedPid,
                             spawnEndListeners = endListeners }
  return spawnedPid

-- | Spawn a process on the local node normally without instantiation parameters
-- (because they are normally only needed for remote spawns) and atomically
-- listen for end.
spawnListenEnd' :: Process () -> S.Seq DestId -> Process ProcessId
spawnListenEnd' action endListeners =
  spawnListenEnd (\_ -> action) emptyHeader BS.empty endListeners

-- | Spawn a process on the local node normally for another process and
-- atomically listen for end.
spawnListenEndAsProxy :: Entry -> ProcessId -> Header -> Payload ->
                         S.Seq DestId -> Process ()
spawnListenEndAsProxy entry pid header payload endListeners =
  spawnListenEndAnnotatedAsProxy entry pid header payload S.empty endListeners

-- | Spawn a process on the local node normally with annotation for another
-- process and atomically listen for end.
spawnListenEndAnnotatedAsProxy :: Entry -> ProcessId -> Header -> Payload ->
                                  S.Seq Annotation -> S.Seq DestId -> Process ()
spawnListenEndAnnotatedAsProxy entry pid header payload annotations
  endListeners = do
  spawnedPid <- newProcessId
  sendEvent $ SpawnMessage { spawnMessage =
                               Message { msgSourceId = NormalSource pid,
                                         msgDestId = ProcessDest spawnedPid,
                                         msgHeader = header,
                                         msgPayload = payload,
                                         msgAnnotations = annotations },
                             spawnEntry = entry,
                             spawnProcessId = spawnedPid,
                             spawnEndListeners = endListeners } 

-- | Spawn a process on the local node normally for another process without
-- instantiation parameters (because they are normally only needed for remote
-- spawns) and atomically listen for End.
spawnListenEndAsProxy' :: Process () -> ProcessId -> S.Seq DestId -> Process ()
spawnListenEndAsProxy' action pid endListeners =
  spawnListenEndAsProxy (\_ -> action) pid emptyHeader BS.empty endListeners

-- | Send a message to a process or group.
send :: B.Binary a => DestId -> Header -> a -> Process ()
send did header payload = sendAnnotated did header payload S.empty

-- | Send a message to a process or group with annotations.
sendAnnotated :: B.Binary a => DestId -> Header -> a -> S.Seq Annotation ->
                 Process ()
sendAnnotated did header payload annotations = do
  pid <- myProcessId
  sendEvent $ UserMessage { umsgMessage =
                              Message { msgSourceId = NormalSource pid,
                                        msgDestId = did,
                                        msgHeader = header,
                                        msgPayload = encode payload,
                                        msgAnnotations = annotations } }

-- | Send a message to a process or group for another process.
sendAsProxy :: B.Binary a => DestId -> SourceId -> Header -> a -> Process ()
sendAsProxy did sid header payload =
  sendAnnotatedAsProxy did sid header payload S.empty

-- | Send a message to a process or group for another process with annotation.
sendAnnotatedAsProxy :: B.Binary a => DestId -> SourceId -> Header -> a ->
                        S.Seq Annotation -> Process ()
sendAnnotatedAsProxy did sid header payload annotations = do
  sendEvent $ UserMessage { umsgMessage =
                              Message { msgSourceId = sid,
                                        msgDestId = did,
                                        msgHeader = header,
                                        msgPayload = encode payload,
                                        msgAnnotations = annotations } }

-- | Send a message with unencoded data to a process or group.
sendRaw :: DestId -> Header -> Payload -> Process ()
sendRaw did header payload = sendRawAnnotated did header payload S.empty

-- | Send a message with unencoded data to a process or group with annotations.
sendRawAnnotated :: DestId -> Header -> Payload -> S.Seq Annotation ->
                     Process ()
sendRawAnnotated did header payload annotations = do
  pid <- myProcessId
  sendEvent $ UserMessage { umsgMessage =
                              Message { msgSourceId = NormalSource pid,
                                        msgDestId = did,
                                        msgHeader = header,
                                        msgPayload = payload,
                                        msgAnnotations = annotations } }

-- | Send a message with unencoded data to a process or group for another
-- process.
sendRawAsProxy :: DestId -> SourceId -> Header -> Payload -> Process ()
sendRawAsProxy did sid header payload =
  sendAnnotatedAsProxy did sid header payload S.empty

-- | Send a message with unencoded data to a process or group for another
-- process with annotation.
sendRawAnnotatedAsProxy :: DestId -> SourceId -> Header -> Payload ->
                            S.Seq Annotation -> Process ()
sendRawAnnotatedAsProxy did sid header payload annotations = do
  sendEvent $ UserMessage { umsgMessage =
                              Message { msgSourceId = sid,
                                        msgDestId = did,
                                        msgHeader = header,
                                        msgPayload = payload,
                                        msgAnnotations = annotations } }

-- | Quit the current process.
quit :: Header -> Payload -> Process ()
quit header payload = do
  pid <- myProcessId
  sendEvent $ QuitMessage { quitProcessId = pid,
                            quitHeader = header,
                            quitPayload = payload }
  threadId <- liftIO myThreadId
  liftIO $ killThread threadId

-- | Quit the current process with a generic quit message header and payload.
quit' :: Process ()
quit' = quit genericQuitHeader BS.empty

-- | Generic quit header
genericQuitHeader :: Header
genericQuitHeader = makeHeader ("genericQuit" :: T.Text)

-- | Kill another process or process group.
kill :: DestId -> Header -> Payload -> Process ()
kill did header payload = do
  pid <- myProcessId
  sendEvent $ KillMessage { killProcessId = pid,
                            killDestId = did,
                            killHeader = header,
                            killPayload = payload }

-- | Kill another process or process group with a generic kill message header
-- and payload.
kill' :: DestId -> Process ()
kill' did = kill did genericKillHeader BS.empty

-- | Generic kill header
genericKillHeader :: Header
genericKillHeader = makeHeader ("genericKill" :: T.Text)

-- | Kill another process or process group for another process.
killAsProxy :: DestId -> ProcessId -> Header -> Payload -> Process ()
killAsProxy did pid header payload = do
  sendEvent $ KillMessage { killProcessId = pid,
                            killDestId = did,
                            killHeader = header,
                            killPayload = payload }

-- | Kill another process or process group for another process with a generic
-- kill message header and payload.
killAsProxy' :: DestId -> ProcessId -> Process ()
killAsProxy' did pid =
  killAsProxy did pid genericKillHeader BS.empty

-- | Shutdown a node.
shutdown :: NodeId -> Header -> Payload -> Process ()
shutdown nid header payload = do
  pid <- myProcessId
  sendEvent $ ShutdownMessage { shutProcessId = pid,
                                shutNodeId = nid,
                                shutHeader = header,
                                shutPayload = payload }
  myNid <- myNodeId
  if nid == myNid
    then do tid <- liftIO myThreadId
            liftIO $ killThread tid
    else return ()

-- | Shutdown a node with a generic shutdown message header and payload.
shutdown' :: NodeId -> Process ()
shutdown' nid = shutdown nid genericShutdownHeader BS.empty

-- | Generic shutdown header
genericShutdownHeader :: Header
genericShutdownHeader = makeHeader ("genericShutdown" :: T.Text)

-- | Shutdown a node for another process.
shutdownAsProxy :: NodeId -> ProcessId -> Header -> Payload -> Process ()
shutdownAsProxy nid pid header payload = do
  sendEvent $ ShutdownMessage { shutProcessId = pid,
                                shutNodeId = nid,
                                shutHeader = header,
                                shutPayload = payload }
  myNid <- myNodeId
  if nid == myNid
    then do tid <- liftIO myThreadId
            liftIO $ killThread tid
    else return ()

-- | Shutdown anode for another process with a generic shutdown message header
-- and payload.
shutdownAsProxy' :: NodeId -> ProcessId -> Process ()
shutdownAsProxy' nid pid =
  shutdownAsProxy nid pid genericShutdownHeader BS.empty

-- | Subscribe to a group.
subscribe :: GroupId -> Process ()
subscribe gid = do
  pid <- myProcessId
  sendEvent $ SubscribeMessage { subProcessId = pid,
                                 subGroupId = gid }

-- | Unsubscribe from a group.
unsubscribe :: GroupId -> Process ()
unsubscribe gid = do
  pid <- myProcessId
  sendEvent $ UnsubscribeMessage { usubProcessId = pid,
                                   usubGroupId = gid }

-- | Subscribe another process to a group.
subscribeAsProxy :: GroupId -> ProcessId -> Process ()
subscribeAsProxy gid pid = do
  sendEvent $ SubscribeMessage { subProcessId = pid,
                                 subGroupId = gid }

-- | Unsubscribe another process from a group.
unsubscribeAsProxy :: GroupId -> ProcessId -> Process ()
unsubscribeAsProxy gid pid = do
  sendEvent $ UnsubscribeMessage { usubProcessId = pid,
                                   usubGroupId = gid }

-- | Listen for termination of another process or any member of a group.
listenEnd :: DestId -> Process ()
listenEnd listenedId = do
  pid <- myProcessId
  sendEvent $ ListenEndMessage { lendListenedId = listenedId,
                                 lendListenerId = ProcessDest pid }

-- | Stop listening for termination of another process or any member of a group.
unlistenEnd :: DestId -> Process ()
unlistenEnd listenedId = do
  pid <- myProcessId
  sendEvent $ UnlistenEndMessage { ulendListenedId = listenedId,
                                   ulendListenerId = ProcessDest pid }

-- | Set another process or group to listen for termination of another process
-- or any member of a group.
listenEndAsProxy :: DestId -> DestId -> Process ()
listenEndAsProxy listenedId listenerId = do
  sendEvent $ ListenEndMessage { lendListenedId = listenedId,
                                 lendListenerId = listenerId }

-- | Set another process or group to listen for termination of another process
-- or any member of a group.
unlistenEndAsProxy :: DestId -> DestId -> Process ()
unlistenEndAsProxy listenedId listenerId = do
  sendEvent $ UnlistenEndMessage { ulendListenedId = listenedId,
                                   ulendListenerId = listenerId }

-- | Listen for name assignment and unassignment.
listenAssign :: Process ()
listenAssign = do
  pid <- myProcessId
  sendEvent $ ListenAssignMessage { lassDestId = ProcessDest pid }

-- | Stop listening for name assignment and unassignment.
unlistenAssign :: Process ()
unlistenAssign = do
  pid <- myProcessId
  sendEvent $ UnlistenAssignMessage { ulassDestId = ProcessDest pid }

-- | Set another process or group to listen for name assignment and
-- unassignment.
listenAssignAsProxy :: DestId -> Process ()
listenAssignAsProxy listenerId = do
  sendEvent $ ListenAssignMessage { lassDestId = listenerId }

-- | Set another process or group to stop listening for name assignment and
-- unassignment.
unlistenAssignAsProxy :: DestId -> Process ()
unlistenAssignAsProxy listenerId = do
  sendEvent $ UnlistenAssignMessage { ulassDestId = listenerId }

-- | Assign a name to a process or group.
assign :: Name -> DestId -> Process ()
assign name did = do
  names <- nodeNames . procNode <$> Process St.get
  join . liftIO . atomically $ do
    names' <- readTVar names
    let (names'', new) =
          case M.lookup name names' of
            Just entries ->
              case S.findIndexL (\(did', _) -> did == did') entries of
                Just index ->
                  let entries' =
                        S.adjust (\(did', count) -> (did', count + 1))
                        index entries
                  in (M.insert name entries' names', False)
                Nothing -> (M.insert name (entries |> (did, 1)) names', False)
            Nothing -> (M.insert name (S.singleton (did, 1)) names', True)
    writeTVar names names''
    return . sendEvent $ AssignMessage { assName = name,
                                         assDestId = did,
                                         assNew = new }

-- | Unassign a name from a process or group.
unassign :: Name -> DestId -> Process ()
unassign name did = do
  names <- nodeNames . procNode <$> Process St.get
  join . liftIO . atomically $ do
    names' <- readTVar names
    let (names'', new) =
          case M.lookup name names' of
            Just entries ->
              case S.findIndexL (\(did', _) -> did == did') entries of
                Just index ->
                  case S.lookup index entries of
                    Just (_, count)
                      | count > 1 ->
                        let entries' =
                              S.adjust (\(did', count) -> (did', count - 1))
                              index entries
                        in (M.insert name entries' names', False)
                      | True ->
                          (M.insert name (S.deleteAt index entries) names',
                           index == 0)
                    Nothing -> (names', False)
                Nothing -> (names', False)
            Nothing -> (names', False)
    writeTVar names names''
    return . sendEvent $ UnassignMessage { uassName = name,
                                           uassDestId = did,
                                           uassNew = new }

-- | Try to look up a process or group by name.
tryLookup :: Name -> Process (Maybe DestId)
tryLookup name = do
  processInfo <- Process St.get
  names <- liftIO . atomically . readTVar . nodeNames $ procNode processInfo
  case M.lookup name names of
    Just entries ->
      case S.viewl entries of
        (did, _) :< _ -> return $ Just did
        EmptyL -> return Nothing
    Nothing -> return Nothing

-- | Do a blocking lookup of a process or group by name.
lookup :: Name -> Process DestId
lookup name = do
  processInfo <- Process St.get
  liftIO . atomically $ do
    names <- readTVar .  nodeNames $ procNode processInfo
    case M.lookup name names of
      Just entries ->
        case S.viewl entries of
          (did, _) :< _ -> return did
          EmptyL -> retry
      Nothing -> retry

-- | Look up all the names.
lookupAll :: Process (M.HashMap Name DestId)
lookupAll = do
  names <- nodeNames . procNode <$> Process St.get
  names' <- liftIO . atomically $ readTVar names
  return $ foldl' (\prevMap (name, entries) ->
            case S.viewl entries of
              (did, _) :< _ -> M.insert name did prevMap
              _ -> prevMap)
    M.empty (M.toList names')

-- | Generate a new group Id.
newGroupId :: Process GroupId
newGroupId = do
  processInfo <- Process St.get
  (nextSequenceNum, randomNum) <- liftIO . newUnique $ procNode processInfo
  return $ GroupId { gidSequenceNum = nextSequenceNum,
                     gidRandomNum = randomNum,
                     gidOriginalNodeId = Just . nodeId $ procNode processInfo }

-- | Generate a new process Id.
newProcessId :: Process ProcessId
newProcessId = do
  processInfo <- Process St.get
  (nextSequenceNum, randomNum) <- liftIO . newUnique $ procNode processInfo
  return $ ProcessId { pidSequenceNum = nextSequenceNum,
                       pidRandomNum = randomNum,
                       pidNodeId = nodeId $ procNode processInfo }

-- | Generate a new process Id for a node.
newProcessIdForNode :: Node -> IO ProcessId
newProcessIdForNode node = do
  (nextSequenceNum, randomNum) <- newUnique node
  return $ ProcessId { pidSequenceNum = nextSequenceNum,
                       pidRandomNum = randomNum,
                       pidNodeId = nodeId node }

-- | Generate a new unique Id.
newUniqueId :: Process UniqueId
newUniqueId = do
  processInfo <- Process St.get
  (nextSequenceNum, randomNum) <- liftIO . newUnique $ procNode processInfo
  return $ UniqueId { uidSequenceNum = nextSequenceNum,
                      uidRandomNum = randomNum,
                      uidNodeId = nodeId $ procNode processInfo }

-- | Generate a new node-unique Id.
newUnique :: Node -> IO (Integer, Integer)
newUnique node = do
  atomically $ do
    gen <- readTVar $ nodeGen node
    nextSequenceNum <- readTVar $ nodeNextSequenceNum node
    let (randomNum, gen') = R.random gen
    writeTVar (nodeGen node) gen'
    writeTVar (nodeNextSequenceNum node) $ nextSequenceNum + 1
    return (nextSequenceNum, randomNum)

-- | Connect to a local node.
connect :: Node -> Process ()
connect node = do
  sendEvent $ ConnectMessage { connNode = node }

-- | Connect to a remote node.
connectRemote :: Integer -> NS.SockAddr -> Maybe Integer -> Process ()
connectRemote fixedNum address randomNum = do
  let pnid = PartialNodeId { pnidFixedNum = fixedNum,
                             pnidAddress = Just $ toSockAddr' address,
                             pnidRandomNum = randomNum }
  sendEvent $ ConnectRemoteMessage { conrNodeId = pnid }

-- | Handle an exception.
catch :: E.Exception e => Process a -> (e -> Process a) -> Process a
catch action handler = do
  state <- Process $ St.get
  (value, state) <-
    liftIO $ E.catch (St.runStateT (let Process action' = action
                                    in action') state)
    (\e -> St.runStateT (let Process action' = handler e
                         in action') state)
  Process $ St.put state
  return value

-- | Handle an exception with a selection predicate.
catchJust :: E.Exception e => (e -> Maybe b) -> Process a -> (b -> Process a) ->
             Process a
catchJust pred action handler = do
  state <- Process $ St.get
  (value, state) <-
    liftIO $ E.catchJust pred (St.runStateT (let Process action' = action
                                             in action') state)
    (\b -> St.runStateT (let Process action' = handler b
                         in action') state)
  Process $ St.put state
  return value

-- | Handle an exception, with arguments reversed.
handle :: E.Exception e => (e -> Process a) -> Process a -> Process a
handle = flip catch

-- | Handle an exception with a selection predicate, with arguments reordered.
handleJust :: E.Exception e => (e -> Maybe b) -> (b -> Process a) ->
              Process a -> Process a
handleJust pred handler action = catchJust pred action handler

-- | Do the basic work of sending a message.
sendEvent :: LocalMessage -> Process ()
sendEvent message = do
  node <- Process $ procNode <$> St.get
  liftIO . atomically . writeTQueue (nodeQueue node) $
    LocalReceived { lrcvMessage = message `seq` message }

-- | Read messages from the queue.
receive :: S.Seq (Handler a) -> Process a
receive options = do
  alreadyReceived <- liftIO . atomically . readTVar . procExtra =<<
                     Process St.get
  found <- matchAndExecutePrefound options alreadyReceived
  case found of
    Just found -> return found
    Nothing -> matchUntilFoundThenExecute
  where matchUntilFoundThenExecute = do
          processInfo <- Process St.get
          message <- liftIO . atomically . readTQueue $ procQueue processInfo
          case match options message of
            Just action -> action
            Nothing -> do
              liftIO . atomically $ do
                extra <- readTVar $ procExtra processInfo
                writeTVar (procExtra processInfo) $ extra |> message
              matchUntilFoundThenExecute

-- | Read messages from the queue.
tryReceive :: S.Seq (Handler a) -> Process (Maybe a)
tryReceive options = do
  alreadyReceived <- liftIO . atomically . readTVar . procExtra =<<
                     Process St.get
  found <- matchAndExecutePrefound options alreadyReceived
  case found of
    Just found -> return $ Just found
    Nothing -> tryMatchThenExecute
  where tryMatchThenExecute = do
          processInfo <- Process St.get
          message <- liftIO . atomically . tryReadTQueue $ procQueue processInfo
          case message of
            Just message ->
              case match options message of
                Just action -> Just <$> action
                Nothing -> do
                  liftIO . atomically $ do
                    extra <- readTVar $ procExtra processInfo
                    writeTVar (procExtra processInfo) $ extra |> message
                  tryMatchThenExecute
            Nothing -> return Nothing

-- | Read messages from the queue with a timeout (in microseconds).
receiveWithTimeout :: Int -> S.Seq (Handler a) -> Process (Maybe a)
receiveWithTimeout timeout options = do
  alreadyReceived <- liftIO . atomically . readTVar . procExtra =<<
                     Process St.get
  found <- matchAndExecutePrefound options alreadyReceived
  case found of
    Just found -> return $ Just found
    Nothing -> do
      timedOut <- liftIO $ atomically newEmptyTMVar
      waiter <- liftIO . async $ do
        threadDelay timeout
        atomically $ putTMVar timedOut ()
      matchUntilFoundThenExecute timedOut waiter
  where matchUntilFoundThenExecute timedOut waiter = do
          processInfo <- Process St.get
          message <- liftIO . atomically $
            (Right <$> (readTQueue $ procQueue processInfo)) `orElse`
            (Left <$> readTMVar timedOut)
          case message of
            Right message ->
              case match options message of
                Just action -> do
                  liftIO $ cancel waiter
                  Just <$> action
                Nothing -> do
                  liftIO . atomically $ do
                    extra <- readTVar $ procExtra processInfo
                    writeTVar (procExtra processInfo) $ extra |> message
                  matchUntilFoundThenExecute timedOut waiter
            Left () -> return Nothing

-- | Attempt to match a list of already-received messages agianst a set of
-- options, and if one is found, execute the action.
matchAndExecutePrefound :: S.Seq (Handler a) -> S.Seq LocalMessage ->
                           Process (Maybe a)
matchAndExecutePrefound options messages = matchAndExecutePrefound' messages 0
  where matchAndExecutePrefound' alreadyReceived index =
          case S.viewl alreadyReceived of
            message :< rest ->
              case match options message of
                Just action -> do
                  processInfo <- Process St.get
                  liftIO . atomically $
                    writeTVar (procExtra processInfo)
                    (S.deleteAt index messages)
                  Just <$> action
                Nothing -> matchAndExecutePrefound' rest $ index + 1
            EmptyL -> return Nothing

-- | Match a message against a set of options
match :: S.Seq (Handler a) -> LocalMessage -> Maybe (Process a)
match options message =
  case message of
    UserMessage {..} ->
      case S.viewl options of
        handler :< rest ->
          case handler umsgMessage of
            action@(Just _) -> action
            Nothing -> match rest message
        EmptyL -> Nothing
    _ -> Nothing

-- | Encode data to a strict ByteString
encode :: B.Binary a => a -> BS.ByteString
encode = BSL.toStrict . B.encode
