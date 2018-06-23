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
   Handler,
   Entry,
   Header,
   Payload,
   Name,
   Key,
   ProcessId,
   NodeId,
   PartialNodeId,
   GroupId,
   SourceId (..),
   DestId (..),
   UserRemoteConnectFailed (..),
   UserRemoteDisconnected (..),
   MessageContainer (..),
   myProcessId,
   myNodeId,
   nodeIdOfProcessId,
   spawnInit,
   spawnInit',
   spawn,
   spawn',
   spawnAsProxy,
   spawnAsProxy',
   spawnListenEnd,
   spawnListenEnd',
   spawnListenEndAsProxy,
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
   sendAsProxy,
   receive,
   tryReceive,
   subscribe,
   unsubscribe,
   subscribeAsProxy,
   unsubscribeAsProxy,
   assign,
   unassign,
   tryLookup,
   lookup,
   newGroupId,
   connect,
   connectRemote,
   catch,
   catchJust,
   handle,
   listenEnd,
   unlistenEnd,
   listenEndAsProxy,
   unlistenEndAsProxy)

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
                           killThread)
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
import qualified Control.Monad.Trans.State.Strict as St
import qualified Control.Exception.Base as E
import Data.Functor ((<$>))
import Control.Monad ((=<<))
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

-- | Spawn a process on the local node without a preexisting process.
spawnInit :: Entry -> Node -> Header -> Payload -> IO ProcessId
spawnInit entry node header payload = do
  spawnedPid <- newProcessIdForNode node
  let message =
        SpawnMessage { spawnSourceId = NoSource,
                       spawnEntry = entry,
                       spawnProcessId = spawnedPid,
                       spawnHeader = header,
                       spawnPayload = payload,
                       spawnEndListeners = S.empty } 
  atomically . writeTQueue (nodeQueue node) $
    LocalReceived { lrcvMessage = message }
  return spawnedPid

-- | Spawn a process on the local node without a preexisting process without
-- instantiation parameters (because they are normally only needed for remote
-- spawns).
spawnInit' :: Process () -> Node -> IO ProcessId
spawnInit' action node = spawnInit (\_ _ _ -> action) node BS.empty BS.empty

-- | Spawn a process on the local node normally.
spawn :: Entry -> Header -> Payload -> Process ProcessId
spawn entry header payload = do
  pid <- myProcessId
  spawnedPid <- newProcessId
  sendRaw $ SpawnMessage { spawnSourceId = NormalSource pid,
                           spawnEntry = entry,
                           spawnProcessId = spawnedPid,
                           spawnHeader = header,
                           spawnPayload = payload,
                           spawnEndListeners = S.empty }
  return spawnedPid

-- | Spawn a process on the local node normally without instantiation parameters
-- (because they are normally only needed for remote spawns).
spawn' :: Process () -> Process ProcessId
spawn' action = spawn (\_ _ _ -> action) BS.empty BS.empty

-- | Spawn a process on the local node normally for another process.
spawnAsProxy :: Entry -> ProcessId -> Header -> Payload -> Process ()
spawnAsProxy entry pid header payload = do
  spawnedPid <- newProcessId
  sendRaw $ SpawnMessage { spawnSourceId = NormalSource pid,
                           spawnEntry = entry,
                           spawnProcessId = spawnedPid,
                           spawnHeader = header,
                           spawnPayload = payload,
                           spawnEndListeners = S.empty } 

-- | Spawn a process on the local node normally for another process without
-- instantiation parameters (because they are normally only needed for remote
-- spawns).
spawnAsProxy' :: Process () -> ProcessId -> Process ()
spawnAsProxy' action pid = spawnAsProxy (\_ _ _ -> action) pid BS.empty BS.empty

-- | Spawn a process on the local node normally and atomically listen for end.
spawnListenEnd :: Entry -> Header -> Payload -> S.Seq DestId ->
                  Process ProcessId
spawnListenEnd entry header payload endListeners = do
  pid <- myProcessId
  spawnedPid <- newProcessId
  sendRaw $ SpawnMessage { spawnSourceId = NormalSource pid,
                           spawnEntry = entry,
                           spawnProcessId = spawnedPid,
                           spawnHeader = header,
                           spawnPayload = payload,
                           spawnEndListeners = endListeners }
  return spawnedPid

-- | Spawn a process on the local node normally without instantiation parameters
-- (because they are normally only needed for remote spawns) and atomically
-- listen for end.
spawnListenEnd' :: Process () -> S.Seq DestId -> Process ProcessId
spawnListenEnd' action endListeners =
  spawnListenEnd (\_ _ _ -> action) BS.empty BS.empty endListeners

-- | Spawn a process on the local node normally for another process and
-- atomically listen for end.
spawnListenEndAsProxy :: Entry -> ProcessId -> Header -> Payload ->
                         S.Seq DestId -> Process ()
spawnListenEndAsProxy entry pid header payload endListeners = do
  spawnedPid <- newProcessId
  sendRaw $ SpawnMessage { spawnSourceId = NormalSource pid,
                           spawnEntry = entry,
                           spawnProcessId = spawnedPid,
                           spawnHeader = header,
                           spawnPayload = payload,
                           spawnEndListeners = endListeners } 

-- | Spawn a process on the local node normally for another process without
-- instantiation parameters (because they are normally only needed for remote
-- spawns) and atomically listen for End.
spawnListenEndAsProxy' :: Process () -> ProcessId -> S.Seq DestId -> Process ()
spawnListenEndAsProxy' action pid endListeners =
  spawnListenEndAsProxy (\_ _ _ -> action) pid BS.empty BS.empty endListeners

-- | Send a message to a process or group.
send :: DestId -> Header -> Payload -> Process ()
send did header payload = do
  pid <- myProcessId
  sendRaw $ UserMessage { umsgSourceId = NormalSource pid,
                          umsgDestId = did,
                          umsgHeader = header,
                          umsgPayload = payload }
  
-- | Send a message to a process or group for another process.
sendAsProxy :: DestId -> SourceId -> Header -> Payload -> Process ()
sendAsProxy did sid header payload = do
  sendRaw $ UserMessage { umsgSourceId = sid,
                          umsgDestId = did,
                          umsgHeader = header,
                          umsgPayload = payload }

-- | Quit the current process.
quit :: Header -> Payload -> Process ()
quit header payload = do
  pid <- myProcessId
  sendRaw $ QuitMessage { quitProcessId = pid,
                          quitHeader = header,
                          quitPayload = payload }
  threadId <- liftIO myThreadId
  liftIO $ killThread threadId

-- | Quit the current process with a generic quit message header and payload.
quit' :: Process ()
quit' = quit (encode ("genericQuit" :: T.Text)) BS.empty

-- | Kill another process or process group.
kill :: DestId -> Header -> Payload -> Process ()
kill did header payload = do
  pid <- myProcessId
  sendRaw $ KillMessage { killProcessId = pid,
                          killDestId = did,
                          killHeader = header,
                          killPayload = payload }

-- | Kill another process or process group with a generic kill message header
-- and payload.
kill' :: DestId -> Process ()
kill' did = kill did (encode ("genericKill" :: T.Text)) BS.empty

-- | Kill another process or process group for another process.
killAsProxy :: DestId -> ProcessId -> Header -> Payload -> Process ()
killAsProxy did pid header payload = do
  sendRaw $ KillMessage { killProcessId = pid,
                          killDestId = did,
                          killHeader = header,
                          killPayload = payload }

-- | Kill another process or process group for another process with a generic
-- kill message header and payload.
killAsProxy' :: DestId -> ProcessId -> Process ()
killAsProxy' did pid =
  killAsProxy did pid (encode ("genericKill" :: T.Text)) BS.empty

-- | Shutdown a node.
shutdown :: NodeId -> Header -> Payload -> Process ()
shutdown nid header payload = do
  pid <- myProcessId
  sendRaw $ ShutdownMessage { shutProcessId = pid,
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
shutdown' nid = shutdown nid (encode ("genericShutdown" :: T.Text)) BS.empty

-- | Shutdown a node for another process.
shutdownAsProxy :: NodeId -> ProcessId -> Header -> Payload -> Process ()
shutdownAsProxy nid pid header payload = do
  sendRaw $ ShutdownMessage { shutProcessId = pid,
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
  shutdownAsProxy nid pid (encode ("genericShutdown" :: T.Text)) BS.empty

-- | Subscribe to a group.
subscribe :: GroupId -> Process ()
subscribe gid = do
  pid <- myProcessId
  sendRaw $ SubscribeMessage { subProcessId = pid,
                               subGroupId = gid }

-- | Unsubscribe from a group.
unsubscribe :: GroupId -> Process ()
unsubscribe gid = do
  pid <- myProcessId
  sendRaw $ UnsubscribeMessage { usubProcessId = pid,
                                 usubGroupId = gid }

-- | Subscribe another process to a group.
subscribeAsProxy :: GroupId -> ProcessId -> Process ()
subscribeAsProxy gid pid = do
  sendRaw $ SubscribeMessage { subProcessId = pid,
                               subGroupId = gid }

-- | Unsubscribe another process from a group.
unsubscribeAsProxy :: GroupId -> ProcessId -> Process ()
unsubscribeAsProxy gid pid = do
  sendRaw $ UnsubscribeMessage { usubProcessId = pid,
                                 usubGroupId = gid }

-- | Listen for termination of another process or any member of a group.
listenEnd :: DestId -> Process ()
listenEnd listenedId = do
  pid <- myProcessId
  sendRaw $ ListenEndMessage { lendListenedId = listenedId,
                               lendListenerId = ProcessDest pid }

-- | Stop listening for termination of another process or any member of a group.
unlistenEnd :: DestId -> Process ()
unlistenEnd listenedId = do
  pid <- myProcessId
  sendRaw $ UnlistenEndMessage { ulendListenedId = listenedId,
                                 ulendListenerId = ProcessDest pid }

-- | Set another process or group to listen for termination of another process
-- or any member of a group.
listenEndAsProxy :: DestId -> DestId -> Process ()
listenEndAsProxy listenedId listenerId = do
  sendRaw $ ListenEndMessage { lendListenedId = listenedId,
                               lendListenerId = listenerId }

-- | Set another process or group to listen for termination of another process
-- or any member of a group.
unlistenEndAsProxy :: DestId -> DestId -> Process ()
unlistenEndAsProxy listenedId listenerId = do
  sendRaw $ UnlistenEndMessage { ulendListenedId = listenedId,
                                 ulendListenerId = listenerId }

-- | Assign a name to a process or group.
assign :: Name -> DestId -> Process ()
assign name did = do
  sendRaw $ AssignMessage { assName = name,
                            assDestId = did }
  names <- nodeNames . procNode <$> Process St.get
  names' <- liftIO . atomically $ readTVar names
  liftIO . atomically . writeTVar names $
    case M.lookup name names' of
      Just entries ->
        case S.findIndexL (\(did', _) -> did == did') entries of
          Just index ->
            let entries' =
                  S.adjust (\(did', count) -> (did', count + 1)) index entries
            in M.insert name entries' names'
          Nothing -> M.insert name (entries |> (did, 1)) names'
      Nothing -> M.insert name (S.singleton (did, 1)) names'

-- | Unassign a name from a process or group.
unassign :: Name -> DestId -> Process ()
unassign name did = do
  sendRaw $ UnassignMessage { uassName = name,
                              uassDestId = did }
  names <- nodeNames . procNode <$> Process St.get
  names' <- liftIO . atomically $ readTVar names
  liftIO . atomically . writeTVar names $
    case M.lookup name names' of
      Just entries ->
        case S.findIndexL (\(did', _) -> did == did') entries of
          Just index ->
            case S.lookup index entries of
              Just (_, count)
                | count > 1 ->
                  let entries' = S.adjust (\(did', count) -> (did', count - 1))
                                 index entries
                  in M.insert name entries' names'
                | True -> M.insert name (S.deleteAt index entries) names'
              Nothing -> names'
          Nothing -> names'
      Nothing -> names'

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

-- | genreate a new node-unique Id.
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
  sendRaw $ ConnectMessage { connNode = node }

-- | Connect to a remote node.
connectRemote :: Integer -> NS.SockAddr -> Maybe Integer -> Process ()
connectRemote fixedNum address randomNum = do
  let pnid = PartialNodeId { pnidFixedNum = fixedNum,
                             pnidAddress = Just $ toSockAddr' address,
                             pnidRandomNum = randomNum }
  sendRaw $ ConnectRemoteMessage { conrNodeId = pnid }

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
sendRaw :: Message -> Process ()
sendRaw message = do
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

-- | Attempt to match a list of already-received messages agianst a set of
-- options, and if one is found, execute the action.
matchAndExecutePrefound :: S.Seq (Handler a) -> S.Seq Message ->
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
match :: S.Seq (Handler a) -> Message -> Maybe (Process a)
match options message =
  case message of
    UserMessage {..} ->
      case S.viewl options of
        handler :< rest ->
          case handler umsgSourceId umsgDestId umsgHeader umsgPayload of
            action@(Just _) -> action
            Nothing -> match rest message
        EmptyL -> Nothing
    _ -> Nothing

-- | Encode data to a strict ByteString
encode :: B.Binary a => a -> BS.ByteString
encode = BSL.toStrict . B.encode
