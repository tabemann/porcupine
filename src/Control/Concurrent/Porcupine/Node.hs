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

{-# LANGUAGE OverloadedStrings, OverloadedLists, RecordWildCards #-}

module Control.Concurrent.Porcupine.Node

  (Node,
   start)

where

import Control.Concurrent.Porcupine.Private.Types
import qualified Data.ByteString.Lazy as BS
import qualified Data.Binary as B
import qualified Data.Sequence as S
import qualified Data.Text as T
import qualified Data.HashMap.Lazy as M
import qualified System.Random as R
import qualified Network.Socket as NS
import Data.Sequence (ViewL (..))
import Data.Sequence ((|>))
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
                                      writeQueue)
import Control.Concurrent.STM.TMVar (TMVar,
                                     newEmptyTMVar,
                                     putTMVar,
                                     takeTMVar)
import qualified Control.Monad.Trans.State.Strict as St
import Control.Monad.IO.Class (MonadIO (..))
import Control.Exception.Base (SomeException,
                               Exception (..),
                               AsyncException (..),
                               catch)
import Control.Monad (forM_)
import Data.Functor ((<$>),
                     fmap))

-- | Start a node with a given fixed index, optional address, and optional key.
start :: Integer -> Maybe NS.SockAddr -> Maybe ByteString -> IO Node
start fixedNum address key = do
  gen <- R.newStdGen
  (randomNum, gen') <- R.random gen
  let nid = NodeId { nidFixedNum = fixedNum,
                     nidAddress = toSockAddr' <$> address,
                     nidRandomNum = randomNum }
  queue <- atomically newTQueue
  remoteQueue <- atomically newTQueue
  names <- atomically $ newTVar M.empty
  gen'' <- atomically $ newTVar gen'
  nextSequenceNum <- atomically $ newTVar 0
  let info = Node { nodeId = nid,
                    nodeQueue = queue,
                    nodeRemoteQueue = remoteQueue,
                    nodeGen = gen'',
                    nodeNextSequenceNum = nextSequenceNum,
                    nodeNames = names }
      nodeState = NodeState { nodeInfo = info,
                              nodeKey = key,
                              nodeReadOrder = False,
                              nodeProcesses = S.empty,
                              nodeRemoteNodes = M.empty,
                              nodePendingRemoteNodes = S.empty
                              nodeGroups = S.empty }
  async $ St.runStateT runNode nodeState >> return ()
  return $ Node { nodeId = nid,
                  nodeOutput = input }

-- | Run a node.
runNode :: St.StateT NodeState IO ()
runNode = do
  readOrder <- nodeReadOrder <$> St.get
  input <- nodeQueue . nodeInfo <$> St.get
  remoteQueue <- nodeRemoteQueue . nodeInfo <$> St.get
  received <-
    if not readOrder
    then
      liftIO . atomically $ (Right <$> readTQueue input) `orElse`
      (Left <$> readTQueue remoteQueue)
    else
      liftIO . atomically $ (Left <$> readTQueue remoteQueue) `orElse`
      (Right <$> readTQuee input)
  St.modify $ \state -> state { nodeReadOrder = not readOrder }
  case received of
    Right message -> handleLocalMessage message
    Left event -> handleRemoteEvent event

-- | Handle a local message.
handleLocalMessage :: Message -> St.StateT NodeState IO ()
handleLocalMessage UserMessage{..} =
  handleLocalUserMessage umsgSourceId umsgDestId umsgHeader umsgPayload
handleLocalMessage SpawnMessage{..} =
  handleLocalSpawnMessage spawnSourceId spawnEntry spawnProcessId spawnHeader
  spawnPayload
handleLocalMessage QuitMessage{..} =
  handleLocalQuitMessage quitProcessId quitHeader quitPayload
handleLocalMessage EndMessage{..} =
  handleLocalEndMessage endProcessId endException
handleLocalMessage KillMessage{..} =
  handleLocalKillMessage killProcessId killDestId killHeader killPayload
handleLocalMessage SubscribeMessage{..} =
  handleLocalSubscribeMessage subProcessId subGroupId
handleLocalMessage UnsubscribeMessage{..} =
  handleLocalUnsubscribeMessage usubProcessId usubGroupId
handleLocalMessage AssignMessage{..} =
  handleLocalAssignMessage assName assDestId
handleLocalMessage UnassignMessage{..} =
  handleLocalUnassignMessage uassName uassDestId
handleLocalMessage ShutdownMessage{..} =
  handleLocalShutdownMessage shutProcessId shutNodeId shutHeader shutPayload
handleLocalMessage ConnectMessage{..} =
  handleLocalConnectMessage connNode
handleLocalMessage ConnectRemoteMessage{..} =
  handleLocalConnectRemoteMessage conrNodeId conrKey
handleLocalMessage ListenEndMessage{..} =
  handleLocalListenEndMessage lendListenedId lendListenerId
handleLocalMessage UnlistenEndMessage{..} =
  handleLocalUnlistenEndMessage ulendListenedId ulendListenerId
handleLocalMessage HelloMessage{..} =
  handleLocalHelloMessage heloNode
handleLocalMessage JoinMessage{..} =
  handleLocalJoinMessage joinNode

-- | Handle a remote event.
handleRemoteEvent :: RemoteEvent -> St.StateT NodeState IO ()
handleRemoteEvent RemoteReceived{..} =
  handleRemoteMessage recvNodeId recvMessage
handleRemoteDisconnected RemoteDisconnected{..} =
  handleRemoteDisconnected dconNodeId

-- | Handle a remote message.
handleRemoteMessage :: NodeID -> RemoteMessage -> St.StateT NodeState IO ()
handleRemoteMessage nodeId RemoteUserMessage{..} =
  handleRemoteUserMessage nodeId rumsgSourceId rumsgDestId rumsgHeader
  rumsgPayload
handleRemoteMessage nodeId RemoteEndMessage{..} =
  handleRemoteEndMessage nodeId rendSourceId rendHeader rendPayload
handleRemoteMessage nodeId RemoteKillMessage{..} =
  handleRemoteKillMessage nodeId rkillProcessId rkillDestId rkillHeader
  rkillPayload
handleRemoteMessage nodeId RemoteSubscribeMessage{..} =
  handleRemoteSubscribeMessage nodeId rsubProcessId rsubGroupId
handleRemoteMessage nodeId RemoteUnsubscribeMessage{..} =
  handleRemoteUnsubscribeMessage nodeId rusubProcessId rusubGroupId
handleRemoteMessage nodeId RemoteAssignMessage{..} =
  handleRemoteAssignMessage nodeId rassName rassDestId
handleRemoteMessage nodeId RemoteUnassignMessage{..} =
  handleRemoteUnassignMessage nodeId ruassName ruassDestId
handleRemoteMessage nodeId RemoteShutdownMessage{..} =
  handleRemoteShutdownMessage nodeId rshutProcessId rshutHeader rshutPayload
handleRemoteMessage nodeId RemoteHelloMessage{..} =
  handleRemoteHelloMessage nodeId rheloNodeId rheloKey
handleRemoteMessage nodeId RemoteListenEndMessage{..} =
  handleRemoteListenEndMessage nodeId rlendListenedId rlendListenerId
handleRemoteMessage nodeId RemoteUnlistenEndMessage{..} =
  handleRemoteUnlistenEndMessage nodeId rulendListenedId rulendListenerId
handleRemoteMessage nodeId RemoteJoinMessage{..} =
  handleRemoteJoinMessage nodeId rjoinNodeId

-- | Handle a remote disconnected event.
handleRemoteDisconnected :: NodeId -> St.StateT NodeState IO ()
handleRemoteDisconnected = runNode

-- | Handle a local user message.
handleLocalUserMessage :: SourceId -> DestId -> Header -> Payload ->
                          St.StateT NodeState IO ()
handleLocalUserMessage sourceId destId header payload = do
  sendUserMessage destId sourceId header payload
  runNode

-- | Handle a local spawn message.
handleLocalSpawnMessage :: SourceId -> Entry -> ProcessId -> Header ->
                           Payload -> St.StateT NodeState IO ()
handleLocalSpawnMessage sourceId entry processId header payload = do
  state <- St.get
  queue <- liftIO $ atomically newTQueue
  extra <- liftIO . atomically $ newTVar S.empty
  let processInfo = ProcessInfo { procId = processId,
                                  procQueue = queue,
                                  procExtra = extra,
                                  procNode = nodeInfo state }
  asyncThread <- liftIO . async $ do
    maybeException <-
      catch (do St.runState (let Process procState =
                                   entry sourceId header payload
                             in procState) processInfo
                return Nothing) $ \e -> return $ Just e
    atomically . writeTQueue (nodeQueue $ nodeInfo state) $
      EndMessage { endProcessId = processId, endException = maybeException }
  let processState = ProcessState { pstateInfo = processInfo,
                                    pstateAsync = asyncThread,
                                    pstateEndMessage = Nothing,
                                    pstateEndCause = Nothing,
                                    pstateEndListeners = S.empty }
  St.modify $ \state ->
    state { nodeProcesses = M.insert processId processState nodeProcesses }
  runNode

-- | Handle a local quit message.
handleLocalQuitMessage :: ProcessId -> Header -> Payload ->
                          St.StateT NodeState IO ()
handleLocalQuitMessage pid header payload = do
  updateProcess (\process ->
                    if not $ pstateTerminating process
                    then
                      process {
                        pstateEndMessage =
                          case pstateEndMessage of
                            Just _ -> pstateEndMessage
                            Nothing -> Just (header, payload),
                        pstateTerminating = True }
                    else process)
    pid
  runNode

-- | Handle a local end message.
handleLocalEndMessage :: ProcessId -> Maybe SomeException ->
                         St.StateT NodeState IO ()
handleLocalEndMessage processId exception = do
  removeListeners processId
  state <- St.get
  case M.lookup processId $ nodeProcesses state of
    Just process -> do
      removeProcess processId
      let message = case exception of
            Just exception ->
              let asyncException = (fromException exception) :: AsyncException
              in case asyncException of
                Just asyncException
                  | asyncException /= ThreadKilled &&
                    asyncException /= UserInterrupt ->
                      Just (B.encode $ "exceptionExit" :: T.Text,
                             B.encode . T.pack $ show exception)
                  | True -> Nothing
                Nothing -> 
                  Just (B.encode $ "exceptionExit" :: T.Text,
                         B.encode . T.pack $ show exception)
            Nothing -> Nothing
      sendEndMessageForProcess process message
  runNode

-- | Handle a local kill message.
handleLocalKillMessage :: ProcessId -> DestId -> Header -> Payload ->
                          St.StateT NodeState IO ()
handleLocalKillMessage processId destId header payload = do
  case destId of
    ProcessDest destPid -> killProcess processId destPid header payload
    GroupDest destGid -> killGroup processId destGid header payload
  runNode

-- | Handle a local subscribe message.
handleLocalSubscribeMessage :: ProcessId -> GroupId -> St.StateT NodeState IO ()
handleLocalSubscribeMessage pid gid = do
  state <- St.get
  case M.lookup gid $ nodeGroups state of
    Just group ->
      case S.findIndexL (\(pid', _) -> pid == pid') $
           groupLocalSubscribers group of
        Just index -> do
          updateGroup
            (\group -> group { groupLocalSubscribers =
                                 S.adjust (\(pid, count) -> (pid, count + 1))
                                 index $ groupRemoteSubscribers group })
            gid
        Nothing -> do
          updateGroup
            (\group -> group { groupLocalSubscribers =
                                 groupLocalSubscribers group |> (pid, 1) })
            gid
    Nothing -> do addGroup gid $
                    GroupState { groupId = gid,
                                 groupLocalSubscribers = S.empty,
                                 groupRemoteSubscribers = S.singleton (pid, 1) }
  broadcastRemoteMessage $ RemoteSubscribeMessage { rsubProcessId = pid,
                                                    rsubGroupId = gid }
  runNode

-- | Handle a local unsubscribe message.
handleLocalUnsubscribeMessage :: ProcessId -> GroupId ->
                                 St.StateT NodeState IO ()
handleLocalUnsubscribeMessage pid gid = do
  state <- St.get
  case M.lookup gid $ nodeGroups state of
    Just group ->
      case S.findIndexL (\(pid', _) -> pid == pid') $
           groupLocalSubscribers group of
        Just index ->
          case S.lookup index $ groupLocalSubscribers group of
            Just (_, count)
              | count > 1 -> do
                  updateGroup
                    (\group ->
                        group { groupLocalSubscribers =
                                  S.adjust (\(pid, count) -> (pid, count - 1))
                                  index $ groupLocalSubscribers group })
                    gid
              | True -> do
                  updateGroup
                    (\group ->
                        group { groupLocalSubscribers =
                                  S.deleteAt index $
                                  groupLocalSubscribers group })
                    gid
            Nothing -> error "impossible"
        Nothing -> return ()
    Nothing -> return ()
  broadcastRemoteMessage $ RemoteUnsubscribeMessage { rusubProcessId = pid,
                                                      rusubGroupId = gid }
  runNode

-- | Handle a local assign message.
handleLocalAssignMessage :: Name -> DestId -> St.StateT NodeState IO ()
handleLocalAssignMessage name destId = do
  broadcastRemoteMessage $ RemoteAssignMessage { rassName = name,
                                                 rassDestId = destId }
  runNode

-- | Handle a local unassign message.
handleLocalUnassignMessage :: Name -> DestId -> St.StateT NodeState IO ()
handleLocalUnassignMessage name destId = do
  broadcastRemoteMessage $ RemoteUnassignMessage { ruassName = name,
                                                   ruassDestId = destId }
  runNode

-- | Handle a local shutdown message.
handleLocalShutdownMessage :: ProcessId -> NodeId -> Header -> Payload ->
                              St.StateT NodeState IO ()
handleLocalShutdownMessage pid nid header payload = do
  nid' <- nodeId . nodeInfo <$> St.get
  if nid == nid'
    then shutdownLocalNode pid header payload
    else do sendRemoteMessage nid $
              RemoteShutdownMessage { rshutProcessId = pid,
                                      rshutHeader = header,
                                      rshutPayload = payload }
            runNode

-- | Handle a local connect message.
handleLocalConnectMessage :: Node -> St.StateT NodeState IO ()
handleLocalConnectMessage node = do
  connectLocalMessage node
  runNode

-- | Handle a local connect remote message.
handleLocalConnectRemoteMessage :: NodeId -> Key -> St.StateT NodeState IO ()
handleLocalConnectRemoteMessage nid key = runNode

-- | Handle a local listen end message.
handleLocalListenEndMessage :: DestId -> DestId -> St.StateT NodeState IO ()
handleLocalListenEndMessage listenedId listenerId = runNode

-- | handle a local unlisten end message.
handleLocalUnlistenEndMessage :: DestId -> DestId -> St.StateT NodeState IO ()
handleLocalUnlistenEndMessage listenedId listenerId = runNode

-- | Handle a local hello message.
handleLocalHelloMessage :: Node -> St.StateT NodeState IO ()
handleLocalHelloMessage node = do
  state <- St.get
  case M.lookup (nodeId node) $ nodeLocalNodes state of
    Nothing -> connectLocal node
    Just _ -> return ()
  runNode

-- | Handle a local join message.
handleLocalJoinMessage :: Node -> St.StateT NodeState IO ()
handleLocalJoinMessage node = do
  state <- St.get
  case M.lookup (nodeId node) $ nodeLocalNodes state of
    Nothing -> connectLocalWithoutBroadcast node
    Just _ -> return ()
  runNode

-- | Handle a remote user message.
handleRemoteUserMessage :: NodeId -> SourceId -> DestId -> Header -> Payload ->
                           St.StateT NodeState IO ()
handleRemoteUserMessage nid sourceId destId header payload = runNode

-- | Handle a remote process end message.
handleRemoteEndMessage :: NodeId -> SourceId -> Header -> Payload ->
                          St.StateT NodeState IO ()
handleRemoteEndMessage nid sourceId header payload = runNode

-- | Handle a remote kill message.
handleRemoteKillMessage :: NodeId -> SourceId -> DestId -> Header -> Payload ->
                           St.StateT NodeState IO ()
handleRemoteKillMessage nid processId destId header payload =
  case destId of
    ProcessDest destPid -> killLocalProcess processId destPid header payload
    GroupDest destGid -> killGroupForRemote processId destGid header payload
  runNode

-- | Handle a remote subscribe message.
handleRemoteSubscribeMessage :: NodeId -> ProcessId -> GroupId ->
                                St.StateT NodeState IO ()
handleRemoteSubscribeMessage nid _ gid = do
  state <- St.get
  case M.lookup gid $ nodeGroups state of
    Just group ->
      case S.findIndexL (\(nid', _) -> nid == nid') $
           groupRemoteSubscribers group of
        Just index -> do
          updateGroup
            (\group -> group { groupRemoteSubscribers =
                                 S.adjust (\(nid, count) -> (nid, count + 1))
                                 index $ groupRemoteSubscribers group })
            gid
        Nothing -> do
          updateGroup
            (\group -> group { groupRemoteSubscribers =
                                 groupRemoteSubscribers group |> (nid, 1) })
            gid
    Nothing -> do addGroup gid $
                    GroupState { groupId = gid,
                                 groupLocalSubscribers = S.empty,
                                 groupRemoteSubscribers = S.singleton (nid, 1) }
  runNode

-- | Handle a remote unsubscribe message.
handleRemoteUnsubscribeMessage :: NodeId -> ProcessId -> GroupId ->
                                  St.StateT NodeState IO ()
handleRemoteUnsubscribeMessage nid _ gid = do
  state <- St.get
  case M.lookup gid $ nodeGroups state of
    Just group ->
      case S.findIndexL (\(nid', _) -> nid == nid') $
           groupRemoteSubscribers group of
        Just index ->
          case S.lookup index $ groupRemoteSubscribers group of
            Just (_, count)
              | count > 1 -> do
                updateGroup
                  (\group ->
                     group { groupRemoteSubscribers =
                               S.adjust (\(nid, count) -> (nid, count - 1))
                               index $ groupRemoteSubscribers group })
                  gid
              | True -> do
                  updateGroup
                    (\group ->
                       group { groupRemoteSubscribers =
                                 S.deleteAt index $
                                 groupRemoteSubscribers group })
                    gid
            Nothing -> error "impossible"
        Nothing -> return ()
    Nothing -> return ()
  runNode

-- | Handle a remote assign message.
handleRemoteAssignMessage :: NodeId -> Name -> DestId ->
                             St.StateT NodeState IO ()
handleRemoteAssignMessage _ name destId = do
  names <- nodeNames . nodeInfo <$> St.get
  liftIO . atomically $ do
    nameMap <- readTVar names
    writeTVar names $ M.insert name destId nameMap
  runNode

-- | Handle a remote unassign message.
handleRemoteUnassignMessage :: NodeId -> Name -> DestId ->
                               St.StateT NodeState IO ()
handleRemoteUnassignMessage _ name destId = do
  names <- nodeNames . nodeInfo <$> St.get
  liftIO . atomically $ do
    nameMap <- readTVar names
    writeTVar names $ M.delete name nameMap
  runNode

-- | Handle a remote shutdown message.
handleRemoteShutdownMessage :: NodeId -> ProcessId -> Header -> Payload ->
                               St.StateT NodeState IO ()
handleRemoteShutdownMessage nid pid header payload = runNode

-- | Handle a remote hello message.
handleRemoteHelloMessage :: NodeId -> NodeId -> Key -> St.StateT NodeState IO ()
handleRemoteHelloMessage nid nid' key = runNode

-- | Handle a remote listen end message.
handleRemoteListenEndMessage :: NodeId -> DestId -> DestId ->
                                St.StateT NodeState IO ()
handleRemoteListenEndMessage nid listenedId listenerId = runNode

-- | Handle a remote unlisten end message.
handleRemoteUnlistenEndMessage :: NodeId -> DestId -> DestId ->
                                  St.StateT NodeState IO ()
handleRemoteUnlistenEndMessage nid listenedId listenerId = runNode

-- | Handle a remote join message.
handleRemoteJoinMessage :: NodeId -> NodeId -> St.StateT NodeState IO ()
handleRemoteJoinMessage nid remoteNid = runNode

-- | Send a user message to a process.
sendUserMessageToProcess :: DestId -> SourceId -> Header -> Payload ->
                            St.StateT NodeState IO ()
sendUserMessage destId sourceId header payload = do
  state <- St.get
  case destId of
    ProcessDest pid ->
      if pidNodeId pid == nodeId (nodeInfo state)
      then case M.lookup pid $ nodeProcesses state of
             Just process -> do
               liftIO . atomically $ do
                 writeTQueue (procInput process) $
                   UserMessage { umsgSourceId = sourceId,
                                 umsgDestId = destId,
                                 umsgHeader = header,
                                 umsgPayload = payload }
             Nothing -> return ()
      else do sendRemoteNode (pidNodeId pid) $
                RemoteUserMessage { rumsgSourceId = sourceId,
                                    rumsgDestId = destId,
                                    rumsgHeader = header,
                                    rumsgPayload = payload }
    GroupDest gid ->
      case M.lookup gid $ nodeGroups state of
        Just group -> do
          forM_ (groupLocalSubscribers group) $ \(pid, _) ->
            case M.lookup pid $ nodeProcesses state of
              Just process -> do
                liftIO . atomically $ do
                  writeTQueue (procInput process) $
                    UserMessage { umsgSourceId = sourceId,
                                  umsgDestId = destId,
                                  umsgHeader = header,
                                  umsgPayload = payload }
              Nothing -> return ()
          forM_ (groupRemoteSubscribers group) $ \(nid, _) -> do
            sendRemoteNode nid $
              RemoteUserMessage { rumsgSourceId = sourceId,
                                  rumsgDestId = destId,
                                  rumsgHeader = header,
                                  rumsgPayload = payload }
-- | Send a locall message.
sendLocalMessage :: NodeId -> Message -> St.StateT NodeState IO ()
sendLocalMessage nid message = do
  state <- St.get
  case M.lookup nid $ nodeLocalNodes state of
    Just node -> liftIO . atomically $ writeTQueue (nodeQueue node) message
    Nothing -> return ()

-- | Send a remote message.
sendRemoteMessage :: NodeId -> RemoteMessage -> St.StateT NodeState IO ()
sendRemoteMessage nid message = do
  state <- St.get
  case M.lookup nid $ nodeRemoteNodes state of
    Just rnode -> liftIO . atomically $ writeTQueue (rnodeOutput rnode) message
    Nothing ->
      let index = (flip S.findIndexL) (nodePendingRemoteNodes state) $ \pnode ->
            let pnid = prnodeId pnode
            in pnidFixedNum pnid == nidFixedNum nid &&
               pnidAddress pnid == nidAddresss nid
      in case index of
        Just index ->
          case S.lookup index $ nodePendingRemoteNodes state of
            Just pnode ->
              liftIO . atomically $ writeTQueue (prnodeOutput pnode) message
            Nothing -> error "impossible"
        Nothing -> return ()

-- | Broadcast a remote message.
broadcastRemoteMessage :: RemoteMessage -> St.StateT NodeState IO ()
broadcastRemoteMessage message = do
  state <- St.get
  forM_ (nodeRemoteNodes state) $ \rnode ->
    liftIO . atomically $ writeTQueue (rnodeOutput rnode) message
  forM_ (nodePendingRemoteNodes state) $ \pnode ->
    liftIO . atomically $ writeTQueue (prnodeOutput pnode) message

-- | Remove listeners for a process Id
removeListeners :: ProcessId -> St.StateT NodeState IO ()
removeListeners processId = do
  St.modify $ \state -> state {
    nodeGroups =
      (flip fmap) (nodeGroups state) $ \group ->
        let localSubscribers = S.filter (\(pid, _) -> pid /= processId) $
                               groupLocalSubscribers group
        in group { groupLocalSubscribers = localSubscribers },
    nodeProcesses =
      (flip fmap) (nodeProcesses state) $ \process ->
        let endListeners =
              (flip S.filter) (pstateEndListeners process) $ \(did, _) ->
                case did of
                  ProcessDest pid -> pid /= processId
                  GroupDest _ -> True
        in process { pstateEndListeners = endListeners },
    nodeRemoteNodes =
      (flip fmap) (nodeRemoteNodes state) $ \node ->
        let endListeners =
              (flip S.filter) (rnodeEndListeners node) $ \(did, _) ->
              case did of
                ProcessDest pid -> pid /= processId
                GroupDest _ -> True
        in node { rnodeEndListeners = endListeners },
    nodePendingRemoteNodes =
      (flip fmap) (nodePendingRemoteNodes state) $ \pnode ->
        let endListeners =
              (flip S.filter) (prnodeEndListeners pnode) $ \(did, _) ->
              case did of
                ProcessDest pid -> pid /= processId
                GroupDest _ -> True
        in pnode { prnodeEndListeners = endListeners } }

-- | Remove a process
removeProcess :: ProcessId -> St.StateT NodeState IO ()
removeProcess processId = do
  St.modify $ \state ->
    state { nodeProcesses = M.delete processId nodeProcesses }

-- | Send message to all end listeners of a process
sendEndMessageForProcess :: ProcessState -> Maybe (Header, Payload) ->
                            St.StateT NodeState IO ()
sendEndMessageForProcess process message = do
  let (header, payload) =
        case pstateEndMessage process of
          Just message -> message
          Nothing ->
            case message of
              Just message -> message
              Nothing -> (encode $ "genericEnd" :: T.Text, BS.empty)
      sourceId =
        case pstateEndCause process of
          Just causeId ->
            CauseSource { causeSourceId = procId $ pstateInfo process,
                          causeCauseId = causeId }
          Nothing -> NormalSource . procId $ pstateInfo process
  forM_ (pstateEndListeners process) $ \(did, _) ->
    sendUserMessage did sourceId header payload

-- | Kill a process.
killProcess :: ProcessId -> ProcessId -> Header -> Payload ->
               St.StateT NodeState IO ()
killProcess pid targetPid header payload = do
  node <- nodeInfo <$> St.get
  if pidNodeId processId == nodeId node
    then killLocalProcess pid targetPid header payload
    else sendRemoteMessage (pidNodeId targetPid) $
         RemoteKillMessage { rkillProcessId pid,
                             rkillDestId = SourceDest targetPid,
                             rkillHeader = header,
                             rkillPayload = payload }

-- | Kill a local process.
killLocalProcess :: ProcessId -> ProcessId -> Header -> Payload ->
                    St.StateT NodeState IO ()
killLocalProcess sourcePid destPid header payload = do
  state <- St.get
  case M.lookup destPid $ nodeProcesses state of
    Just process ->
      if not $ pstateTerminating process
      then do
        liftIO . cancel $ pstateAsync process
        updateProcess (\process ->
                          process {
                            pstateEndMessage =
                              case pstateEndMessage of
                                Just _ -> pstateEndMessage
                                Nothing -> Just (header, payload),
                            pstateEndCause = sourcePid,
                            pstateTerminating = True })
          pid
      else return ()
    Nothing -> return ()

-- | Kill a group.
killGroup :: SourceId -> GroupId -> Header -> Payload ->
             St.StateT NodeState IO ()
killGroup sourcePid destGid header payload = do
  state <- St.get
  case M.lookup destGid $ nodeGroups state of
    Just group -> do
      forM_ (groupLocalSubscribers group) $ \(pid, _) ->
        killLocalProcess $ NormalSource pid
      forM_ (groupRemoteSubscribers group) $ \(nid, _) -> do
        sendRemoteMessage nid $
          RemoteKillMessage { rkillProcessId = sourceId,
                              rkillDestId = GroupDest destGid,
                              rkillHeader = header,
                              rkillPayload = payload }
    Nothing -> return ()

-- | Kill a group in response to a remote message.
killGroupForRemote :: SourceId -> GroupId -> Header -> Payload ->
                      St.StateT NodeState IO ()
killGroupForRemote sourceId gid header payload = do
  state <- St.get
  case M.lookup destGid $ nodeGroups state of
    Just group -> do
      forM_ (groupLocalSubscribers group) $ \(pid, _) ->
        killLocalProcess $ NormalSource pid
    Nothing -> return ()

-- | Update a process Id.
updateProcess :: (ProcessState -> ProcessState) -> ProcessId ->
                 St.StateT NodeState IO ()
updateProcess f pid =
  St.modify $ \state ->
                state { nodeProcesses = M.adjust f pid $ nodeProcesses state }

-- | Update a group Id.
updateGroup :: (GroupState -> GroupState) -> GroupId ->
               St.StateT NodeState IO ()
updateGroup f gid =
  St.modify $ \state ->
                state { nodeGroups = M.adjust f gid $ nodeGroups state }

-- | Add a group.
addGroup :: GroupId -> GroupState -> St.StateT NodeState IO ()
addGroup gid group = do
  St.modify $ \state ->
                state { nodeGroups = M.insert gid group $ nodeGroups state }

-- | Shutdown the local node.
shutdownLocalNode :: ProcessId -> Header -> Payload -> St.StateT NodeState IO ()
shutdownLocalNode pid header payload = do
  state <- St.get
  broadcastRemoteMessage RemoteLeaveMessage
  liftIO . atomically $ do
    forM_ (M.elems $ nodeRemoteNodes state) $ \node ->
      putTMVar (rnodeTerminate node) ()
  St.modify $ \state -> state { nodeRemoteNodes = S.empty }
  
-- | Start local communication with another node.
startLocalCommunication :: Node -> St.State NodeState IO ()
startLocalCommunication node = do
  output <- liftIO $ atomically newTQueue
  terminate <- liftIO $ atomically newEmptyTMVar
  let remoteNodeState =
        RemoteNodeState { rnodeId = nodeId node,
                          rnodeOutput = output,
                          rnodeTerminate = terminate,
                          rnodeEndListeners = S.empty }
  St.modify $ \state ->
    state { nodeRemoteNodes = M.insert (nodeId node) remoteNodeState $
                              nodeRemoteNodes state }
  selfNid <- nodeId . nodeInfo <$> St.get
  liftIO . async $ runLocalCommunication selfNid output terminate

-- | Run local communication with another node
runLocalCommunication :: NodeId -> TQueue RemoteMessage -> TMVar () -> IO ()
runLocalCommunication selfNid output terminate = do
  event <- atomically $ do
    (Right <$> readTQueue output) `orElse` (Left <$> takeTMVar terminate)
  case event of
    Right remoteMessage -> do
      atomically . writeTQueue (nodeRemoteQueue node) $
        RemoteReceived { recvNodeId = selfNid,
                         recvMessage = remoteMessage }
      runLocalCommunication selfNid output terminate
    Left () -> do
      atomically . writeTQueue (nodeRemoteQueue node) $
        RemoteDisconnected { dconNodeId = selfNid }
             
-- | Connect to a local node.
connectLocal :: Node -> St.StateT NodeState IO ()
connectLocal node = do
  localNodes <- findLocalNodes
  startLocalCommunication node
  nids <- M.keys . nodeLocalNodes <$> St.get
  St.modify $ \state -> do
    state { nodeLocalNodes =
              M.insert (nodeId node) node $ nodeLocalNodes state }
  forM_ nides $ \nid -> sendLocalMessage nid $ JoinMessage { joinNode = node }
  selfNode <- nodeInfo <$> St.get
  sendLocalMessage (nodeId node) $ HelloMessage { heloNode = selfNode }

-- | Connect to a local node without broadcasting join messages.
connectLocalWithoutBroadcast :: Node -> St.StateT NodeState IO ()
connectLocalWithoutBroadcast node = do
  localNodes <- findLocalNodes
  startLocalCommunication node
  nids <- M.keys . nodeLocalNodes <$> St.get
  St.modify $ \state -> do
    state { nodeLocalNodes =
              M.insert (nodeId node) node $ nodeLocalNodes state }
  selfNode <- nodeInfo <$> St.get
  sendLocalMessage (nodeId node) $ HelloMessage { heloNode = selfNode }
  
