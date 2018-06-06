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
import qualified Network.Socket.ByteString as NSB
import qualified Data.ByteString as BSS
import Data.Word (Int64)
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
                                     tryPutTMVar,
                                     takeTMVar,
                                     readTMVar)
import qualified Control.Monad.Trans.State.Strict as St
import Control.Monad.IO.Class (MonadIO (..))
import Control.Exception.Base (SomeException,
                               Exception (..),
                               AsyncException (..),
                               IOException (..),
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
runNode :: NodeM ()
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
handleLocalMessage :: Message -> NodeM ()
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
handleRemoteEvent :: RemoteEvent -> NodeM ()
handleRemoteEvent RemoteConnected{..} =
  handleRemoteConnected rconNodeId
handleRemoteEvent RemoteReceived{..} =
  handleRemoteMessage recvNodeId recvMessage
handleRemoteDisconnected RemoteDisconnected{..} =
  handleRemoteDisconnected dconNodeId

-- | Handle a remote message.
handleRemoteMessage :: NodeID -> RemoteMessage -> NodeM ()
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

-- | Handle a remote connected event.
handleRemoteConnected :: NodeId -> NS.Socket -> NodeM ()
handleRemoteConnected nid sock = do
  pnode <- findPendingRemoteNode nid
  case pnode of
    Just pnode -> do
      St.modify $ \state ->
        let rnode = RemoteNodeState { rnodeId = nid,
                                      rnodeOutput = prnodeOutput pnode,
                                      rnodeTerminate = prnodeTerminate pnode,
                                      rnodeEndListeners =
                                        prnodeEndListeners pnode }
        in state { nodePendingRemoteNodes =
                     S.filter (\pnode' -> not $ matchPartialNodeId
                                          (prnodeId pnode) nid) $
                     nodePendingRemoteNodes state,
                   nodeRemoteNodes =
                     M.insert nid rnode $ nodeRemoteNodes state }
      broadcastExceptRemoteMessage (prnodeId pnode) $
        RemoteJoinMessage { rjoinNodeId = nid }
    Nothing -> do
      output <- liftIO $ atomically newTQueue
      terminate <- liftIO $ atomically newEmptyTMVar
      St.modify $ \state ->
        let rnode = RemoteNodeState { rnodeId = nid,
                                      rnodeOutput = output,
                                      rnodeTerminate = terminate,
                                      rnodeEndListeners = S.empty }
        in state { nodeRemoteNodes =
                     M.insert nid rnode $ nodeRemoteNodes state }
      runSocket nid output terminate socket
  runNode
      
-- | Handle a remote disconnected event.
handleRemoteDisconnected :: NodeId -> NodeM ()
handleRemoteDisconnected = runNode

-- | Handle a local user message.
handleLocalUserMessage :: SourceId -> DestId -> Header -> Payload -> NodeM ()
handleLocalUserMessage sourceId destId header payload = do
  sendUserMessage destId sourceId header payload
  runNode

-- | Handle a local spawn message.
handleLocalSpawnMessage :: SourceId -> Entry -> ProcessId -> Header ->
                           Payload -> NodeM ()
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
handleLocalQuitMessage :: ProcessId -> Header -> Payload -> NodeM ()
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
handleLocalEndMessage :: ProcessId -> Maybe SomeException -> NodeM ()
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
handleLocalKillMessage :: ProcessId -> DestId -> Header -> Payload -> NodeM ()
handleLocalKillMessage processId destId header payload = do
  case destId of
    ProcessDest destPid -> killProcess processId destPid header payload
    GroupDest destGid -> killGroup processId destGid header payload
  runNode

-- | Handle a local subscribe message.
handleLocalSubscribeMessage :: ProcessId -> GroupId -> NodeM ()
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
                                 groupRemoteSubscribers = S.singleton (pid, 1),
                                 groupEndListeners = S.empty }
  broadcastRemoteMessage $ RemoteSubscribeMessage { rsubProcessId = pid,
                                                    rsubGroupId = gid }
  runNode

-- | Handle a local unsubscribe message.
handleLocalUnsubscribeMessage :: ProcessId -> GroupId -> NodeM ()
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
handleLocalAssignMessage :: Name -> DestId -> NodeM ()
handleLocalAssignMessage name destId = do
  broadcastRemoteMessage $ RemoteAssignMessage { rassName = name,
                                                 rassDestId = destId }
  runNode

-- | Handle a local unassign message.
handleLocalUnassignMessage :: Name -> DestId -> NodeM ()
handleLocalUnassignMessage name destId = do
  broadcastRemoteMessage $ RemoteUnassignMessage { ruassName = name,
                                                   ruassDestId = destId }
  runNode

-- | Handle a local shutdown message.
handleLocalShutdownMessage :: ProcessId -> NodeId -> Header -> Payload ->
                              NodeM ()
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
handleLocalConnectMessage :: Node -> NodeM ()
handleLocalConnectMessage node = do
  connectLocalMessage node
  runNode

-- | Handle a local connect remote message.
handleLocalConnectRemoteMessage :: NodeId -> Key -> NodeM ()
handleLocalConnectRemoteMessage nid key = runNode

-- | Handle a local listen end message.
handleLocalListenEndMessage :: DestId -> DestId -> NodeM ()
handleLocalListenEndMessage listenedId listenerId = do
  case listenedId of
    ProcessDest pid -> do
      nid <- nodeId . nodeInfo <$> St.get
      if pidNodeId pid == nid
        then registerLocalProcessEndListener pid listenerId
        else do
          registerRemoteNodeEndListener (pidNodeId pid) listenerId
          sendRemoteMessage (pidNodeId pid) $
            RemoteListenEndMessage { rlendListenedId = listenedId,
                                     rlendListenerId = listenerId }
    GroupDest gid -> do
      registerGroupEndListener gid listenerId
      broadcastRemoteMessage $
        RemoteListenEndMessage { rlendListenedId = listenedId,
                                 rlendListenerId = listenerId }
  runNode

-- | handle a local unlisten end message.
handleLocalUnlistenEndMessage :: DestId -> DestId -> NodeM ()
handleLocalUnlistenEndMessage listenedId listenerId = do
  case listenedId of
    ProcessDest pid -> do
      nid <- nodeId . nodeInfo <$> St.get
      if pidNodeId pid == nid
        then unregisterLocalProcessEndListener pid listenerId
        else do
          unregisterRemoteNodeEndListener (pidNodeId pid) listenerId
          sendRemoteMessage (pidNodeId pid) $
            RemoteUnlistenEndMessage { rulendListenedId = listenedId,
                                       rulendListenerId = listenerId }
    GroupDest gid -> do
      unregisterGroupEndListener gid listenerId
      broadcastRemoteMessage $
        RemoteUnlistenEndMessage { rulendListenedId = listenedId,
                                   rulendListenerId = listenerId }
  runNode

-- | Handle a local hello message.
handleLocalHelloMessage :: Node -> NodeM ()
handleLocalHelloMessage node = do
  state <- St.get
  case M.lookup (nodeId node) $ nodeLocalNodes state of
    Nothing -> connectLocal node
    Just _ -> return ()
  runNode

-- | Handle a local join message.
handleLocalJoinMessage :: Node -> NodeM ()
handleLocalJoinMessage node = do
  state <- St.get
  case M.lookup (nodeId node) $ nodeLocalNodes state of
    Nothing -> connectLocalWithoutBroadcast node
    Just _ -> return ()
  runNode

-- | Handle a remote user message.
handleRemoteUserMessage :: NodeId -> SourceId -> DestId -> Header -> Payload ->
                           NodeM ()
handleRemoteUserMessage nid sourceId destId header payload = runNode

-- | Handle a remote process end message.
handleRemoteEndMessage :: NodeId -> SourceId -> Header -> Payload ->
                          NodeM ()
handleRemoteEndMessage nid sourceId header payload = runNode

-- | Handle a remote kill message.
handleRemoteKillMessage :: NodeId -> SourceId -> DestId -> Header -> Payload ->
                           NodeM ()
handleRemoteKillMessage nid processId destId header payload =
  case destId of
    ProcessDest destPid -> killLocalProcess processId destPid header payload
    GroupDest destGid -> killGroupForRemote processId destGid header payload
  runNode

-- | Handle a remote subscribe message.
handleRemoteSubscribeMessage :: NodeId -> ProcessId -> GroupId -> NodeM ()
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
handleRemoteUnsubscribeMessage :: NodeId -> ProcessId -> GroupId -> NodeM ()
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
handleRemoteAssignMessage :: NodeId -> Name -> DestId -> NodeM ()
handleRemoteAssignMessage _ name destId = do
  names <- nodeNames . nodeInfo <$> St.get
  liftIO . atomically $ do
    nameMap <- readTVar names
    writeTVar names $ M.insert name destId nameMap
  runNode

-- | Handle a remote unassign message.
handleRemoteUnassignMessage :: NodeId -> Name -> DestId -> NodeM ()
handleRemoteUnassignMessage _ name destId = do
  names <- nodeNames . nodeInfo <$> St.get
  liftIO . atomically $ do
    nameMap <- readTVar names
    writeTVar names $ M.delete name nameMap
  runNode

-- | Handle a remote shutdown message.
handleRemoteShutdownMessage :: NodeId -> ProcessId -> Header -> Payload ->
                               NodeM ()
handleRemoteShutdownMessage nid pid header payload =
  shutdownLocalNode pid header payload

-- | Handle a remote hello message.
handleRemoteHelloMessage :: NodeId -> NodeId -> Key -> NodeM ()
handleRemoteHelloMessage _ _ _ = runNode

-- | Handle a remote listen end message.
handleRemoteListenEndMessage :: NodeId -> DestId -> DestId -> NodeM ()
handleRemoteListenEndMessage nid listenedId listenerId = do
  case listenedId of
    ProcessDest pid -> do
      selfNid <- nodeId . nodeInfo <$> St.get
      if pidNodeId pid == selfNid
        then registerLocalProcessEndListener pid listenerId
        else return ()
    GroupDest gid -> registerGroupEndListener gid listenerId
  runNode

-- | Handle a remote unlisten end message.
handleRemoteUnlistenEndMessage :: NodeId -> DestId -> DestId -> NodeM ()
handleRemoteUnlistenEndMessage nid listenedId listenerId = do
  case listenedId of
    ProcessDest pid -> do
      selfNid <- nodeId . nodeInfo <$> St.get
      if pidNodeId pid == selfNid
        then unregisterLocalProcessEndListener pid listenerId
        else return ()
    GroupDest gid -> unregisterGroupEndListener gid listenerId
  runNode

-- | Handle a remote join message.
handleRemoteJoinMessage :: NodeId -> NodeId -> NodeM ()
handleRemoteJoinMessage nid remoteNid = runNode

-- | Send a user message to a process.
sendUserMessage :: DestId -> SourceId -> Header -> Payload -> NodeM ()
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
sendLocalMessage :: Node -> Message -> NodeM ()
sendLocalMessage node message = do
  liftIO . atomically $ writeTQueue (nodeQueue node) message

-- | Send a remote message.
sendRemoteMessage :: NodeId -> RemoteMessage -> NodeM ()
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
broadcastRemoteMessage :: RemoteMessage -> NodeM ()
broadcastRemoteMessage message = do
  state <- St.get
  forM_ (nodeRemoteNodes state) $ \rnode ->
    liftIO . atomically $ writeTQueue (rnodeOutput rnode) message
  forM_ (nodePendingRemoteNodes state) $ \pnode ->
    liftIO . atomically $ writeTQueue (prnodeOutput pnode) message

-- | Broadcast a remote message to all remote nodes except a particular node.
broadcastExceptRemoteMessage :: NodeId -> RemoteMessage -> NodeM ()
broadcaseExceptRemoteMessage nid message = do
  state <- St.get
  forM_ (nodeRemoteNodes state) $ \rnode ->
    if rnodeId rnode /= nid
    then liftIO . atomically $ writeTQueue (rnodeOutput rnode) message
    else return ()
  forM_ (nodePendingRemoteNodes state) $ \pnode ->
    if not $ matchPartialNodeId (prnodeId pnode) nid
    then liftIO . atomically $ writeTQueue (prnodeOutput pnode) message
    else return ()

-- | Remove listeners for a process Id
removeListeners :: ProcessId -> NodeM ()
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
removeProcess :: ProcessId -> NodeM ()
removeProcess processId = do
  St.modify $ \state ->
    state { nodeProcesses = M.delete processId nodeProcesses }

-- | Send message to all end listeners of a process
sendEndMessageForProcess :: ProcessState -> Maybe (Header, Payload) -> NodeM ()
sendEndMessageForProcess process message = do
  let (header, payload) =
        case pstateEndMessage process of
          Just message -> message
          Nothing ->
            case message of
              Just message -> message
              Nothing -> (encode $ "genericEnd" :: T.Text, BS.empty)
      pid = procId $ pstateInfo process
      sourceId =
        case pstateEndCause process of
          Just causeId ->
            CauseSource { causeSourceId = pid,
                          causeCauseId = causeId }
          Nothing -> NormalSource pid
  forM_ (pstateEndListeners process) $ \(did, _) ->
    sendUserMessage did sourceId header payload
  groups <- M.elems . nodeGroups <$> St.get
  forM_ groups $ \group -> do
    case findIndexL (\(pid', _) -> pid == pid') $ groupLocalSubscribers group of
      Just _ -> do
        for (groupEndListeners group) $ \(did, _) ->
          sendUserMessage did sourceId header payload
      Nothing -> return ()

-- | Kill a process.
killProcess :: ProcessId -> ProcessId -> Header -> Payload -> NodeM ()
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
killLocalProcess :: ProcessId -> ProcessId -> Header -> Payload -> NodeM ()
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
killGroup :: SourceId -> GroupId -> Header -> Payload -> NodeM ()
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
killGroupForRemote :: SourceId -> GroupId -> Header -> Payload -> NodeM ()
killGroupForRemote sourceId gid header payload = do
  state <- St.get
  case M.lookup destGid $ nodeGroups state of
    Just group -> do
      forM_ (groupLocalSubscribers group) $ \(pid, _) ->
        killLocalProcess $ NormalSource pid
    Nothing -> return ()

-- | Update a process.
updateProcess :: (ProcessState -> ProcessState) -> ProcessId -> NodeM ()
updateProcess f pid =
  St.modify $ \state ->
                state { nodeProcesses = M.adjust f pid $ nodeProcesses state }

-- | Update a group.
updateGroup :: (GroupState -> GroupState) -> GroupId -> NodeM ()
updateGroup f gid =
  St.modify $ \state ->
                state { nodeGroups = M.adjust f gid $ nodeGroups state }

-- | Update a remote node.
updateRemoteNode :: (RemoteNodeState -> RemoteNodeState) -> NodeId -> NodeM ()
updateRemoteNode f nid =
  St.modify $ \state ->
                state { nodeRemoteNodes = M.adjust f nid $ nodeRemoteNodes nid }

-- | Add a group.
addGroup :: GroupId -> GroupState -> NodeM ()
addGroup gid group = do
  St.modify $ \state ->
                state { nodeGroups = M.insert gid group $ nodeGroups state }

-- | Register local process end listener.
registerLocalProcessEndListener :: ProcessId -> DestId -> NodeM ()
registerLocalProcessEndListener pid listenerId = do
  updateProcess
    (\process ->
        case S.findIndexL (\(did, _) -> listenerId == did) $
             pstateEndListeners process of
          Just index ->
            process { pstateEndListeners =
                        S.adjust (\(did, count) -> (did, count + 1))
                        index $ pstateEndListeners process }
          Nothing ->
            process { pstateEndListeners = pstateEndListeners process |>
                                           S.singleton (listenerId, 1) })
    pid

-- | Register remote node end listener.
registerRemoteNodeEndListener :: NodeId -> DestId -> NodeM ()
registerRemoteNodeEndListener nid listenerId = do
  updateRemoteNode
    (\rnode ->
        case S.findIndexL (\(did, _) -> listenerId == did) $
             rnodeEndListeners rnode of
          Just index ->
            rnode { rnodeEndListeners =
                      S.adjust (\(did, count) -> (did, count + 1))
                      index $ rnodeEndListeners rnode }
          Nothing ->
            rnode { rnodeEndListeners = rnodeEndListeners rnode |>
                                        S.singleton (listenerId, 1) })
    (pidNodeId pid)

-- | Register group Id end listener.
registerGroupEndListener :: GroupId -> DestId -> NodeM ()
registerGroupEndListener gid listenerId = do
  updateGroup
    (\group ->
        case S.findIndexL (\(did, _) -> listenerId == did) $
             groupEndListeners group of
          Just index ->
            group { groupEndListeners =
                      S.adjust (\(did, count) -> (did, count + 1))
                      index $ groupEndListeners group }
          Nothing ->
            group { groupEndListeners = groupEndListeners group |>
                                        S.singleton (listenerId, 1) })
    gid

-- | Unregister local process end listener.
unregisterLocalProcessEndListener :: ProcessId -> DestId -> NodeM ()
unregisterLocalProcessEndListener pid listenerId = do
  updateProcess
    (\process ->
       case S.findIndexL (\(did, _) -> listenerId == did) $
            pstateEndListeners process of
         Just index ->
           case S.lookup index $ pstateEndListeners process of
             Just (_, count)
               | count > 1 ->
                 process { pstateEndListeners =
                             S.adjust (\(did, count) -> (did, count - 1))
                             index $ pstateEndListeners process }
               | True ->
                 process { pstateEndListeners =
                             S.deleteAt index $ pstateEndListeners process }
             Nothing -> error "impossible"
         Nothing -> return ())
    pid

-- | Unregister remote node end listener.
unregisterRemoteNodeEndListener :: NodeId -> DestId -> NodeM ()
unregisterRemoteNodeEndListener nid listenerId = do
  updateRemoteNode
    (\rnode ->
       case S.findIndexL (\(did, _) -> listenerId == did) $
            rnodeEndListeners rnode of
         Just index ->
           case S.lookup index $ rnodeEndListeners rnode of
             Just (_, count)
               | count > 1 ->
                 rnode { rnodeEndListeners =
                             S.adjust (\(did, count) -> (did, count - 1))
                             index $ rnodeEndListeners rnode }
               | True ->
                 rnode { rnodeEndListeners =
                             S.deleteAt index $ rnodeEndListeners rnode }
             Nothing -> error "impossible"
         Nothing -> return ())
    nid

-- | Unregister group end listener.
unregisterGroupEndListener :: GroupId -> DestId -> NodeM ()
unregisterGroupEndListener gid listenerId = do
  updateGroup
    (\group ->
       case S.findIndexL (\(did, _) -> listenerId == did) $
            groupEndListeners group of
         Just index ->
           case S.lookup index $ groupEndListeners group of
             Just (_, count)
               | count > 1 ->
                 group { groupEndListeners =
                             S.adjust (\(did, count) -> (did, count - 1))
                             index $ groupEndListeners group }
               | True ->
                 group { groupEndListeners =
                             S.deleteAt index $ groupEndListeners group }
             Nothing -> error "impossible"
         Nothing -> return ())
    gid

-- | Shutdown the local node.
shutdownLocalNode :: ProcessId -> Header -> Payload -> NodeM ()
shutdownLocalNode pid header payload = do
  state <- St.get
  broadcastRemoteMessage RemoteLeaveMessage
  liftIO . atomically $ do
    forM_ (M.elems $ nodeRemoteNodes state) $ \node ->
      putTMVar (rnodeTerminate node) ()
  St.modify $ \state -> state { nodeRemoteNodes = S.empty }
  
-- | Start local communication with another node.
startLocalCommunication :: Node -> NodeM ()
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
connectLocal :: Node -> NodeM ()
connectLocal node = do
  startLocalCommunication node
  nodes <- M.elems . nodeLocalNodes <$> St.get
  St.modify $ \state -> do
    state { nodeLocalNodes =
              M.insert (nodeId node) node $ nodeLocalNodes state }
  forM_ nodes $ \node' -> do
    sendLocalMessage node' $ JoinMessage { joinNode = node }
  selfNode <- nodeInfo <$> St.get
  sendLocalMessage (nodeId node) $ HelloMessage { heloNode = selfNode }

-- | Connect to a local node without broadcasting join messages.
connectLocalWithoutBroadcast :: Node -> NodeM ()
connectLocalWithoutBroadcast node = do
  startLocalCommunication node
  St.modify $ \state -> do
    state { nodeLocalNodes =
              M.insert (nodeId node) node $ nodeLocalNodes state }
  selfNode <- nodeInfo <$> St.get
  sendLocalMessage (nodeId node) $ HelloMessage { heloNode = selfNode }

-- | Get whether a partial node Id matches a node Id.
matchPartialNodeId :: PartialNodeId -> NodeId -> Bool
matchPartialNodeId pnid nid =
  (pnidFixedNum pnid == nidFixedNum nid) &&
  (pnidAddress pnid == nidAddress nid) &&
  (case pnidRandomNum pnid of
     Nothing -> True
     Just randomNum -> randomNum == nidRandomNum nid)

-- | Find a pending remote node.
findPendingRemoteNode :: NodeId -> NodeM PendingRemoteNodeState
findPendingRemoteNode nid = do
  pending <- nodeRemotePendingNodes St.get
  case S.findIndexL
       (\pnode -> matchPartialNodeId (prnodeId pnode) nid) pending of
    Just index -> return $ S.lookup index pending
    Nothing -> return Nothing

-- | Run a socket.
runSocket :: NodeId -> TQueue RemoteMessage -> TMVar () -> NS.Socket -> NodeM ()
runSocket nid output terminate socket = do
  terminateOutput <- liftIO $ atomically newEmptyTMVar
  finalizeInput <- liftIO $ atomically newEmptyTMVar
  finalizeOutput <- liftIO $ atomically newEmptyTMVar
  async $ do
    atomically $ do
      takeTMVar terminate
      putTMVar terminateOutput ()
    NS.shutdown socket NS.ShutdownReceive
    atomically $ do
      takeTMVar finalizeInput
      takeTMVar finalizeOutput
    NS.close socket
  input <- nodeRemoteQueue . nodeInfo <$> St.get
  async $ runSocketInput nid input terminateOutput finalizeInput socket
  async $ runSocketOutput nid output terminateOutput finalizeOutput socket

-- | Run socket input.
runSocketInput :: NodeId -> TQueue RemoteEvent -> TMVar () -> TMVar () ->
                  NS.Socket -> IO ()
runSocketInput nid input terminate finalize socket = do
  catch (runsocketInput' nid input socket BS.empty)
    (\e -> const (return ()) (e :: IOException))
  atomically $ do
    writeTQueue input $ RemoteDisconnected { dconNodeId = nid }
    tryPutTMVar terminate () >> return ()
    putTMVar finalize ()

-- | Actually run socket input
runSocketInput' :: NodeId -> TQueue RemoteEvent -> NS.Socket ->
                   BS.ByteString -> IO ()
runSocketInput' nid input socket buffer = do
  if BS.length buffer < 8
    then do block <- BS.fromStrict <$> NSB.recv socket 4096
            if BS.null block
              then runSocketInput' nid input socket $ BS.append buffer block
              else return ()
    else let (lengthField, rest) = BS.splitAt 8 buffer
         in runSocketInput'' nid input socket rest
            (B.decode lengthField :: Int64)

-- | After having gotten the length field, continue parsing a message
runSocketInput'' :: NodeId -> TQueue RemoteEvent -> NS.Socket ->
                    BS.ByteString -> Int64 -> fromIntegral
runSocketInput'' nid input socket buffer messageLength = do
  if BS.length buffer < messageLength
    then do block <- BS.fromStrict <$> NSB.recv socket 4096
            if BS.null block
              then runSocketInput'' nid input socket (BS.append buffer block)
                   messageLength
              else return ()
    else do let (messageBuffer, rest) = BS.splitAt messageLength buffer
                message = B.decode messageBuffer
            atomically . writeTQueue input $
              remoteReceived { recvNodeId = nid, recvMessage = message }
            runSocketInput' nid input socket rest

-- | Run socket output.
runSocketOutput :: NodeId -> TQueue RemoteMessage -> TMVar () -> TMVar () ->
                   NS.Socket -> IO ()
runSocketOutput nid input terminate finalize socket = do
  event <- atomically $ (Right <$> readTQueue input) `orElse`
                        (Left <$> readTMVar terminate)
  case event of
    Right message -> do
      let messageData = B.encode message
          messageLengthField = B.encode $ BS.length messageData
          fullMessageData = BS.append messageLengthField messageData
      continue <- catch (do NSB.sendAll socket $ BS.toStrict fullMessageData
                            return True)
                  (\e -> const (return False) (e :: IOException))
      if continue
        then runSocketOutput nic input terminate finalize socket
        else do NS.shutdown socket NS.ShutdownBoth
                atomically $ putTMVar finalize ()
    Left () -> do NS.shutdown socket NS.ShutdownBoth
                  atomically $ putTMVar finalize ()
