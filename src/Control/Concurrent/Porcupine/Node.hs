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

module Control.Concurrent.Porcupine.Node

  (Node,
   start,
   waitShutdown,
   waitShutdownSTM,
   getNodeId)

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
import Data.Int (Int64)
import Data.Word (Word32)
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
                                      writeTQueue)
import Control.Concurrent.STM.TMVar (TMVar,
                                     newEmptyTMVar,
                                     putTMVar,
                                     tryPutTMVar,
                                     tryReadTMVar,
                                     takeTMVar,
                                     readTMVar)
import qualified Control.Monad.Trans.State.Strict as St
import Control.Monad.IO.Class (MonadIO (..))
import Control.Exception.Base (SomeException,
                               Exception (..),
                               AsyncException (..),
                               IOException (..),
                               catch)
import Control.Monad (forM,
                      forM_,
                      replicateM,
                      (=<<))
import Data.Functor ((<$>),
                     fmap)
import Text.Printf (printf)

-- | Start a node with a given fixed index, optional address, and optional key.
start :: Integer -> Maybe NS.SockAddr -> Key -> IO Node
start fixedNum address key = do
  gen <- R.newStdGen
  let (randomNum, gen') = R.random gen
      nid = NodeId { nidFixedNum = fixedNum,
                     nidAddress = toSockAddr' <$> address,
                     nidRandomNum = randomNum }
  queue <- atomically newTQueue
  remoteQueue <- atomically newTQueue
  names <- atomically $ newTVar M.empty
  gen'' <- atomically $ newTVar gen'
  nextSequenceNum <- atomically $ newTVar 0
  terminate <- atomically newEmptyTMVar
  shutdown <- atomically newEmptyTMVar
  let info = Node { nodeId = nid,
                    nodeQueue = queue,
                    nodeRemoteQueue = remoteQueue,
                    nodeGen = gen'',
                    nodeNextSequenceNum = nextSequenceNum,
                    nodeShutdown = shutdown,
                    nodeNames = names }
      nodeState = NodeState { nodeInfo = info,
                              nodeKey = key,
                              nodeReadOrder = False,
                              nodeTerminate = terminate,
                              nodeProcesses = M.empty,
                              nodeLocalNodes = M.empty,
                              nodeRemoteNodes = M.empty,
                              nodePendingRemoteNodes = S.empty,
                              nodeGroups = M.empty }
  async $ St.runStateT runNode nodeState >> return ()
  case address of
    Just address -> startListen nid address remoteQueue terminate key
    Nothing -> return ()
  return info

-- | Wait for a node to shutdown.
waitShutdown :: Node -> IO ()
waitShutdown = atomically . readTMVar . nodeShutdown

-- | Wait for a node to shutdown in STM.
waitShutdownSTM :: Node -> STM ()
waitShutdownSTM = readTMVar . nodeShutdown

-- | Get the node Id of a node.
getNodeId :: Node -> NodeId
getNodeId = nodeId

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
      (Right <$> readTQueue input)
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
  handleLocalConnectRemoteMessage conrNodeId
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
  handleRemoteConnected rconNodeId rconSocket rconBuffer
handleRemoteEvent RemoteConnectFailed{..} =
  handleRemoteConnectFailed rcflNodeId
handleRemoteEvent RemoteReceived{..} =
  handleRemoteMessage recvNodeId recvMessage
handleRemoteEvent RemoteDisconnected{..} =
  handleRemoteDisconnected dconNodeId

-- | Handle a remote message.
handleRemoteMessage :: NodeId -> RemoteMessage -> NodeM ()
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
handleRemoteConnected :: NodeId -> NS.Socket -> BS.ByteString -> NodeM ()
handleRemoteConnected nid socket buffer = do
  pnode <- findPendingRemoteNode nid
  terminate <- nodeTerminate <$> St.get
  output <- case pnode of
    Just pnode -> do
      St.modify $ \state ->
        let rnode = RemoteNodeState { rnodeId = nid,
                                      rnodeOutput = prnodeOutput pnode,
                                      rnodeEndListeners =
                                        prnodeEndListeners pnode }
        in state { nodePendingRemoteNodes =
                     S.filter (\pnode' -> not $ matchPartialNodeId
                                          (prnodeId pnode) nid) $
                     nodePendingRemoteNodes state,
                   nodeRemoteNodes =
                     M.insert nid rnode $ nodeRemoteNodes state }
      broadcastExceptRemoteMessage nid $
        RemoteJoinMessage { rjoinNodeId = nid }
      return $ prnodeOutput pnode
    Nothing -> do
      output <- liftIO $ atomically newTQueue
      St.modify $ \state ->
        let rnode = RemoteNodeState { rnodeId = nid,
                                      rnodeOutput = output,
                                      rnodeEndListeners = S.empty }
        in state { nodeRemoteNodes =
                     M.insert nid rnode $ nodeRemoteNodes state }
      broadcastExceptRemoteMessage nid $
        RemoteJoinMessage { rjoinNodeId = nid }
      return output
  runSocket nid output terminate socket buffer
  updateRemoteNames nid
  runNode

-- | Handle a remote connect failed event.
handleRemoteConnectFailed :: PartialNodeId -> NodeM ()
handleRemoteConnectFailed pnid = do
  pnodes <- nodePendingRemoteNodes <$> St.get
  let header = B.encode ("remoteConnectFailed" :: T.Text)
      payload = B.encode $ UserRemoteConnectFailed { urcfNodeId = pnid }
  forM_ pnodes $ \pnode ->
    if prnodeId pnode == pnid
    then do
      forM_ (prnodeEndListeners pnode) $ \(did, _) ->
        sendLocalUserMessage did NoSource header payload
    else return ()
  St.modify $ \state ->
    state { nodePendingRemoteNodes =
              S.filter (\pnode -> prnodeId pnode /= pnid) $
              nodePendingRemoteNodes state }
  runNode

-- | Handle a remote disconnected event.
handleRemoteDisconnected :: NodeId -> NodeM ()
handleRemoteDisconnected nid = do
  rnodes <- nodeRemoteNodes <$> St.get
  let header = B.encode ("remoteDisconnected" :: T.Text)
      payload = B.encode $ UserRemoteDisconnected  { urdcNodeId = nid }
  case M.lookup nid rnodes of
    Just rnode -> do
      forM_ (rnodeEndListeners rnode) $ \(did, _) ->
        sendLocalUserMessage did NoSource header payload
      St.modify $ \state ->
        state { nodeRemoteNodes = M.delete nid $ nodeRemoteNodes state }
    Nothing -> return ()
  runNode

-- | Handle a local user message.
handleLocalUserMessage :: SourceId -> DestId -> Header -> Payload -> NodeM ()
handleLocalUserMessage sourceId destId header payload = do
  sendLocalUserMessage destId sourceId header payload
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
      catch (do St.evalStateT (do let Process action =
                                        entry sourceId header payload
                                    in do action
                                          return ())
                  processInfo
                return Nothing) $
            (\e -> return $ Just (e :: SomeException))
    atomically . writeTQueue (nodeQueue $ nodeInfo state) $
      EndMessage { endProcessId = processId, endException = maybeException }
  let processState = ProcessState { pstateInfo = processInfo,
                                    pstateAsync = asyncThread,
                                    pstateTerminating = False,
                                    pstateEndMessage = Nothing,
                                    pstateEndCause = Nothing,
                                    pstateEndListeners = S.empty }
  St.modify $ \state ->
    state { nodeProcesses = M.insert processId processState $
                            nodeProcesses state }
  runNode

-- | Handle a local quit message.
handleLocalQuitMessage :: ProcessId -> Header -> Payload -> NodeM ()
handleLocalQuitMessage pid header payload = do
  updateProcess (\process ->
                    if not $ pstateTerminating process
                    then
                      process {
                        pstateEndMessage =
                          case pstateEndMessage process of
                            Just message -> Just message
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
              let asyncException =
                    (fromException exception) :: Maybe AsyncException
              in case asyncException of
                Just asyncException
                  | asyncException /= ThreadKilled &&
                    asyncException /= UserInterrupt ->
                      Just (B.encode ("exceptionExit" :: T.Text),
                             B.encode . T.pack $ show exception)
                  | True -> Nothing
                Nothing -> 
                  Just (B.encode ("exceptionExit" :: T.Text),
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
                                 index $ groupLocalSubscribers group })
            gid
        Nothing -> do
          updateGroup
            (\group -> group { groupLocalSubscribers =
                                 groupLocalSubscribers group |> (pid, 1) })
            gid
    Nothing -> do addGroup gid $
                    GroupState { groupId = gid,
                                 groupLocalSubscribers = S.singleton (pid, 1),
                                 groupRemoteSubscribers = S.empty,
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
  connectLocal node
  runNode

-- | Handle a local connect remote message.
handleLocalConnectRemoteMessage :: PartialNodeId -> NodeM ()
handleLocalConnectRemoteMessage pnid = do
  connectRemote pnid
  runNode

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
  updateRemoteNames $ nodeId node
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
handleRemoteUserMessage nid sourceId destId header payload = do
  handleIncomingUserMessage destId sourceId header payload
  runNode

-- | Handle a remote process end message.
handleRemoteEndMessage :: NodeId -> SourceId -> Header -> Payload ->
                          NodeM ()
handleRemoteEndMessage nid sourceId header payload = runNode

-- | Handle a remote kill message.
handleRemoteKillMessage :: NodeId -> ProcessId -> DestId -> Header -> Payload ->
                           NodeM ()
handleRemoteKillMessage nid processId destId header payload = do
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
                                 groupRemoteSubscribers = S.singleton (nid, 1),
                                 groupEndListeners = S.empty }
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
handleRemoteAssignMessage _ name did = do
  names <- nodeNames . nodeInfo <$> St.get
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
  runNode

-- | Handle a remote unassign message.
handleRemoteUnassignMessage :: NodeId -> Name -> DestId -> NodeM ()
handleRemoteUnassignMessage _ name did = do
  names <- nodeNames . nodeInfo <$> St.get
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
handleRemoteJoinMessage nid remoteNid = do
  connectRemote (partialNodeIdOfNodeId remoteNid)
  runNode

-- | Send a user message to a process.
sendLocalUserMessage :: DestId -> SourceId -> Header -> Payload -> NodeM ()
sendLocalUserMessage destId sourceId header payload = do
  state <- St.get
  case destId of
    ProcessDest pid ->
      if pidNodeId pid == nodeId (nodeInfo state)
      then case M.lookup pid $ nodeProcesses state of
             Just process -> do
               liftIO . atomically $ do
                 writeTQueue (procQueue $ pstateInfo process) $
                   UserMessage { umsgSourceId = sourceId,
                                 umsgDestId = destId,
                                 umsgHeader = header,
                                 umsgPayload = payload }
             Nothing -> return ()
      else do sendRemoteMessage (pidNodeId pid) $
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
                  writeTQueue (procQueue $ pstateInfo process) $
                    UserMessage { umsgSourceId = sourceId,
                                  umsgDestId = destId,
                                  umsgHeader = header,
                                  umsgPayload = payload }
              Nothing -> return ()
          forM_ (groupRemoteSubscribers group) $ \(nid, _) -> do
            sendRemoteMessage nid $
              RemoteUserMessage { rumsgSourceId = sourceId,
                                  rumsgDestId = destId,
                                  rumsgHeader = header,
                                  rumsgPayload = payload }

-- | Send incoming user message to destination processes.
handleIncomingUserMessage :: DestId -> SourceId -> Header -> Payload -> NodeM ()
handleIncomingUserMessage destId sourceId header payload = do
  state <- St.get
  case destId of
    ProcessDest pid ->
      if pidNodeId pid == nodeId (nodeInfo state)
      then case M.lookup pid $ nodeProcesses state of
             Just process -> do
               liftIO . atomically $ do
                 writeTQueue (procQueue $ pstateInfo process) $
                   UserMessage { umsgSourceId = sourceId,
                                 umsgDestId = destId,
                                 umsgHeader = header,
                                 umsgPayload = payload }
             Nothing -> return ()
      else return ()
    GroupDest gid ->
      case M.lookup gid $ nodeGroups state of
        Just group -> do
          forM_ (groupLocalSubscribers group) $ \(pid, _) ->
            case M.lookup pid $ nodeProcesses state of
              Just process -> do
                liftIO . atomically $ do
                  writeTQueue (procQueue $ pstateInfo process) $
                    UserMessage { umsgSourceId = sourceId,
                                  umsgDestId = destId,
                                  umsgHeader = header,
                                  umsgPayload = payload }
              Nothing -> return ()

-- | Send a locall message.
sendLocalMessage :: Node -> Message -> NodeM ()
sendLocalMessage node message = do
  liftIO . atomically $ writeTQueue (nodeQueue node) message

-- | Send a remote message.
sendRemoteMessage :: NodeId -> RemoteMessage -> NodeM ()
sendRemoteMessage nid message = do
  state <- St.get
  case M.lookup nid $ nodeRemoteNodes state of
    Just rnode -> do
      liftIO . atomically $ writeTQueue (rnodeOutput rnode) message
    Nothing ->
      let index = (flip S.findIndexL) (nodePendingRemoteNodes state) $ \pnode ->
            let pnid = prnodeId pnode
            in pnidFixedNum pnid == nidFixedNum nid &&
               pnidAddress pnid == nidAddress nid
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
broadcastExceptRemoteMessage nid message = do
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
    state { nodeProcesses = M.delete processId $ nodeProcesses state }

-- | Send message to all end listeners of a process
sendEndMessageForProcess :: ProcessState -> Maybe (Header, Payload) -> NodeM ()
sendEndMessageForProcess process message = do
  let (header, payload) =
        case pstateEndMessage process of
          Just message -> message
          Nothing ->
            case message of
              Just message -> message
              Nothing -> (B.encode ("genericEnd" :: T.Text), BS.empty)
      pid = procId $ pstateInfo process
      sourceId =
        case pstateEndCause process of
          Just causeId ->
            CauseSource { causeSourceId = pid,
                          causeCauseId = causeId }
          Nothing -> NormalSource pid
  forM_ (pstateEndListeners process) $ \(did, _) ->
    sendLocalUserMessage did sourceId header payload
  groups <- M.elems . nodeGroups <$> St.get
  forM_ groups $ \group -> do
    case S.findIndexL (\(pid', _) -> pid == pid') $
         groupLocalSubscribers group of
      Just _ -> do
        forM_ (groupEndListeners group) $ \(did, _) ->
          sendLocalUserMessage did sourceId header payload
      Nothing -> return ()

-- | Kill a process.
killProcess :: ProcessId -> ProcessId -> Header -> Payload -> NodeM ()
killProcess pid targetPid header payload = do
  node <- nodeInfo <$> St.get
  if pidNodeId pid == nodeId node
    then killLocalProcess pid targetPid header payload
    else sendRemoteMessage (pidNodeId targetPid) $
         RemoteKillMessage { rkillProcessId = pid,
                             rkillDestId = ProcessDest targetPid,
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
                              case pstateEndMessage process of
                                Just message -> Just message
                                Nothing -> Just (header, payload),
                            pstateEndCause = Just sourcePid,
                            pstateTerminating = True })
          destPid
      else return ()
    Nothing -> return ()

-- | Kill a group.
killGroup :: ProcessId -> GroupId -> Header -> Payload -> NodeM ()
killGroup pid destGid header payload = do
  state <- St.get
  case M.lookup destGid $ nodeGroups state of
    Just group -> do
      forM_ (groupLocalSubscribers group) $ \(pid', _) ->
        killLocalProcess pid pid' header payload
      forM_ (groupRemoteSubscribers group) $ \(nid, _) -> do
        sendRemoteMessage nid $
          RemoteKillMessage { rkillProcessId = pid,
                              rkillDestId = GroupDest destGid,
                              rkillHeader = header,
                              rkillPayload = payload }
    Nothing -> return ()

-- | Kill a group in response to a remote message.
killGroupForRemote :: ProcessId -> GroupId -> Header -> Payload -> NodeM ()
killGroupForRemote pid gid header payload = do
  state <- St.get
  case M.lookup gid $ nodeGroups state of
    Just group -> do
      forM_ (groupLocalSubscribers group) $ \(pid', _) ->
        killLocalProcess pid pid' header payload
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
                state { nodeRemoteNodes =
                          M.adjust f nid $ nodeRemoteNodes state }

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
                                           (listenerId, 1) })
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
                                        (listenerId, 1) })
    nid

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
                                        (listenerId, 1) })
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
         Nothing -> process)
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
         Nothing -> rnode)
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
         Nothing -> group)
    gid

-- | Shutdown the local node.
shutdownLocalNode :: ProcessId -> Header -> Payload -> NodeM ()
shutdownLocalNode pid header payload = do
  processes <- nodeProcesses <$> St.get
  forM_ processes $ \process ->
    killLocalProcess pid (procId $ pstateInfo process) header payload
  terminate <- nodeTerminate <$> St.get
  broadcastRemoteMessage RemoteLeaveMessage
  liftIO . atomically $ putTMVar terminate ()
  St.modify $ \state -> state { nodeRemoteNodes = M.empty }
  liftIO . atomically . (flip putTMVar) () . nodeShutdown . nodeInfo =<< St.get
  
-- | Start local communication with another node.
startLocalCommunication :: Node -> NodeM ()
startLocalCommunication node = do
  output <- liftIO $ atomically newTQueue
  terminate <- nodeTerminate <$> St.get
  let remoteNodeState =
        RemoteNodeState { rnodeId = nodeId node,
                          rnodeOutput = output,
                          rnodeEndListeners = S.empty }
  St.modify $ \state ->
    state { nodeRemoteNodes = M.insert (nodeId node) remoteNodeState $
                              nodeRemoteNodes state }
  selfNid <- nodeId . nodeInfo <$> St.get
  let remoteQueue = nodeRemoteQueue node
  (liftIO . async $ runLocalCommunication selfNid output remoteQueue
    terminate) >> return ()

-- | Run local communication with another node
runLocalCommunication :: NodeId -> TQueue RemoteMessage -> TQueue RemoteEvent ->
                         TMVar () -> IO ()
runLocalCommunication selfNid output remoteQueue terminate = do
  event <- atomically $ do
    (Right <$> readTQueue output) `orElse` (Left <$> takeTMVar terminate)
  case event of
    Right remoteMessage -> do
      atomically . writeTQueue remoteQueue $
        RemoteReceived { recvNodeId = selfNid,
                         recvMessage = remoteMessage }
      runLocalCommunication selfNid output remoteQueue terminate
    Left () -> do
      atomically . writeTQueue remoteQueue $
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
  sendLocalMessage node $ HelloMessage { heloNode = selfNode }

-- | Connect to a local node without broadcasting join messages.
connectLocalWithoutBroadcast :: Node -> NodeM ()
connectLocalWithoutBroadcast node = do
  startLocalCommunication node
  St.modify $ \state -> do
    state { nodeLocalNodes =
              M.insert (nodeId node) node $ nodeLocalNodes state }
  selfNode <- nodeInfo <$> St.get
  sendLocalMessage node $ HelloMessage { heloNode = selfNode }

-- | Get whether a partial node Id matches a node Id.
matchPartialNodeId :: PartialNodeId -> NodeId -> Bool
matchPartialNodeId pnid nid =
  (pnidFixedNum pnid == nidFixedNum nid) &&
  (pnidAddress pnid == nidAddress nid) &&
  (case pnidRandomNum pnid of
     Nothing -> True
     Just randomNum -> randomNum == nidRandomNum nid)

-- | Get whether a partial node Id matches another partial node Id.
matchPartialNodeId' :: PartialNodeId -> PartialNodeId -> Bool
matchPartialNodeId' pnid0 pnid1 =
  (pnidFixedNum pnid0 == pnidFixedNum pnid1) &&
  (pnidAddress pnid0 == pnidAddress pnid1) &&
  (case (pnidRandomNum pnid0, pnidRandomNum pnid1) of
     (Just randomNum0, Just randomNum1) -> randomNum0 == randomNum1
     _ -> True)

-- | Find a pending remote node.
findPendingRemoteNode :: NodeId -> NodeM (Maybe PendingRemoteNodeState)
findPendingRemoteNode nid = do
  pending <- nodePendingRemoteNodes <$> St.get
  case S.findIndexL
       (\pnode -> matchPartialNodeId (prnodeId pnode) nid) pending of
    Just index -> return $ S.lookup index pending
    Nothing -> return Nothing

-- | Run a socket.
runSocket :: NodeId -> TQueue RemoteMessage -> TMVar () -> NS.Socket ->
             BS.ByteString -> NodeM ()
runSocket nid output terminate socket buffer = do
  terminateOutput <- liftIO $ atomically newEmptyTMVar
  finalizeInput <- liftIO $ atomically newEmptyTMVar
  finalizeOutput <- liftIO $ atomically newEmptyTMVar
  liftIO . async $ do
    atomically $ do
      takeTMVar terminate
      putTMVar terminateOutput ()
    NS.shutdown socket NS.ShutdownReceive
    atomically $ do
      takeTMVar finalizeInput
      takeTMVar finalizeOutput
    NS.close socket
  input <- nodeRemoteQueue . nodeInfo <$> St.get
  (liftIO . async $ do
    runSocketInput nid input terminateOutput finalizeInput socket buffer) >>
    return ()
  (liftIO . async $ do
    runSocketOutput nid output terminateOutput finalizeOutput socket) >>
    return ()

-- | Run socket input.
runSocketInput :: NodeId -> TQueue RemoteEvent -> TMVar () -> TMVar () ->
                  NS.Socket -> BS.ByteString -> IO ()
runSocketInput nid input terminate finalize socket buffer = do
  catch (runSocketInput' nid input socket buffer)
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
                    BS.ByteString -> Int64 -> IO ()
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
              RemoteReceived { recvNodeId = nid, recvMessage = message }
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
        then runSocketOutput nid input terminate finalize socket
        else do NS.shutdown socket NS.ShutdownBoth
                atomically $ putTMVar finalize ()
    Left () -> do NS.shutdown socket NS.ShutdownBoth
                  atomically $ putTMVar finalize ()

-- | See if a connection already exists with a node.
isAlreadyConnected :: PartialNodeId -> NodeM Bool
isAlreadyConnected pnid = do
  rnodes <- nodeRemoteNodes <$> St.get
  let connectedToAny = any (matchPartialNodeId pnid) $ M.keys rnodes
  if not connectedToAny
    then do
      pnodes <- nodePendingRemoteNodes <$> St.get
      return $ any (matchPartialNodeId' pnid . prnodeId) pnodes
    else return True

-- | Connect to a remote node.
connectRemote :: PartialNodeId -> NodeM ()
connectRemote pnid = do
  alreadyConnected <- isAlreadyConnected pnid
  if alreadyConnected
    then case pnidAddress pnid of
           Just address -> do
             let address' = fromSockAddr' address
             socket <- liftIO $ NS.socket (familyOfSockAddr address') NS.Stream
                       NS.defaultProtocol
             result <- liftIO $ catch (Right <$> NS.connect socket address')
                       (\e -> const (return $ Left ()) (e :: IOException))
             remoteQueue <- nodeRemoteQueue . nodeInfo <$> St.get
             case result of
               Right _ -> do
                 nid <- nodeId . nodeInfo <$> St.get
                 key <- nodeKey <$> St.get
                 terminate <- nodeTerminate <$> St.get
                 output <- liftIO $ atomically newTQueue
                 let pnode = PendingRemoteNodeState { prnodeId = pnid,
                                                      prnodeOutput = output,
                                                      prnodeEndListeners =
                                                        S.empty }
                 St.modify $ \state ->
                   state { nodePendingRemoteNodes =
                             nodePendingRemoteNodes state |> pnode }
                 liftIO $ handshakeWithSocket nid remoteQueue terminate socket
                   key (Just pnid)
               Left _ -> do
                 liftIO . atomically . writeTQueue remoteQueue $
                   RemoteConnectFailed { rcflNodeId = pnid }
           Nothing -> return ()
    else return ()

-- | Handshake with socket.
handshakeWithSocket :: NodeId -> TQueue RemoteEvent -> TMVar () -> NS.Socket ->
                       Key -> Maybe PartialNodeId -> IO ()
handshakeWithSocket nid remoteQueue terminate socket key pnid = do
  _ <- async $ do
    success <- atomically newEmptyTMVar
    doneShuttingDown <- atomically newEmptyTMVar
    async $ do
      result <- atomically $ (Right <$> readTMVar success) `orElse`
                             (Left <$> readTMVar terminate)
      case result of
        Right () -> return ()
        Left () -> do NS.shutdown socket NS.ShutdownBoth
                      atomically $ putTMVar doneShuttingDown ()
    let messageData = B.encode $ RemoteHelloMessage { rheloNodeId = nid,
                                                      rheloKey = key }
        messageLengthField = B.encode $ BS.length messageData
        fullMessageData = BS.append messageLengthField messageData
    continue <- catch (do NSB.sendAll socket $ BS.toStrict fullMessageData
                          return True)
                (\e -> const (return False) (e :: IOException))
    if continue
      then handshakeWithSocket' nid remoteQueue terminate socket key BS.empty
           pnid success doneShuttingDown
      else do
        (atomically $ tryPutTMVar terminate ()) >> return ()
        atomically $ readTMVar doneShuttingDown
        NS.close socket
        notifyHandshakeFailure remoteQueue pnid
  return ()

-- | Receive the hello message length during handshaking.
handshakeWithSocket' :: NodeId -> TQueue RemoteEvent -> TMVar () -> NS.Socket ->
                        Key -> BS.ByteString -> Maybe PartialNodeId ->
                        TMVar () -> TMVar () -> IO ()
handshakeWithSocket' nid remoteQueue terminate socket key buffer pnid
  success doneShuttingDown = do
  continue <- (== Nothing) <$> (atomically $ tryReadTMVar terminate)
  if continue
    then
      if BS.length buffer < 12
      then do
        block <- catch (Just . BS.fromStrict <$> NSB.recv socket 4096)
                 (\e -> const (return Nothing) (e :: IOException))
        case block of
          Just block
            | not $ BS.null block ->
              handshakeWithSocket' nid remoteQueue terminate socket key
                (BS.append buffer block) pnid success doneShuttingDown
          _ -> do
            (atomically $ tryPutTMVar terminate ()) >> return ()
            atomically $ readTMVar doneShuttingDown
            NS.close socket
            notifyHandshakeFailure remoteQueue pnid
      else let (magicField, rest) = BS.splitAt 4 buffer
               (lengthField, rest') = BS.splitAt 8 rest
           in if (B.decode magicField :: Word32) == 0x904C0914
              then handshakeWithSocket'' nid remoteQueue terminate socket key
                   rest' (B.decode lengthField :: Int64) pnid success
                   doneShuttingDown
              else do
                (atomically $ tryPutTMVar terminate ()) >> return ()
                atomically $ readTMVar doneShuttingDown
                NS.close socket
                notifyHandshakeFailure remoteQueue pnid
    else do
      atomically $ readTMVar doneShuttingDown
      NS.close socket
      notifyHandshakeFailure remoteQueue pnid

-- | Complete receiving the hello message during handshaking.
handshakeWithSocket'' :: NodeId -> TQueue RemoteEvent -> TMVar () ->
                         NS.Socket -> Key -> BS.ByteString -> Int64 ->
                         Maybe PartialNodeId -> TMVar () -> TMVar() -> IO ()
handshakeWithSocket'' nid remoteQueue terminate socket key buffer
  messageLength pnid success doneShuttingDown = do
  continue <- (== Nothing) <$> (atomically $ tryReadTMVar terminate)
  if continue
    then
      if BS.length buffer < messageLength
      then do
        block <- catch (Just . BS.fromStrict <$> NSB.recv socket 4096)
                 (\e -> const (return Nothing) (e :: IOException))
        case block of
          Just block
            | not $ BS.null block ->
              handshakeWithSocket'' nid remoteQueue terminate socket key
                (BS.append buffer block) messageLength pnid success
                doneShuttingDown
          _ -> do
            (atomically $ tryPutTMVar terminate ()) >> return ()
            atomically $ readTMVar doneShuttingDown
            NS.close socket
            notifyHandshakeFailure remoteQueue pnid
      else let (messageData, rest) = BS.splitAt messageLength buffer
               message = B.decode messageData :: RemoteMessage
           in case message of
                RemoteHelloMessage{..}
                  | rheloKey == key -> do
                      atomically $ do
                        writeTQueue remoteQueue $
                          RemoteConnected { rconNodeId = rheloNodeId,
                                            rconSocket = socket,
                                            rconBuffer = rest }
                        putTMVar success ()
                _ -> do
                  (atomically $ tryPutTMVar terminate ()) >> return ()
                  atomically $ readTMVar doneShuttingDown
                  NS.close socket
                  notifyHandshakeFailure remoteQueue pnid
    else do
      atomically $ readTMVar doneShuttingDown
      NS.close socket
      notifyHandshakeFailure remoteQueue pnid

-- | Notify failure if partial node Id is present.
notifyHandshakeFailure :: TQueue RemoteEvent -> Maybe PartialNodeId -> IO ()
notifyHandshakeFailure remoteQueue (Just pnid) = do
  atomically . writeTQueue remoteQueue $
    RemoteConnectFailed { rcflNodeId = pnid }
notifyHandshakeFailure _ Nothing = return ()

-- | Get family of SockAddr
familyOfSockAddr :: NS.SockAddr -> NS.Family
familyOfSockAddr (NS.SockAddrInet _ _) = NS.AF_INET
familyOfSockAddr (NS.SockAddrInet6 _ _ _ _) = NS.AF_INET6
familyOfSockAddr (NS.SockAddrUnix _) = NS.AF_UNIX
familyOfSockAddr _ = error "not supported"
        
-- | Start listening for incoming connections
startListen :: NodeId -> NS.SockAddr -> TQueue RemoteEvent -> TMVar () -> Key ->
               IO ()
startListen nid address remoteQueue terminate key = do
  socket <- NS.socket (familyOfSockAddr address) NS.Stream NS.defaultProtocol
  NS.bind socket address
  NS.listen socket 5
  doneShuttingDown <- atomically newEmptyTMVar
  async $ do
    (atomically $ readTMVar terminate) >> return ()
    NS.shutdown socket NS.ShutdownBoth
    atomically $ putTMVar doneShuttingDown ()
  (async $ runListen nid remoteQueue socket terminate doneShuttingDown key) >>
    return ()

-- | Run listening for incoming connections.
runListen :: NodeId -> TQueue RemoteEvent -> NS.Socket -> TMVar () ->
             TMVar () -> Key -> IO ()
runListen nid remoteQueue socket terminate doneShuttingDown key = do
  continue <- (== Nothing) <$> (atomically $ tryReadTMVar terminate)
  if continue
    then do
      result <- catch (Just <$> NS.accept socket)
                (\e -> const (return Nothing) (e :: IOException))
      case result of
        Just (socket', _) -> do
          handshakeWithSocket nid remoteQueue terminate socket' key Nothing
          runListen nid remoteQueue socket terminate doneShuttingDown key
        Nothing -> do
          (atomically $ tryPutTMVar terminate ()) >> return ()
          atomically $ readTMVar doneShuttingDown
          NS.close socket
    else do
      (atomically $ tryPutTMVar terminate ()) >> return ()
      atomically $ readTMVar doneShuttingDown
      NS.close socket

-- | Update assigned names for remote node.
updateRemoteNames :: NodeId -> NodeM ()
updateRemoteNames nid = do
  names <- liftIO . atomically . readTVar =<< nodeNames . nodeInfo <$> St.get
  forM_ (M.keys names) $ \name ->
    case M.lookup name names of
      Just dids -> do
        forM_ dids $ \(did, count) -> do
          replicateM (fromIntegral count) $
            sendRemoteMessage nid $ RemoteAssignMessage { rassName = name,
                                                          rassDestId = did }
      Nothing -> error "impossible"
