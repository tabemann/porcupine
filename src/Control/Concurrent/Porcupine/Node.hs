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
import qualified Data.ByteString.Lazy as BSL
import qualified Data.Binary as B
import qualified Data.Sequence as S
import qualified Data.Text as T
import qualified Data.HashMap.Strict as M
import qualified System.Random as R
import qualified Network.Socket as NS
import qualified Network.Socket.ByteString as NSB
import qualified Data.ByteString as BS
import Data.Word (Word32,
                  Word64)
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
                      when,
                      (=<<))
import Data.Functor ((<$>),
                     fmap)
import Data.Foldable (foldl',
                      toList)
import Data.Monoid (mconcat)
import Text.Printf (printf)
import Debug.Trace (trace)

-- | The magic value
magicValue :: Word32
magicValue = 0x904C0914

-- | Do log or not
logActive :: Bool
logActive = False

-- | Log a message.
logMessage :: MonadIO a => String -> a ()
logMessage string = when logActive . liftIO $ putStr string

-- | Start a node with a given fixed index, optional address, and optional key.
start :: Integer -> Maybe NS.SockAddr -> Key -> IO Node
start fixedNum address key = do
  gen <- R.newStdGen
  let (randomNum, gen') = R.random gen
      nid = NodeId { nidFixedNum = fixedNum,
                     nidAddress = toSockAddr' <$> address,
                     nidRandomNum = randomNum }
  queue <- atomically newTQueue
  names <- atomically $ newTVar M.empty
  gen'' <- atomically $ newTVar gen'
  nextSequenceNum <- atomically $ newTVar 0
  terminate <- atomically newEmptyTMVar
  shutdown <- atomically newEmptyTMVar
  listenShutdown <- atomically newEmptyTMVar
  let info = Node { nodeId = nid,
                    nodeQueue = queue,
                    nodeGen = gen'',
                    nodeNextSequenceNum = nextSequenceNum,
                    nodeShutdown = shutdown,
                    nodeNames = names }
      nodeState = NodeState { nodeInfo = info,
                              nodeKey = key,
                              nodeTerminate = terminate,
                              nodeListenShutdown = listenShutdown,
                              nodeProcesses = M.empty,
                              nodeLocalNodes = M.empty,
                              nodeRemoteNodes = M.empty,
                              nodePendingRemoteNodes = S.empty,
                              nodeGroups = M.empty }
  async $ St.runStateT runNode nodeState >> return ()
  case address of
    Just address -> startListen nid address queue terminate listenShutdown
                    key
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
  queue <- nodeQueue . nodeInfo <$> St.get
  shutdown <- nodeShutdown . nodeInfo <$> St.get
  event <- liftIO . atomically $ do
    (Left <$> readTMVar shutdown) `orElse` (Right <$> readTQueue queue)
  case event of
    Right event -> do
      state <- St.get
      state' <- do
        liftIO $ catch (do (_, state') <- St.runStateT (handleEvent event) state
                           return state')
          (\e -> do putStrLn $ show (e :: SomeException)
                    return state)
      St.modify $ \_ -> state'
      runNode
    Left () ->
      logMessage "Actually shutting down\n"

-- | Handle a local message.
handleLocalMessage :: LocalMessage -> NodeM ()
handleLocalMessage UserMessage{..} = do
  logMessage $ printf "GOT UserMessage\n"
  handleLocalUserMessage umsgMessage
handleLocalMessage SpawnMessage{..} = do
  logMessage $ printf "GOT SpawnMessage\n"
  handleLocalSpawnMessage spawnMessage spawnEntry spawnProcessId
    spawnEndListeners
handleLocalMessage QuitMessage{..} = do
  logMessage $ printf "GOT QuitMessage FROM %s\n" $ show quitProcessId
  handleLocalQuitMessage quitProcessId quitHeader quitPayload
handleLocalMessage EndMessage{..} = do
  logMessage $ printf "GOT EndMessage\n"
  handleLocalEndMessage endProcessId endException
handleLocalMessage KillMessage{..} = do
  logMessage $ printf "GOT KillMessage\n"
  handleLocalKillMessage killProcessId killDestId killHeader killPayload
handleLocalMessage SubscribeMessage{..} = do
  logMessage $ printf "GOT SubscribeMessage\n"
  handleLocalSubscribeMessage subProcessId subGroupId
handleLocalMessage UnsubscribeMessage{..} = do
  logMessage $ printf "GOT UnsubscribeMessage\n"
  handleLocalUnsubscribeMessage usubProcessId usubGroupId
handleLocalMessage AssignMessage{..} = do
  logMessage $ printf "GOT AssignMessage\n"
  handleLocalAssignMessage assName assDestId
handleLocalMessage UnassignMessage{..} = do
  logMessage $ printf "GOT UnassignMessage\n"
  handleLocalUnassignMessage uassName uassDestId
handleLocalMessage ShutdownMessage{..} = do
  logMessage $ printf "GOT ShutdownMessage FROM %s\n" $ show shutProcessId
  handleLocalShutdownMessage shutProcessId shutNodeId shutHeader shutPayload
handleLocalMessage ConnectMessage{..} = do
  logMessage $ printf "GOT ConnectMessage\n"
  handleLocalConnectMessage connNode
handleLocalMessage ConnectRemoteMessage{..} = do
  logMessage $ printf "GOT ConnectRemoteMessage\n"
  handleLocalConnectRemoteMessage conrNodeId
handleLocalMessage ListenEndMessage{..} = do
  logMessage $ printf "GOT ListenEndMessage\n"
  handleLocalListenEndMessage lendListenedId lendListenerId
handleLocalMessage UnlistenEndMessage{..} = do
  logMessage $ printf "GOT UnlistenEndMessage\n"
  handleLocalUnlistenEndMessage ulendListenedId ulendListenerId
handleLocalMessage HelloMessage{..} = do
  logMessage $ printf "GOT HelloMessage\n"
  handleLocalHelloMessage heloNode
handleLocalMessage JoinMessage{..} = do
  logMessage $ printf "GOT JoinMessage\n"
  handleLocalJoinMessage joinNode

-- | Handle a remote event.
handleEvent :: Event -> NodeM ()
handleEvent RemoteConnected{..} = do
  logMessage . printf "GOT RemoteConnected FROM %s\n" $ show rconNodeId
  handleRemoteConnected rconNodeId rconSocket rconBuffer
handleEvent RemoteConnectFailed{..} = do
  logMessage . printf "GOT RemoteConnectFailed FROM %s\n" $ show rcflNodeId
  handleRemoteConnectFailed rcflNodeId
handleEvent RemoteReceived{..} =  
  handleRemoteMessage recvNodeId recvMessage
handleEvent RemoteDisconnected{..} = do
  logMessage . printf "GOT RemoteDisconnected FROM %s\n" $ show dconNodeId
  handleRemoteDisconnected dconNodeId
handleEvent LocalReceived{..} = do
  handleLocalMessage lrcvMessage

-- | Handle a remote message.
handleRemoteMessage :: NodeId -> RemoteMessage -> NodeM ()
handleRemoteMessage nodeId RemoteUserMessage{..} = do
  logMessage $ printf "GOT RemoteUserMessage FROM %s\n" (show nodeId)
  handleRemoteUserMessage nodeId rumsgMessage
handleRemoteMessage nodeId RemoteEndMessage{..} = do
  logMessage . printf "GOT RemoteEndMessage FROM %s" $ show nodeId
  handleRemoteEndMessage nodeId rendSourceId rendHeader rendPayload
handleRemoteMessage nodeId RemoteKillMessage{..} = do
  logMessage . printf "GOT RemoteKillMessage FROM %s" $ show nodeId
  handleRemoteKillMessage nodeId rkillProcessId rkillDestId rkillHeader
    rkillPayload
handleRemoteMessage nodeId RemoteSubscribeMessage{..} = do
  logMessage . printf "GOT RemoteSubscribeMessage FROM %s" $ show nodeId
  handleRemoteSubscribeMessage nodeId rsubProcessId rsubGroupId
handleRemoteMessage nodeId RemoteUnsubscribeMessage{..} = do
  logMessage . printf "GOT RemoteUnsubscribeMessage FROM %s" $ show nodeId
  handleRemoteUnsubscribeMessage nodeId rusubProcessId rusubGroupId
handleRemoteMessage nodeId RemoteAssignMessage{..} = do
  logMessage . printf "GOT RemoteAssignMessage FROM %s" $ show nodeId
  handleRemoteAssignMessage nodeId rassName rassDestId
handleRemoteMessage nodeId RemoteUnassignMessage{..} = do
  logMessage . printf "GOT RemoteUnassignMessage FROM %s" $ show nodeId
  handleRemoteUnassignMessage nodeId ruassName ruassDestId
handleRemoteMessage nodeId RemoteShutdownMessage{..} = do
  logMessage . printf "GOT RemoteShutdownMessage FROM %s" $ show nodeId
  handleRemoteShutdownMessage nodeId rshutProcessId rshutHeader rshutPayload
handleRemoteMessage nodeId RemoteHelloMessage{..} = do
  logMessage . printf "GOT RemoteHelloMessage FROM %s" $ show nodeId
  handleRemoteHelloMessage nodeId rheloNodeId rheloKey
handleRemoteMessage nodeId RemoteListenEndMessage{..} = do
  logMessage . printf "GOT RemoteListenEndMessage FROM %s" $ show nodeId
  handleRemoteListenEndMessage nodeId rlendListenedId rlendListenerId
handleRemoteMessage nodeId RemoteUnlistenEndMessage{..} = do
  logMessage . printf "GOT RemoteUnlistenEndMessage FROM %s" $ show nodeId
  handleRemoteUnlistenEndMessage nodeId rulendListenedId rulendListenerId
handleRemoteMessage nodeId RemoteJoinMessage{..} = do
  logMessage . printf "GOT RemoteJoinMessage FROM %s" $ show nodeId
  handleRemoteJoinMessage nodeId rjoinNodeId
handleRemoteMessage nodeId RemoteLeaveMessage = do
  logMessage . printf "GOT RemoteLeaveMessage FROM %s" $ show nodeId
  handleRemoteLeaveMessage nodeId

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
  updateRemoteGroups nid

-- | Handle a remote connect failed event.
handleRemoteConnectFailed :: PartialNodeId -> NodeM ()
handleRemoteConnectFailed pnid = do
  pnodes <- nodePendingRemoteNodes <$> St.get
  let header = encode ("remoteConnectFailed" :: T.Text)
      payload = encode $ UserRemoteConnectFailed { urcfNodeId = pnid }
  forM_ pnodes $ \pnode ->
    if prnodeId pnode == pnid
    then do
      forM_ (prnodeEndListeners pnode) $ \(did, _) ->
        sendLocalUserMessage $ Message NoSource did header payload
    else return ()
  St.modify $ \state ->
    state { nodePendingRemoteNodes =
              S.filter (\pnode -> prnodeId pnode /= pnid) $
              nodePendingRemoteNodes state }

-- | Handle a remote disconnected event.
handleRemoteDisconnected :: NodeId -> NodeM ()
handleRemoteDisconnected nid = do
  rnodes <- nodeRemoteNodes <$> St.get
  let header = encode ("remoteDisconnected" :: T.Text)
      payload = encode $ UserRemoteDisconnected  { urdcNodeId = nid }
  case M.lookup nid rnodes of
    Just rnode -> do
      logMessage $ printf "Found remote node for end\n"
      forM_ (rnodeEndListeners rnode) $ \(did, _) -> do
        logMessage . printf "Sending end message to %s\n" $ show did
        sendLocalUserMessage $ Message NoSource did header payload
      St.modify $ \state ->
        state { nodeRemoteNodes = M.delete nid $ nodeRemoteNodes state }
    Nothing -> return ()

-- | Handle a local user message.
handleLocalUserMessage :: Message -> NodeM ()
handleLocalUserMessage message = sendLocalUserMessage message

-- | Handle a local spawn message.
handleLocalSpawnMessage :: Message -> Entry -> ProcessId -> S.Seq DestId ->
                           NodeM ()
handleLocalSpawnMessage message entry processId endListeners = do
  state <- St.get
  queue <- liftIO $ atomically newTQueue
  extra <- liftIO . atomically $ newTVar S.empty
  let processInfo = ProcessInfo { procId = processId,
                                  procQueue = queue,
                                  procExtra = extra,
                                  procNode = nodeInfo state }
  asyncThread <- liftIO . async $ do
    maybeException <-
      catch (do St.evalStateT (do let Process action = entry message
                                  action
                                  return ())
                  processInfo
                return Nothing) $
            (\e -> return $ Just (e :: SomeException))
    atomically . writeTQueue (nodeQueue $ nodeInfo state) $
      LocalReceived { lrcvMessage =
                      EndMessage { endProcessId = processId,
                                   endException = maybeException } }
  let processState =
        ProcessState { pstateInfo = processInfo,
                       pstateAsync = asyncThread,
                       pstateTerminating = False,
                       pstateEndMessage = Nothing,
                       pstateEndCause = Nothing,
                       pstateEndListeners = normalizeEndListeners endListeners }
  St.modify $ \state ->
    state { nodeProcesses = M.insert processId processState $
                            nodeProcesses state }

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
                            Nothing ->
                              Just (quitHeader',
                                    packageMessage header payload),
                        pstateTerminating = True }
                    else process)
    pid

-- | Quit header
quitHeader' :: BS.ByteString
quitHeader' = encode ("quit" :: T.Text)

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
                      Just (diedHeader,
                            encode . T.pack $ show exception)
                  | True -> Nothing
                Nothing -> 
                  Just (diedHeader,
                        encode . T.pack $ show exception)
            Nothing -> Nothing
      sendEndMessageForProcess process message

-- | Died header.
diedHeader :: BS.ByteString
diedHeader = encode ("died" :: T.Text)

-- | Handle a local kill message.
handleLocalKillMessage :: ProcessId -> DestId -> Header -> Payload -> NodeM ()
handleLocalKillMessage processId destId header payload = do
  case destId of
    ProcessDest destPid -> killProcess processId destPid header payload
    GroupDest destGid -> killGroup processId destGid header payload

-- | Handle a local subscribe message.
handleLocalSubscribeMessage :: ProcessId -> GroupId -> NodeM ()
handleLocalSubscribeMessage pid gid = do
  addSubscriber pid gid
  broadcastRemoteMessage $ RemoteSubscribeMessage { rsubProcessId = pid,
                                                    rsubGroupId = gid }

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
                                  S.adjust' (\(pid, count) -> (pid, count - 1))
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

-- | Handle a local assign message.
handleLocalAssignMessage :: Name -> DestId -> NodeM ()
handleLocalAssignMessage name destId = do
  broadcastRemoteMessage $ RemoteAssignMessage { rassName = name,
                                                 rassDestId = destId }
  

-- | Handle a local unassign message.
handleLocalUnassignMessage :: Name -> DestId -> NodeM ()
handleLocalUnassignMessage name destId = do
  broadcastRemoteMessage $ RemoteUnassignMessage { ruassName = name,
                                                   ruassDestId = destId }

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

-- | Handle a local connect message.
handleLocalConnectMessage :: Node -> NodeM ()
handleLocalConnectMessage node = do
  connectLocal node

-- | Handle a local connect remote message.
handleLocalConnectRemoteMessage :: PartialNodeId -> NodeM ()
handleLocalConnectRemoteMessage pnid = do
  connectRemote pnid

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

-- | Handle a local hello message.
handleLocalHelloMessage :: Node -> NodeM ()
handleLocalHelloMessage node = do
  state <- St.get
  case M.lookup (nodeId node) $ nodeLocalNodes state of
    Nothing -> connectLocal node
    Just _ -> return ()
  updateRemoteNames $ nodeId node
  updateRemoteGroups $ nodeId node

-- | Handle a local join message.
handleLocalJoinMessage :: Node -> NodeM ()
handleLocalJoinMessage node = do
  state <- St.get
  case M.lookup (nodeId node) $ nodeLocalNodes state of
    Nothing -> connectLocalWithoutBroadcast node
    Just _ -> return ()

-- | Handle a remote user message.
handleRemoteUserMessage :: NodeId -> Message -> NodeM ()
handleRemoteUserMessage nid msg = do
  handleIncomingUserMessage msg

-- | Handle a remote process end message.
handleRemoteEndMessage :: NodeId -> SourceId -> Header -> Payload ->
                          NodeM ()
handleRemoteEndMessage nid sourceId header payload = return ()

-- | Handle a remote kill message.
handleRemoteKillMessage :: NodeId -> ProcessId -> DestId -> Header -> Payload ->
                           NodeM ()
handleRemoteKillMessage nid processId destId header payload = do
  case destId of
    ProcessDest destPid -> killLocalProcess processId destPid header payload
    GroupDest destGid -> killGroupForRemote processId destGid header payload

-- | Handle a remote subscribe message.
handleRemoteSubscribeMessage :: NodeId -> ProcessId -> GroupId -> NodeM ()
handleRemoteSubscribeMessage _ pid gid = addSubscriber pid gid

-- | Handle a remote unsubscribe message.
handleRemoteUnsubscribeMessage :: NodeId -> ProcessId -> GroupId -> NodeM ()
handleRemoteUnsubscribeMessage nid _ gid = do
  state <- St.get
  case M.lookup gid $ nodeGroups state of
    Just group -> do
      case S.findIndexL (\(nid', _) -> nid == nid') $
           groupRemoteSubscribers group of
        Just index ->
          case S.lookup index $ groupRemoteSubscribers group of
            Just (_, count)
              | count > 1 -> do
                updateGroup
                  (\group ->
                     group { groupRemoteSubscribers =
                               S.adjust' (\(nid, count) -> (nid, count - 1))
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
      forM_ (groupEndListeners group) $ \(did, count) -> do
        updateRemoteNode
          (\node ->
              node { rnodeEndListeners = mconcat . toList $ fmap
                                         (\endListener@(did', count') ->
                                             if did == did'
                                             then if count' > 1
                                                  then S.singleton
                                                       (did', count' - count)
                                                  else S.empty
                                             else S.singleton endListener)
                                         (rnodeEndListeners node) })
          nid
    Nothing -> return ()

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
                  S.adjust' (\(did', count) -> (did', count + 1)) index entries
            in M.insert name entries' names'
          Nothing -> M.insert name (entries |> (did, 1)) names'
      Nothing -> M.insert name (S.singleton (did, 1)) names'

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
                  let entries' = S.adjust' (\(did', count) -> (did', count - 1))
                                 index entries
                  in M.insert name entries' names'
                | True -> M.insert name (S.deleteAt index entries) names'
              Nothing -> names'
          Nothing -> names'
      Nothing -> names'

-- | Handle a remote shutdown message.
handleRemoteShutdownMessage :: NodeId -> ProcessId -> Header -> Payload ->
                               NodeM ()
handleRemoteShutdownMessage nid pid header payload = do
  shutdownLocalNode pid header payload
  nid' <- nodeId . nodeInfo <$> St.get
  logMessage . printf "SHUTTING DOWN %s\n" $ show nid'

-- | Handle a remote hello message.
handleRemoteHelloMessage :: NodeId -> NodeId -> Key -> NodeM ()
handleRemoteHelloMessage _ _ _ = return ()

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

-- | Handle a remote join message.
handleRemoteJoinMessage :: NodeId -> NodeId -> NodeM ()
handleRemoteJoinMessage nid remoteNid = do
  connectRemote (partialNodeIdOfNodeId remoteNid)

-- | Handle a remote leave message.
handleRemoteLeaveMessage :: NodeId -> NodeM ()
handleRemoteLeaveMessage nid = return ()

-- | Send a user message to a process.
sendLocalUserMessage :: Message -> NodeM ()
sendLocalUserMessage message = do
  state <- St.get
  case msgDestId message of
    ProcessDest pid ->
      if pidNodeId pid == nodeId (nodeInfo state)
      then case M.lookup pid $ nodeProcesses state of
             Just process -> do
               liftIO . atomically $ do
                 writeTQueue (procQueue $ pstateInfo process) $
                   UserMessage { umsgMessage = message }
             Nothing -> return ()
      else do sendRemoteMessage (pidNodeId pid) $
                RemoteUserMessage { rumsgMessage = message }
    GroupDest gid ->
      case M.lookup gid $ nodeGroups state of
        Just group -> do
          forM_ (groupLocalSubscribers group) $ \(pid, _) ->
            case M.lookup pid $ nodeProcesses state of
              Just process -> do
                liftIO . atomically $ do
                  writeTQueue (procQueue $ pstateInfo process) $
                    UserMessage { umsgMessage = message }
              Nothing -> return ()
          forM_ (groupRemoteSubscribers group) $ \(nid, _) -> do
            sendRemoteMessage nid $
              RemoteUserMessage { rumsgMessage = message }

-- | Send incoming user message to destination processes.
handleIncomingUserMessage :: Message -> NodeM ()
handleIncomingUserMessage message = do
  state <- St.get
  case msgDestId message of
    ProcessDest pid ->
      if pidNodeId pid == nodeId (nodeInfo state)
      then case M.lookup pid $ nodeProcesses state of
             Just process -> do
               liftIO . atomically $ do
                 writeTQueue (procQueue $ pstateInfo process) $
                   UserMessage { umsgMessage = message }
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
                    UserMessage { umsgMessage = message }
              Nothing -> return ()

-- | Send a local message.
sendLocalMessage :: Node -> LocalMessage -> NodeM ()
sendLocalMessage node message = do
  liftIO . atomically . writeTQueue (nodeQueue node) $
    LocalReceived { lrcvMessage = message }

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
              Nothing -> (encode ("ended" :: T.Text), BS.empty)
      pid = procId $ pstateInfo process
      sourceId =
        case pstateEndCause process of
          Just causeId ->
            CauseSource { causeSourceId = pid,
                          causeCauseId = causeId }
          Nothing -> NormalSource pid
  forM_ (pstateEndListeners process) $ \(did, _) ->
    sendLocalUserMessage $ Message sourceId did header payload
  groups <- M.elems . nodeGroups <$> St.get
  forM_ groups $ \group -> do
    case S.findIndexL (\(pid', _) -> pid == pid') $
         groupLocalSubscribers group of
      Just _ -> do
        forM_ (groupEndListeners group) $ \(did, _) ->
          sendLocalUserMessage $ Message sourceId did header payload
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
                                Nothing ->
                                  Just (killedHeader,
                                        packageMessage header payload),
                            pstateEndCause = Just sourcePid,
                            pstateTerminating = True })
          destPid
      else return ()
    Nothing -> return ()

-- | Killed header
killedHeader :: BS.ByteString
killedHeader = encode ("killed" :: T.Text)

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

-- | Update a pending remote node.
updatePendingRemoteNode :: (PendingRemoteNodeState -> PendingRemoteNodeState) ->
                           PartialNodeId -> NodeM ()
updatePendingRemoteNode f pnid =
  St.modify $
  \state ->
    state { nodePendingRemoteNodes =
              case S.findIndexL (\pnode -> matchPartialNodeId' (prnodeId pnode)
                                           pnid) $
                   nodePendingRemoteNodes state of
                Just index -> S.adjust' f index $ nodePendingRemoteNodes state
                Nothing -> nodePendingRemoteNodes state }

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
                        S.adjust' (\(did, count) -> (did, count + 1))
                        index $ pstateEndListeners process }
          Nothing ->
            process { pstateEndListeners = pstateEndListeners process |>
                                           (listenerId, 1) })
    pid

-- | Register remote node end listener.
registerRemoteNodeEndListener :: NodeId -> DestId -> NodeM ()
registerRemoteNodeEndListener nid listenerId = do
  logMessage $ printf "Adding remote end listener on %s for %s\n"
    (show nid) (show listenerId)
  remoteNodes <- nodeRemoteNodes <$> St.get
  case M.lookup nid remoteNodes of
    Just _ ->
      updateRemoteNode
      (\rnode ->
         case S.findIndexL (\(did, _) -> listenerId == did) $
              rnodeEndListeners rnode of
           Just index ->
             rnode { rnodeEndListeners =
                       S.adjust' (\(did, count) -> (did, count + 1))
                       index $ rnodeEndListeners rnode }
           Nothing ->
             rnode { rnodeEndListeners = rnodeEndListeners rnode |>
                                         (listenerId, 1) })
      nid
    Nothing ->
      updatePendingRemoteNode
      (\pnode ->
         case S.findIndexL (\(did, _) -> listenerId == did) $
              prnodeEndListeners pnode of
           Just index ->
             pnode { prnodeEndListeners =
                       S.adjust' (\(did, count) -> (did, count + 1))
                       index $ prnodeEndListeners pnode }
           Nothing ->
             pnode { prnodeEndListeners = prnodeEndListeners pnode |>
                                          (listenerId, 1) })
      (partialNodeIdOfNodeId nid)

-- | Register group Id end listener.
registerGroupEndListener :: GroupId -> DestId -> NodeM ()
registerGroupEndListener gid listenerId = do
  updateGroup
    (\group ->
        case S.findIndexL (\(did, _) -> listenerId == did) $
             groupEndListeners group of
          Just index ->
            group { groupEndListeners =
                      S.adjust' (\(did, count) -> (did, count + 1))
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
                             S.adjust' (\(did, count) -> (did, count - 1))
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
                             S.adjust' (\(did, count) -> (did, count - 1))
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
                             S.adjust' (\(did, count) -> (did, count - 1))
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
  logMessage "Killing local processes...\n"
  processes <- nodeProcesses <$> St.get
  forM_ processes $ \process ->
    killLocalProcess pid (procId $ pstateInfo process) header payload
  terminate <- nodeTerminate <$> St.get
  logMessage "Broadcasting leave message...\n"
  broadcastRemoteMessage RemoteLeaveMessage
  logMessage "Setting terminate...\n"
  liftIO . atomically $ (tryPutTMVar terminate () >> return ())
  logMessage "Deleting list of remote nodes...\n"
  St.modify $ \state -> state { nodeRemoteNodes = M.empty }
  nid <- nodeId . nodeInfo <$> St.get
  case nidAddress nid of
    Just _ -> do
      logMessage "Waiting for listen shutdown...\n"
      liftIO . atomically . readTMVar . nodeListenShutdown =<< St.get
    Nothing -> return ()
  logMessage "Setting node shutdown...\n"
  liftIO . atomically . (flip putTMVar) () . nodeShutdown . nodeInfo =<< St.get

-- | Handle subscribing to a group.
addSubscriber :: ProcessId -> GroupId -> NodeM ()
addSubscriber pid gid = do
  nid <- nodeId . nodeInfo <$> St.get
  state <- St.get
  if pidNodeId pid == nid
    then do
      case M.lookup gid $ nodeGroups state of
        Just group ->
          case S.findIndexL (\(pid', _) -> pid == pid') $
               groupLocalSubscribers group of
            Just index -> do
              updateGroup
                (\group -> group { groupLocalSubscribers =
                                   S.adjust' (\(pid, count) -> (pid, count + 1))
                                   index $ groupLocalSubscribers group })
                gid
            Nothing -> do
              updateGroup
                (\group -> group { groupLocalSubscribers =
                                   groupLocalSubscribers group |> (pid, 1) })
                gid
        Nothing -> do addGroup gid $
                        GroupState { groupId = gid,
                                     groupLocalSubscribers =
                                       S.singleton (pid, 1),
                                     groupRemoteSubscribers = S.empty,
                                     groupEndListeners = S.empty }
    else do
      case M.lookup gid $ nodeGroups state of
        Just group -> do
          case S.findIndexL (\(nid', _) -> pidNodeId pid == nid') $
               groupRemoteSubscribers group of
            Just index -> do
              updateGroup
                (\group -> group { groupRemoteSubscribers =
                                     S.adjust' (\(nid, count) ->
                                                 (nid, count + 1))
                                     index $ groupRemoteSubscribers group })
                gid
            Nothing -> do
              updateGroup
                (\group -> group { groupRemoteSubscribers =
                                   groupRemoteSubscribers group |>
                                   (pidNodeId pid, 1) })
                gid
          forM_ (groupEndListeners group) $ \(did, count) -> do
            updateRemoteNode
              (\node ->
                 case S.findIndexL (\(did', _) -> did == did') $
                      rnodeEndListeners node of
                   Just index ->
                     case S.lookup index $ rnodeEndListeners node of
                       Just (_, count') ->
                         node { rnodeEndListeners =
                                  S.adjust'
                                  (\(did, count') -> (did, count + count'))
                                  index $ rnodeEndListeners node }
                       Nothing -> error "impossible"
                   Nothing ->
                     node { rnodeEndListeners = rnodeEndListeners node |>
                                                (did, 1) })
              (pidNodeId pid)
        Nothing -> do addGroup gid $
                        GroupState { groupId = gid,
                                     groupLocalSubscribers = S.empty,
                                     groupRemoteSubscribers =
                                       S.singleton (pidNodeId pid, 1),
                                     groupEndListeners = S.empty }
      
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
  let queue = nodeQueue node
  (liftIO . async $ runLocalCommunication selfNid output queue
    terminate) >> return ()

-- | Run local communication with another node
runLocalCommunication :: NodeId -> TQueue RemoteMessage -> TQueue Event ->
                         TMVar () -> IO ()
runLocalCommunication selfNid output queue terminate = do
  event <- atomically $ do
    (Right <$> readTQueue output) `orElse` (Left <$> takeTMVar terminate)
  case event of
    Right remoteMessage -> do
      atomically . writeTQueue queue $
        RemoteReceived { recvNodeId = selfNid,
                         recvMessage = remoteMessage }
      runLocalCommunication selfNid output queue terminate
    Left () -> do
      atomically . writeTQueue queue $
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
      readTMVar terminate `orElse` readTMVar terminateOutput
      tryPutTMVar terminateOutput () >> return ()
    NS.shutdown socket NS.ShutdownReceive
    atomically $ do
      readTMVar finalizeInput
      readTMVar finalizeOutput
    NS.close socket
  input <- nodeQueue . nodeInfo <$> St.get
  (liftIO . async $ do
    runSocketInput nid input terminateOutput finalizeInput socket buffer) >>
    return ()
  (liftIO . async $ do
    runSocketOutput nid output terminateOutput finalizeOutput socket) >>
    return ()

-- | Run socket input.
runSocketInput :: NodeId -> TQueue Event -> TMVar () -> TMVar () ->
                  NS.Socket -> BS.ByteString -> IO ()
runSocketInput nid input terminate finalize socket buffer = do
  catch (runSocketInput' nid input socket buffer)
    (\e -> const (return ()) (e :: IOException))
  atomically $ do
    writeTQueue input $ RemoteDisconnected { dconNodeId = nid }
    tryPutTMVar terminate () >> return ()
    tryPutTMVar finalize () >> return ()
  logMessage . printf "DISCONNECTED FROM %s\n" $ show nid

-- | Actually run socket input
runSocketInput' :: NodeId -> TQueue Event -> NS.Socket ->
                   BS.ByteString -> IO ()
runSocketInput' nid input socket buffer = do
  if BS.length buffer < word64Size
    then do block <- NSB.recv socket 4096
            if not $ BS.null block
              then runSocketInput' nid input socket $ BS.append buffer block
              else return ()
    else let (lengthField, rest) = BS.splitAt word64Size buffer
         in runSocketInput'' nid input socket rest
            (fromIntegral (decode lengthField :: Word64))

-- | After having gotten the length field, continue parsing a message
runSocketInput'' :: NodeId -> TQueue Event -> NS.Socket ->
                    BS.ByteString -> Int -> IO ()
runSocketInput'' nid input socket buffer messageLength = do
  if BS.length buffer < messageLength
    then do block <- NSB.recv socket 4096
            if not $ BS.null block
              then runSocketInput'' nid input socket (BS.append buffer block)
                   messageLength
              else return ()
    else do let (messageBuffer, rest) = BS.splitAt messageLength buffer
                message = decode messageBuffer
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
      let messageData = encode message
          messageLengthField = encode $ BS.length messageData
          fullMessageData = BS.append messageLengthField messageData
      continue <- catch (do NSB.sendAll socket fullMessageData
                            return True)
                  (\e -> const (return False) (e :: IOException))
      if continue
        then runSocketOutput nid input terminate finalize socket
        else do NS.shutdown socket NS.ShutdownBoth
                atomically $ (tryPutTMVar finalize () >> return ())
    Left () -> do NS.shutdown socket NS.ShutdownBoth
                  atomically $ (tryPutTMVar finalize () >> return ())

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
  if not alreadyConnected
    then case pnidAddress pnid of
           Just address -> do
             let address' = fromSockAddr' address
             socket <- liftIO $ NS.socket (familyOfSockAddr address') NS.Stream
                       NS.defaultProtocol
             result <- liftIO $ catch (Right <$> NS.connect socket address')
                       (\e -> const (return $ Left ()) (e :: IOException))
             queue <- nodeQueue . nodeInfo <$> St.get
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
                 liftIO $ handshakeWithSocket nid queue terminate socket
                   key (Just pnid)
               Left _ -> do
                 liftIO $ NS.close socket
                 liftIO . atomically . writeTQueue queue $
                   RemoteConnectFailed { rcflNodeId = pnid }
           Nothing -> return ()
    else return ()

-- | Handshake with socket.
handshakeWithSocket :: NodeId -> TQueue Event -> TMVar () -> NS.Socket ->
                       Key -> Maybe PartialNodeId -> IO ()
handshakeWithSocket nid queue terminate socket key pnid = do
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
    let messageData = encode $ RemoteHelloMessage { rheloNodeId = nid,
                                                    rheloKey = key }
        messageLengthField =
          encode $ (fromIntegral $ BS.length messageData :: Word64)
        magicField = encode magicValue
        fullMessageData =
          BS.append magicField $ BS.append messageLengthField messageData
    continue <- catch (do NSB.sendAll socket fullMessageData
                          return True)
                (\e -> const (return False) (e :: IOException))
    if continue
      then handshakeWithSocket' nid queue terminate socket key BS.empty
           pnid success doneShuttingDown
      else do
        (atomically $ tryPutTMVar terminate ()) >> return ()
        atomically $ readTMVar doneShuttingDown
        NS.close socket
        notifyHandshakeFailure queue pnid
  return ()

-- | Receive the hello message length during handshaking.
handshakeWithSocket' :: NodeId -> TQueue Event -> TMVar () -> NS.Socket ->
                        Key -> BS.ByteString -> Maybe PartialNodeId ->
                        TMVar () -> TMVar () -> IO ()
handshakeWithSocket' nid queue terminate socket key buffer pnid
  success doneShuttingDown = do
  continue <- (== Nothing) <$> (atomically $ tryReadTMVar terminate)
  if continue
    then
      if BS.length buffer < word32Size + word64Size
      then do
        block <- catch (Just <$> NSB.recv socket 4096)
                 (\e -> const (return Nothing) (e :: IOException))
        case block of
          Just block
            | not $ BS.null block ->
              handshakeWithSocket' nid queue terminate socket key
                (BS.append buffer block) pnid success doneShuttingDown
          _ -> do
            (atomically $ tryPutTMVar terminate ()) >> return ()
            atomically $ readTMVar doneShuttingDown
            NS.close socket
            notifyHandshakeFailure queue pnid
      else let (magicField, rest) = BS.splitAt word32Size buffer
               (lengthField, rest') = BS.splitAt word64Size rest
           in if decode magicField == magicValue
              then handshakeWithSocket'' nid queue terminate socket key
                   rest' (fromIntegral (decode lengthField :: Word64)) pnid
                   success doneShuttingDown
              else do
                (atomically $ tryPutTMVar terminate ()) >> return ()
                atomically $ readTMVar doneShuttingDown
                NS.close socket
                notifyHandshakeFailure queue pnid
    else do
      atomically $ readTMVar doneShuttingDown
      NS.close socket
      notifyHandshakeFailure queue pnid

-- | Complete receiving the hello message during handshaking.
handshakeWithSocket'' :: NodeId -> TQueue Event -> TMVar () ->
                         NS.Socket -> Key -> BS.ByteString -> Int ->
                         Maybe PartialNodeId -> TMVar () -> TMVar() -> IO ()
handshakeWithSocket'' nid queue terminate socket key buffer
  messageLength pnid success doneShuttingDown = do
  continue <- (== Nothing) <$> (atomically $ tryReadTMVar terminate)
  if continue
    then
      if BS.length buffer < messageLength
      then do
        block <- catch (Just <$> NSB.recv socket 4096)
                 (\e -> const (return Nothing) (e :: IOException))
        case block of
          Just block
            | not $ BS.null block ->
              handshakeWithSocket'' nid queue terminate socket key
                (BS.append buffer block) messageLength pnid success
                doneShuttingDown
          _ -> do
            (atomically $ tryPutTMVar terminate ()) >> return ()
            atomically $ readTMVar doneShuttingDown
            NS.close socket
            notifyHandshakeFailure queue pnid
      else let (messageData, rest) = BS.splitAt messageLength buffer
               message = decode messageData :: RemoteMessage
           in case message of
                RemoteHelloMessage{..}
                  | rheloKey == key -> do
                      atomically $ do
                        writeTQueue queue $
                          RemoteConnected { rconNodeId = rheloNodeId,
                                            rconSocket = socket,
                                            rconBuffer = rest }
                        putTMVar success ()
                _ -> do
                  (atomically $ tryPutTMVar terminate ()) >> return ()
                  atomically $ readTMVar doneShuttingDown
                  NS.close socket
                  notifyHandshakeFailure queue pnid
    else do
      atomically $ readTMVar doneShuttingDown
      NS.close socket
      notifyHandshakeFailure queue pnid

-- | Notify failure if partial node Id is present.
notifyHandshakeFailure :: TQueue Event -> Maybe PartialNodeId -> IO ()
notifyHandshakeFailure queue (Just pnid) = do
  atomically . writeTQueue queue $
    RemoteConnectFailed { rcflNodeId = pnid }
notifyHandshakeFailure _ Nothing = return ()

-- | Get family of SockAddr
familyOfSockAddr :: NS.SockAddr -> NS.Family
familyOfSockAddr (NS.SockAddrInet _ _) = NS.AF_INET
familyOfSockAddr (NS.SockAddrInet6 _ _ _ _) = NS.AF_INET6
familyOfSockAddr (NS.SockAddrUnix _) = NS.AF_UNIX
familyOfSockAddr _ = error "not supported"
        
-- | Start listening for incoming connections
startListen :: NodeId -> NS.SockAddr -> TQueue Event -> TMVar () ->
               TMVar () -> Key -> IO ()
startListen nid address queue terminate listenShutdown key = do
  socket <- NS.socket (familyOfSockAddr address) NS.Stream NS.defaultProtocol
  NS.setSocketOption socket NS.ReuseAddr 1
  NS.bind socket address
  NS.listen socket 5
  doneShuttingDown <- atomically newEmptyTMVar
  async $ do
    (atomically $ readTMVar terminate) >> return ()
    NS.shutdown socket NS.ShutdownBoth
    atomically $ (tryPutTMVar doneShuttingDown () >> return ())
  (async $ runListen nid queue socket terminate doneShuttingDown
    listenShutdown key) >> return ()

-- | Run listening for incoming connections.
runListen :: NodeId -> TQueue Event -> NS.Socket -> TMVar () ->
             TMVar () -> TMVar () -> Key -> IO ()
runListen nid queue socket terminate doneShuttingDown listenShutdown
  key = do
  continue <- (== Nothing) <$> (atomically $ tryReadTMVar terminate)
  if continue
    then do
      result <- catch (Just <$> NS.accept socket)
                (\e -> const (return Nothing) (e :: IOException))
      case result of
        Just (socket', _) -> do
          handshakeWithSocket nid queue terminate socket' key Nothing
          runListen nid queue socket terminate doneShuttingDown
            listenShutdown key
        Nothing -> do
          atomically $ readTMVar doneShuttingDown
          NS.close socket
          (atomically $ tryPutTMVar listenShutdown ()) >> return ()
    else do
      atomically $ readTMVar doneShuttingDown
      NS.close socket
      (atomically $ tryPutTMVar listenShutdown ()) >> return ()

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

-- | Update groups for remote node.
updateRemoteGroups :: NodeId -> NodeM ()
updateRemoteGroups nid = do
  groups <- nodeGroups <$> St.get
  forM_ (M.elems groups) $ \group -> do
    forM_ (groupLocalSubscribers group) $ \(pid, count) -> do
      replicateM (fromIntegral count) $
        sendRemoteMessage nid $ RemoteSubscribeMessage { rsubProcessId = pid,
                                                         rsubGroupId =
                                                           groupId group }
    forM_ (groupEndListeners group) $ \(did, count) -> do
      replicateM (fromIntegral count) $
        sendRemoteMessage nid $
          RemoteListenEndMessage { rlendListenedId = GroupDest $ groupId group,
                                   rlendListenerId = did }

-- | Put a header and payload in a message container and then encode it.
packageMessage :: Header -> Payload -> BS.ByteString
packageMessage header payload =
  encode $ MessageContainer { mcontHeader = header, mcontPayload = payload }

-- | Normalize end listeners.
normalizeEndListeners :: S.Seq DestId -> S.Seq (DestId, Integer)
normalizeEndListeners dids =
  foldl' (\normalized did ->
            case S.findIndexL (\(did', _) -> did == did') normalized of
              Just index ->
                S.adjust' (\(did, count) -> (did, count + 1)) index normalized
              Nothing -> normalized |> (did, 1))
    S.empty dids

-- | Size of an encoded Word32
word32Size :: Int
word32Size = BS.length $ encode (0 :: Word32)

-- | Size of an encoded Word64
word64Size :: Int
word64Size = BS.length $ encode (0 :: Word64)

-- | Decode data from a strict ByteString.
decode :: B.Binary a => BS.ByteString -> a
decode = B.decode . BSL.fromStrict

-- | Encode data to a strict ByteString
encode :: B.Binary a => a -> BS.ByteString
encode = BSL.toStrict . B.encode
