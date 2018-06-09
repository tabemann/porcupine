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
             DeriveGeneric, MultiParamTypeClasses #-}

module Control.Concurrent.Porcupine.Private.Types

  (Process (..),
   Entry,
   Handler,
   Header,
   Payload,
   Name,
   Key,
   ProcessInfo (..),
   Node (..),
   NodeState (..),
   NodeM,
   ProcessState (..),
   GroupState (..),
   RemoteNodeState (..),
   ProcessId (..),
   NodeId (..),
   PartialNodeId (..),
   SockAddr' (..),
   toSockAddr',
   fromSockAddr',
   GroupId (..),
   Message (..),
   SourceId (..),
   DestId (..),
   RemoteEvent (..),
   RemoteMessage (..),
   UserRemoteConnectFailed (..),
   UserRemoteDisconnected (..))

where

import Control.Concurrent.Async (Async)
import Data.ByteString.Lazy (ByteString)
import Data.Sequence (Seq)
import Control.Concurrent.STM (TVar)
import Control.Concurrent.STM.TQueue (TQueue)
import Control.Concurrent.STM.TMVar (TMVar)
import Network.Socket (Socket,
                       SockAddr (..),
                       PortNumber,
                       HostAddress,
                       FlowInfo,
                       HostAddress6,
                       ScopeID,
                       hostAddressToTuple,
                       tupleToHostAddress,
                       hostAddress6ToTuple,
                       tupleToHostAddress6)
import System.Random (StdGen)
import Control.Monad (Monad)
import Control.Monad.Trans.State.Strict (StateT)
import Data.HashMap.Lazy (HashMap)
import Text.Printf (printf)
import Data.Binary (Binary (..))
import Data.Word (Word8.
                  Word16,
                  Word32)
import Control.Monad.Fail (fail)
import Control.Monad.IO.Class (MonadIO (..))
import Control.Exception.Base (SomeException)
import Data.Hashable (Hashable(..))
import Data.Bits (xor,
                  complement,
                  shift,
                  (.|.))
import Data.Functor ((<$>))

-- | The process monad type
data Process a = Process (StateT ProcessInfo IO a)

-- | The process monad type Monad instance
instance Monad Process where
  (Process x) >>= f = Process (x >>= \value -> let (Process y) = f value in y)
  return x = Process $ return x

-- | The process monad type MonadIO instance
instance MonadIO Process where
  liftIO = Process . liftIO

-- | The entry point type
type Entry = SourceId -> Header -> Payload -> Process a

-- | The message handler type
type Handler a = SourceId -> DestId -> Header -> Payload ->
                 Maybe (Action a)

-- | The header type
type Header = ByteString

-- | The payload type
type Payload = ByteString

-- | The name type
type Name = ByteString

-- | The key type
type Key = ByteString

-- | The process information type
data ProcessInfo =
  ProcessInfo { procId :: ProcessId,
                procQueue :: TQueue Message,
                procExtra :: TVar (Seq Message)
                procNode :: Node }

-- | The local node information type
data Node =
  Node { nodeId :: NodeId,
         nodeQueue :: TQueue Message,
         nodeRemoteQueue :: TQueue RemoteEvent,
         nodeGen :: TVar StdGen,
         nodeNextSequenceNum :: TVar Integer,
         nodeNames :: TVar (HashMap Name (Seq (DestId, Integer))) }

-- | The local node state type
data NodeState =
  NodeState { nodeInfo :: Node,
              nodeKey :: Key,
              nodeReadOrder :: Bool,
              nodeTerminate :: TMVar (),
              nodeProcesses :: HashMap ProcessId ProcessState,
              nodeLocalNodes :: HashMap NodeId Node,
              nodeRemoteNodes :: HashMap NodeId RemoteNodeState,
              nodePendingRemoteNodes :: Seq PendingRemoteNodeState,
              nodeGroups :: HashMap GroupId GroupState }

-- | A node state monad convenience type
type NodeM a = StateT NodeState IO a

-- | The process state type
data ProcessState =
  ProcessState { pstateInfo :: ProcessInfo,
                 pstateAsync :: Async a,
                 pstateTerminating :: Bool,
                 pstateEndMesssage :: Maybe (Header, Payload),
                 pstateEndCause :: Maybe ProcessId,
                 pstateEndListeners :: Seq (DestId, Integer) }

-- | The group state type
data GroupState =
  GroupState { groupId :: GroupId,
               groupLocalSubscribers :: Seq (ProcessId, Integer),
               groupRemoteSubscribers :: Seq (NodeId, Integer),
               groupEndListeners :: Seq (DestId, Integer) }

-- | The remote node state type
data RemoteNodeState =
  RemoteNodeState { rnodeId :: NodeId,
                    rnodeOutput :: TQueue RemoteMessage,
                    rnodeEndListeners :: Seq (DestId, Integer) }

-- | The pending remote node state type
data PendingRemoteNodeState =
  PendingRemoteNodeState { prnodeId :: PartialNodeId,
                           prnodeOutput :: TQueue RemoteMessage,
                           prnodeEndListeners :: Seq (DestId, Integer) }

-- | The process Id type
data ProcessId =
  ProcessId { pidSequenceNum :: Integer,
              pidRandomNum :: Integer,
              pidNodeId :: NodeId }
  deriving (Eq, Ord)

-- | THe process Id Show instance
instance Show ProcessId where
  show pid = printf "pid:%d;%d@%s" (pidSequenceNum pid) (pidRandomNum pid)
             (show $ pidNodeId pid)

-- | The process Id Binary instance
instance Binary ProcessId where
  put pid = do put $ pidSequenceNum pid
               put $ pidRandomNum pid
               put $ pidNodeId pid
  get = do sequenceNum <- get
           randomNum <- get
           nid <- get
           return $ ProcessId { pidSequenceNum = sequenceNum,
                                pidRandomNum = randomNum,
                                pidNodeId = nid }

-- | The process Id Hashble instance
instance Hashable ProcessId where
  hashWithSalt salt pid =
    hashWithSalt salt (pidSequenceNum pid) `xor`
    hashWithSalt salt (pidRandomNum pid) `xor`
    hashWithSalt salt (pidNodeId pid)

-- | The node Id type
data NodeId =
  NodeId { nidFixedNum :: Integer,
           nidAddress :: Maybe SockAddr',
           nidRandomNum :: Integer }
  deriving (Eq, Ord, Generic)

-- | The node Id Show instance
instance Show NodeId where
  show nid = case nidAddress nid of
               Just address -> printf "nid:%d;%d@%s" (nidFixedNum nid)
                               (nidRandomNum nid) (show address)
               Nothing -> printf "nid:%d;%d@local" (nidFixedNum nid)
                          (nidRandomNum nid)

-- | The node Id Binary instance
instance Binary NodeId where
  put nid = do put $ nidFixedNum nid
               put $ nidRandomNum nid
               case nidAddress nid of
                 Nothing -> put $ 0 :: Word8
                 Just address -> do put $ 1 :: Word8
                                    put address
  get = do fixedNum <- get
           randomNum <- get
           hasAddress <- get :: Get Word8
           maybeAddress <- case hasAddress of
             0 -> return Nothing
             1 -> Just <$> get
             _ -> fail "invalid node id serialization"
           return $ NodeId { nidFixedNum = fixedNum,
                             nidRandomNum = randomNum,
                             nidAddress = maybeAddress }

-- | The node Id Hashable instance
instance Hashable NodeId

-- | The partial node Id type
data PartialNodeId =
  PartialNodeId { pnidFixedNum :: Integer,
                  pnidAddress :: Maybe SockAddr',
                  pnidRandomNum :: Naybe Integer }
  deriving (Eq, Ord, Generic)

-- | The partial node Id Show instance
instance Show NodeId where
  show nid =
    let randomNum' = case nidRandomNum nid of
                       Just randomNum -> printf "%d" randomNum
                       Nothing -> "unspecified"
    in case nidAddress nid of
         Just address -> printf "pnid:%d;%s@%s" (nidFixedNum nid)
                         randomNum' (show address)
         Nothing -> printf "pnid:%d;%s@local" (nidFixedNum nid)
                    randomNum'

-- | The partial node Id Binary instance
instance Binary PartialNodeId where
  put pnid = do put $ pnidFixedNum pnid
                case pnidRandomNumber pnid of
                  Nothing -> put $ 0 :: Word8
                  Just randomNum -> do put $ 1 :: Word8
                                       put randomNum
                case pnidAddress pnid of
                 Nothing -> put $ 0 :: Word8
                 Just address -> do put $ 1 :: Word8
                                    put address
  get = do fixedNum <- get
           hasRandomNum <- get :: Get Word8
           maybeRandomNum <- case hasRandomNum of
             0 -> return Nothing
             1 -> Just <$> get
             _ -> fail "invalid partial node id serialization"
           hasAddress <- get :: Get Word8
           maybeAddress <- case hasAddress of
             0 -> return Nothing
             1 -> Just <$> get
             _ -> fail "invalid partial node id serialization"
           return $ PartialNodeId { pnidFixedNum = fixedNum,
                                    pnidRandomNum = maybeRandomNum,
                                    pnidAddress = maybeAddress }

-- | Get partial node Id of node Id
partialNodeIdOfNodeId NodeId{..} =
  PartialNodeId { pnidFixedNum = nidFixedNum,
                  pnidAddress = nidAddress,
                  pnidRandomNum = Just nidRandomNum }

-- | The partial node Id Hashable instance
instance Hashable PartialNodeId

-- | A new SockAddr class
data SockAddr' = SockAddrInet' PortNumber HostAddress
               | SockAddrInet6' PortNumber FlowInfo HostAddress6 ScopeID
               | SockAddrUnix' String
               deriving (Generic)

-- | Convert a SockAddr to a new SockAddr
toSockAddr' (SockAddrInet port address) = SockAddrInet' port address
toSockAddr' (SockAddrInet6 port flow address scope) =
  SockAddrInet6' port flow address scope
toSockAddr' (SockAddrUnix path) = SockAddrUnix' path
toSockAddr' _ = error "only inet, inet6, and unix sockaddrs accepted"

-- | Convert a new SockAddr to a SockAddr
fromSockAddr' (SockAddrInet' port address) = SockAddrInet port address
fromSockAddr' (SockAddrInet6' port flow address scope) =
  SockAddrInet6 port flow address scope
fromSockAddr' (SockAddrUnix' path) = SockAddrUnix path

-- | An Eq istance for SockAddr'
instance Eq SockAddr' where
  x == y = fromSockAddr' x == fromSockAddr' y

-- | An Ord instance for SockAddr'
instance Ord SockAddr' where
  x `compare` y = (fromSockAddr' x) `compare` (fromSockAddr' y)

-- | A Show instance for SockAddr'
instance Show SockAddr' where
  show (SockAddrInet' port address) =
    let (x, y, z, w) = hostAddressToTuple address
    in printf "ipv4:%d.%d.%d.%d:%d" x y z w port
  show (SockAddrInet6' port flow address scope) =
    let (p0, p1, p2, p3, p4, p5, p6, p7) = hostAddress6ToTuple address
    in printf "ipv6:%04x:%04x:%04x:%04x:%04x:%04x:%04x:%04x%%x;%d"
       p0 p1 p2 p3 p4 p5 p6 p7 scope port
  show (SockAddrUnix' path) = printf "unix:%s" path

-- | A Binary instance for SockAddr'
instance Binary SockAddr' where
  put (SockAddrInet' port address) = do
    let (x, y, z, w) = hostAddressToTuple address
    put $ 0 :: Word8
    put x
    put y
    put z
    put w
    put $ fromIntegral port :: Word16
  put (SockAddrInet6' port flow address scope) = do
    let (p0, p1, p2, p3, p4, p5, p6, p7) = hostAddress6ToTuple address
    put $ 1 :: Word8
    put flow
    put p0
    put p1
    put p2
    put p3
    put p4
    put p5
    put p6
    put p7
    put scope
  put (SockAddrUnix' path) = do
    put $ 2 :: Word8
    put path
  get = do
    key <- get :: Get Word8
    case key of
      0 -> do
        x <- get
        y <- get
        z <- get
        w <- get
        port <- get :: Get Word16
        return $ SockAddrInet' (fromIntegral port) (tupleToHostAddress x y z w)
      1 -> do
        flow <- get
        p0 <- get
        p1 <- get
        p2 <- get
        p3 <- get
        p4 <- get
        p5 <- get
        p6 <- get
        p7 <- get
        scope <- get
        return $ SockAddrInet6' flow
          (tupleToHostAddress6 p0 p1 p2 p3 p4 p5 p6 p7) scope
      2 -> SockAddrUnix' <$> get
      _ -> fail "invalid sockaddr serialization"

-- | The SockAddr' Hashable instance
instance Hashable SockAddr'

-- | The group Id type
data GroupId =
  GroupId { gidSequenceNum :: Integer,
            gidRandomNum :: Integer,
            gidOriginalNodeId :: Maybe NodeId }
  deriving (Eq, Ord, Generic)

-- | The group Id Show instance
instance Show GroupId where
  show gid =
    case gidOriginalNodeId of
      Just nid -> printf "gid:%d;%d@%s" (gidSequenceNum gid) (gidRandomNum gid)
                  (show nid)
      Nothing -> printf "gid:%d;%d@none" (gidSequenceNum gid) (gidRandomNum gid)

-- | The group Id Binary instance
instance Binary GroupId where
  put gid = do put $ gidSequenceNum gid
               put $ gidRandomNum gid
               case gidOriginalNodeId gid of
                 Nothing -> put $ 0 :: Word8
                 Just nid -> do put $ 1 :: Word8
                                put nid
  get = do sequenceNum <- get
           randomNum <- get
           hasNid <- get :: Get Word8
           maybeNid <- case hasNid of
             0 -> return Nothing
             1 -> Just <$> get
             _ -> fail "invalid group id serialization"
           return $ GroupId { gidSequenceNum = sequenceNum,
                              gidRandomNum = randomNum,
                              gidOriginalNodeId = maybeNid }

-- | The group Id Hashable instance
instance Hashable GroupId

-- | The message type
data Message = UserMessage { umsgSourceId :: SourceId,
                             umsgDestId :: DestId,
                             umsgHeader :: Header,
                             umsgPayload :: Payload }
             | SpawnMessage { spawnSourceId :: SourceId,
                              spawnEntry :: Entry,
                              spawnProcessId :: ProcessId,
                              spawnHeader :: Header,
                              spawnPayload :: Payload }
             | QuitMessage { quitProcessId :: ProcessId,
                             quitHeader :: Header,
                             quitPayload :: Payload }
             | EndMessage { endProcessId :: ProcessId,
                            endException :: Maybe SomeException }
             | KillMessage { killProcessId :: ProcessId,
                             killDestId :: DestId,
                             killHeader :: Header,
                             killPayload :: Payload }
             | SubscribeMessage { subProcessId :: ProcessId,
                                  subGroupId :: GroupId }
             | UnsubscribeMessage { usubProcessId :: ProcessId,
                                    usubGroupId :: GroupId }
             | AssignMessage { assName :: Name,
                               assDestId :: DestId }
             | UnassignMessage { uassName :: Name,
                                 uassDestId :: DestId }
             | ShutdownMessage { shutProcessId :: ProcessId,
                                 shutNodeId :: NodeId,
                                 shutHeader :: Header,
                                 shutPayload :: Payload }
             | ConnectMessage { connNode :: Node }
             | ConnectRemoteMessage { conrNodeId :: PartialNodeId }
             | ListenEndMessage { lendListenedId :: DestId,
                                  lendListenerId :: DestId }
             | UnlistenEndMessage { ulendListenedId :: DestId,
                                    ulendListenerId :: DestId }
             | HelloMessage { heloNode :: Node }
             | JoinMessage { joinNode :: Node }

-- | The message source type
data SourceId = NoSource
              | NormalSource ProcessId
              | CauseSource { causeSourceId :: ProcessId,
                              causeCauseId :: ProcessId }
              deriving (Eq, Ord, Generic)

-- | The message source type Binary instance
instance Binary SourceId where
  put NoSource = put $ 0 :: Word8
  put (NormalSource pid) = do put $ 1 :: Word8
                              put pid
  put CauseSource{..} = do put $ 2 :: Word8
                           put causeSourceId
                           put causeCauseId
  get = do select <- get :: Get Word8
           case select of
             0 -> return NoSource
             1 -> NormalSource <$> get
             2 -> do sourceId <- get
                     causeId <- get
                     return $ CauseSource { causeSourceId = sourceId,
                                            causeCauseId = causeId }
             _ -> fail "invalid source id serialization"

-- | The group Id Hashable instance
instance Hashable SourceId

-- | The message destination type
data DestId = ProcessDest ProcessId
            | GroupDest GroupId
            deriving (Eq, Ord, Generic)

-- | The message distination type Binary instance
instance Binary DestId where
  put (ProcessDest pid) = do put $ 0 :: Word8
                             put pid
  put (GroupDest gid) = do put $ 1 :: Word8
                           put gid
  get = do select <- get :: Get Word8
           case select of
             0 -> ProcessDest <$> get
             1 -> GroupDest <$> get
             _ -> fail "invalid destination id serialization"

-- | The message destination type Hashable instance
instance Hashable DestId

-- | The remote event type
data RemoteEvent = RemoteConnected { rconNodeId :: NodeId,
                                     rconSocket :: Socket,
                                     rconBuffer :: ByteString }
                 | RemoteConnectFailed { rcflNodeId : PartialNodeId }
                 | RemoteReceived { recvNodeId :: NodeId,
                                    recvMessage :: RemoteMessage }
                 | RemoteDisconnected { dconNodeId :: NodeId }
                 deriving (Eq, Ord)

-- | The remote message type
data RemoteMessage = RemoteUserMessage { rumsgSourceId :: SourceId,
                                         rumsgDestId :: DestId,
                                         rumsgHeader :: Header,
                                         rumsgPayload :: Payload }
                   | RemoteEndMessage { rendSourceId :: SourceId,
                                        rendHeader :: Header,
                                        rendPayload :: Payload }
                   | RemoteKillMessage { rkillProcessId :: ProcessId,
                                         rkillDestId :: DestId,
                                         rkillHeader :: Header,
                                         rkillPayload :: Payload }
                   | RemoteSubscribeMessage { rsubProcessId :: ProcessId,
                                              rsubGroupId :: GroupId }
                   | RemoteUnsubscribeMessage { rusubProcessId :: ProcessId,
                                                rusubGroupId :: GroupId }
                   | RemoteAssignMessage { rassName :: Name,
                                           rassDestId :: DestId }
                   | RemoteUnassignMessage { ruassName :: Name,
                                             ruassDestId :: DestId }
                   | RemoteShutdownMessage { rshutProcessId :: ProcessId,
                                             rshutHeader :: Header,
                                             rshutPayload :: Payload }
                   | RemoteHelloMessage { rheloNodeId :: NodeId,
                                          rheloKey :: Key }
                   | RemoteListenEndMessage { rlendListenedId :: DestId,
                                              rlendListenerId :: DestId }
                   | RemoteUnlistenEndMessage { rulendListenedId :: DestId,
                                                rulendListenerId :: DestId }
                   | RemoteJoinMessage { rjoinNodeId :: NodeId }
                   | RemoteLeaveMessage
                   | RemotePartMessage { rpartNodeId :: NodeId }
                   deriving (Eq, Ord, Generic)

-- | The remote message type Binary instance
instance Binary RemoteMessage where
  put RemoteUserMessage{..} = do
    put $ 0 :: Word8
    put rumsgSourceId
    put rumsgDestId
    put rumsgHeader
    put rumsgPayload
  put RemoteEndMessage{..} = do
    put $ 1 :: Word8
    put rendSourceId
    put rendHeader
    put rendPayload
  put RemoteKillMessage{..} = do
    put $ 2 :: Word8
    put rkillProcessId
    put rkillDestId
    put rkillHeader
    put rkillPayload
  put RemoteSubscribeMessage{..} = do
    put $ 3 :: Word8
    put rsubProcessId
    put rsubGroupId
  put RemoteUnsubscribeMessage{..} = do
    put $ 4 :: Word8
    put rusubProcessId
    put rusubGroupId
  put RemoteAssignMessage{..} = do
    put $ 5 :: Word8
    put rassName
    put rassDestId
  put RemoteUnassignMessage{..} = do
    put $ 6 :: Word8
    put ruassName
    put ruassDestId
  put RemoteShutdownMessage{..} = do
    put $ 7 :: Word8
    put rshutProcessId
    put rshutHeader
    put rshutPayload
  put RemoteHelloMessage{..} = do
    put $ 8 :: Word8
    put rheloNodeId
    put rheloKey
  put RemoteListenEndMessage{..} = do
    put $ 9 :: Word8
    put rlendListenedId
    put rlendListenerId
  put RemoteUnlistenEndMessaege{..} = do
    put $ 10 :: Word8
    put rulendListenedId
    put rulendListenerId
  put RemoteJoinMessage{..} = do
    put $ 11 :: Word8
    put rjoinNodeId
  put RemoteLeaveMessage = do
    put $ 12 :: Word8
  put RemotePartMessage = do
    put $ 13 :: Word8
    put rpartNodeId
  get = do
    select <- get :: Get Word8
    case select of
      0 -> do
        sourceId <- get
        destId <- get
        header <- get
        payload <- get
        return $ RemoteUserMessage { rumsgSourceId = sourceId,
                                     rumsgDestId = destId,
                                     rumsgHeader = header,
                                     rumsgPayload = payload }
      1 -> do
        sourceId <- get
        header <- get
        payload <- get
        return $ RemoteEndMessage { rendSourceId = sourceId,
                                    rendHeader = header,
                                    rendPayload = payload }
      2 -> do
        processId <- get
        destId <- get
        header <- get
        payload <- get
        return $ RemoteKillMessage { rkillProcessId = processId,
                                     rkillDestId = destId,
                                     rkillHeader = header,
                                     rkillPayload = payload }
      3 -> do
        processId <- get
        groupId <- get
        return $ RemoteSubscribeMessage { rsubProcessId = processId,
                                          rsubGroupId = groupId }
      4 -> do
        processId <- get
        groupId <- get
        return $ RemoteUnsubscribeMessage { rusubProcessId = processId,
                                            rusubGroupId = groupId }
      5 -> do
        name <- get
        destId <- get
        return $ RemoteAssignMessage { rassName = name,
                                       rassDestId = destId }
      6 -> do
        name <- get
        destId <- get
        return $ RemoteUnassignMessage { ruassName = name,
                                         ruassDestId = destId }
      7 -> do
        processId <- get
        header <- get
        payload <- get
        return $ RemoteShutdownMessage { rshutProcessId = processId,
                                         rshutHeader = header,
                                         rshutPayload = payload }
      8 -> do
        nodeId <- get
        key <- get
        return $ RemoteHelloMessage { rheloNodeId = nodeId,
                                      rheloKey = key }
      9 -> do
        listenedId <- get
        listenerId <- get
        return $ RemoteListenEndMessage { rlendListenedId = listenedId,
                                          rlendListenerId = listenerId }
      10 -> do
        listenedId <- get
        listenerId <- get
        return $ RemoteUnlistenEndMessage { rulendListenedId = listenedId,
                                            rulendListenerId = listenerId }
      11 -> do
        nid <- get
        return $ RemoteJoinMessage { rjoinNodeId = nid }
      12 -> return RemoteLeaveMessage
      13 -> do
        nid <- get
        return $ RemotePartMessage { rpartNodeId = nid }
      _ -> fail "invalid remote message serialization"

-- | Remote messsage Hashable instance
instance Hashable RemoteMessage

-- | Pending remote node connect failed message.
data UserRemoteConnectFailed =
  UserRemoteConnectFailed { urcfNodeId :: PartialNodeId }
  deriving (Eq, Ord, Generic)

-- | Pending remote node connect failed message Binary instance.
instance Binary UserRemoteConnectFailed where
  put UserRemoteConnectFailed{..} = put urcfNodeId
  get = do pnid <- get
           return $ UserRemoteConnectFailed { urcfNodeId = pnid }

-- | Pending remote node connect failed message Show instance.
instance Show UserRemoteConnectFailed where
  show UserRemoteConnectFailed{..} =
    printf "userRemoteConnectFailed:%s" $ show urcfNodeId

-- | Pending remote node connect failed message Hashable instance
instance Hashable UserRemoteConnectFailed

-- | Remote node disconnected message.
data UserRemoteDisconnected =
  UserRemoteDisconnected { urdcNodeId :: NodeId }
  deriving (Eq, Ord, Generic)

-- | Remote node disconnected message Binary instance.
instance Binary UserRemoteDisconnected where
  put UserRemoteDisconnected{..} = put urdcNodeId
  get = do nid <- get
           return $ UserRemoteDisconnected { urdcNodeId = nid }

-- | Remote node disconnected message Show instance.
instance Show UserRemoteDisconnected where
  show UserRemoteDisconnected{..} =
    printf "userRemoteDisconnected:%s" $ show urcfNodeId

-- | Remote node disconnected message Hashable instance
instance Hashable UserRemoteDisconnected
