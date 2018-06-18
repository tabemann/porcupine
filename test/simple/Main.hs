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

import qualified Control.Concurrent.Porcupine.Process as P
import qualified Control.Concurrent.Porcupine.Node as PN
import qualified Data.Text as T
import Data.Text.IO (putStrLn)
import qualified Data.Binary as B
import qualified Data.Sequence as S
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Network.Socket as NS
import qualified Data.HashMap.Lazy as M
import Data.Sequence ((|>),
                      ViewL (..))
import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad (forM,
                      forM_,
                      foldM,
                      replicateM,
                      (=<<),
                      forever)
import Control.Concurrent (threadDelay)
import Data.Functor ((<$>),
                     fmap)
import Data.Foldable (Foldable,
                      foldl')
import Data.Word (Word16)
import Text.Printf (printf)
import Prelude hiding (putStrLn)
import System.IO (hSetBuffering,
                  BufferMode (..),
                  stdout,
                  stderr)

-- | A simple message receiver.
simpleMessageReceiver :: P.Process ()
simpleMessageReceiver = do
  liftIO $ putStrLn "Starting to receive messages..."
  loop
  where loop = do
          P.receive [\sid did header payload ->
                       if (decode header :: T.Text) == "messageText"
                       then Just . liftIO . putStrLn . T.pack $
                            printf "Received %s" (decode payload :: T.Text)
                       else Nothing,
                     \sid did header payload ->
                       if (decode header :: T.Text) == "normalQuit"
                       then Just $ do
                         liftIO $ putStrLn "Exiting receive process..."
                         P.quit'
                       else Nothing]
          loop

-- | A simple mssage sender.
simpleMessageSender0 :: P.ProcessId -> P.Process ()
simpleMessageSender0 pid = do
  liftIO $ putStrLn "Listening for termination..."
  P.listenEnd $ P.ProcessDest pid
  liftIO $ putStrLn "Starting to send messages..."
  forM_ ([1..100] :: S.Seq Integer) $ \n -> do
    liftIO . putStrLn . T.pack $ printf "Sending %d" n
    P.send (P.ProcessDest pid) (encode ("messageText" :: T.Text))
      (encode . T.pack $ printf "%d" n)
  liftIO $ putStrLn "Sending message requesting quit..."
  P.send (P.ProcessDest pid) (encode ("normalQuit" :: T.Text)) BS.empty
  liftIO $ putStrLn "Waiting for termination..."
  P.receive [\sid did header payload ->
               if sid == P.NormalSource pid then Just $ return () else Nothing]
  liftIO $ putStrLn "Shutting down..."
  nid <- P.myNodeId
  P.shutdown' nid

-- | A simple messaging test.
simpleMessagingTest0 :: IO ()
simpleMessagingTest0 = do
  putStrLn "Starting the node..."
  node <- PN.start 0 Nothing BS.empty
  putStrLn "Starting the receiver process..."
  receiverPid <- P.spawnInit' simpleMessageReceiver node
  putStrLn "Starting the sender process..."
  P.spawnInit' (simpleMessageSender0 receiverPid) node
  PN.waitShutdown node
  putStrLn "The node has shut down"

-- | Another simple mssage sender.
simpleMessageSender1 :: P.ProcessId -> PN.Node -> P.Process ()
simpleMessageSender1 pid node = do
  liftIO $ putStrLn "Connecting nodes..."
  P.connect node
  liftIO $ putStrLn "Listening for termination..."
  P.listenEnd $ P.ProcessDest pid
  liftIO $ putStrLn "Starting to send messages..."
  forM_ ([1..100] :: S.Seq Integer) $ \n -> do
    liftIO . putStrLn . T.pack $ printf "Sending %d" n
    P.send (P.ProcessDest pid) (encode ("messageText" :: T.Text))
      (encode . T.pack $ printf "%d" n)
  liftIO $ putStrLn "Sending message requesting quit..."
  P.send (P.ProcessDest pid) (encode ("normalQuit" :: T.Text)) BS.empty
  liftIO $ putStrLn "Waiting for termination..."
  P.receive [\sid did header payload ->
               if sid == P.NormalSource pid then Just $ return () else Nothing]
  liftIO $ putStrLn "Shutting down..."
  P.shutdown' $ PN.getNodeId node
  nid <- P.myNodeId
  P.shutdown' nid
  
-- | Another simple messaging test.
simpleMessagingTest1 :: IO ()
simpleMessagingTest1 = do
  putStrLn "Starting node 0..."
  node0 <- PN.start 0 Nothing BS.empty
  putStrLn "Starting node 1..."
  node1 <- PN.start 1 Nothing BS.empty
  putStrLn "Starting the receiver process..."
  receiverPid <- P.spawnInit' simpleMessageReceiver node0
  putStrLn "Starting the sender process..."
  P.spawnInit' (simpleMessageSender1 receiverPid node0) node1
  PN.waitShutdown node0
  putStrLn "Node 0 has shut down"
  PN.waitShutdown node1
  putStrLn "Node 1 has shut down"

-- | Yet another simple mssage sender.
simpleMessageSender2 :: P.ProcessId -> P.ProcessId -> PN.Node -> P.Process ()
simpleMessageSender2 pid0 pid1 node = do
  liftIO $ putStrLn "Connecting nodes..."
  P.connect node
  liftIO $ putStrLn "Adding processes to a group..."
  gid <- P.newGroupId
  P.subscribeAsProxy gid pid0
  P.subscribeAsProxy gid pid1
  liftIO $ putStrLn "Listening for termination..."
  P.listenEnd $ P.ProcessDest pid0
  P.listenEnd $ P.ProcessDest pid1
  liftIO $ putStrLn "Starting to send messages..."
  forM_ ([1..100] :: S.Seq Integer) $ \n -> do
    liftIO . putStrLn . T.pack $ printf "Sending %d" n
    P.send (P.GroupDest gid) (encode ("messageText" :: T.Text))
      (encode . T.pack $ printf "%d" n)
  liftIO $ putStrLn "Sending message requesting quit..."
  P.send (P.GroupDest gid) (encode ("normalQuit" :: T.Text)) BS.empty
  liftIO $ putStrLn "Waiting for termination..."
  P.receive [\sid did header payload ->
               if sid == P.NormalSource pid0 then Just $ return () else Nothing]
  P.receive [\sid did header payload ->
               if sid == P.NormalSource pid1 then Just $ return () else Nothing]
  liftIO $ putStrLn "Shutting down..."
  P.shutdown' $ PN.getNodeId node
  nid <- P.myNodeId
  P.shutdown' nid
  
-- | Yet another simple messaging test.
simpleMessagingTest2 = do
  putStrLn "Starting node 0..."
  node0 <- PN.start 0 Nothing BS.empty
  putStrLn "Starting node 1..."
  node1 <- PN.start 1 Nothing BS.empty
  putStrLn "Starting receiver process 0..."
  receiverPid0 <- P.spawnInit' simpleMessageReceiver node0
  putStrLn "Starting receiver processs 1..."
  receiverPid1 <- P.spawnInit' simpleMessageReceiver node1
  putStrLn "Starting the sender process..."
  P.spawnInit' (simpleMessageSender2 receiverPid0 receiverPid1 node0) node1
  PN.waitShutdown node0
  putStrLn "Node 0 has shut down"
  PN.waitShutdown node1
  putStrLn "Node 1 has shut down"
  
-- | A simple message receiver.
simpleMessageReceiver3 :: Integer -> Bool -> P.Process ()
simpleMessageReceiver3 index shutdownOnQuit = do
  liftIO $ putStrLn "Starting to receive messages..."
  loop
  where loop = do
          P.receive [\sid did header payload ->
                       if (decode header :: T.Text) == "messageText"
                       then do
                         Just . liftIO $ printf "Received %s for %d\n"
                           (decode payload :: T.Text) index
                       else Nothing,
                     \sid did header payload ->
                       if (decode header :: T.Text) == "normalQuit"
                       then Just $ do
                         liftIO $ putStrLn "Exiting receive process..."
                         if shutdownOnQuit
                           then P.shutdown' =<< P.myNodeId
                           else return ()
                         P.quit'
                       else Nothing]
          loop

-- | Yet another simple mssage sender.
simpleMessageSender3 :: P.ProcessId -> P.ProcessId -> NS.SockAddr ->
                        P.Process ()
simpleMessageSender3 pid0 pid1 address = do
  liftIO $ putStrLn "Connecting nodes..."
  P.connectRemote 0 address Nothing
  liftIO $ putStrLn "Adding processes to a group..."
  gid <- P.newGroupId
  P.subscribeAsProxy gid pid0
  P.subscribeAsProxy gid pid1
  liftIO $ putStrLn "Listening for termination..."
  P.listenEnd $ P.ProcessDest pid0
  P.listenEnd $ P.ProcessDest pid1
  liftIO $ putStrLn "Starting to send messages..."
  forM_ ([1..100] :: S.Seq Integer) $ \n -> do
    liftIO . putStrLn . T.pack $ printf "Sending %d" n
    P.send (P.GroupDest gid) (encode ("messageText" :: T.Text))
      (encode . T.pack $ printf "%d" n)
  liftIO $ putStrLn "Sending message requesting quit..."
  P.send (P.GroupDest gid) (encode ("normalQuit" :: T.Text)) BS.empty
  liftIO $ putStrLn "Waiting for termination..."
  replicateM 2 $ do
    P.receive [\sid did header payload ->
                 if header == encode ("remoteDisconnected" :: T.Text) ||
                    header == encode ("genericQuit" :: T.Text)
                 then Just . liftIO $ putStrLn "Received end"
                 else Nothing]
  liftIO $ putStrLn "Shutting down..."
  P.shutdown' =<< P.myNodeId

-- | Yet another simple messaging test.
simpleMessagingTest3 = do
  putStrLn "Getting address 0..."
  address0 <- getSockAddr 6660
  case address0 of
    Just address0 -> do
      putStrLn "Getting address 1..."
      address1 <- getSockAddr 6661
      case address1 of
        Just address1 -> do
          putStrLn "Starting node 0..."
          node0 <- PN.start 0 (Just address0) BS.empty
          putStrLn "Starting node 1..."
          node1 <- PN.start 1 (Just address1) BS.empty
          putStrLn "Starting receiver process 0..."
          receiverPid0 <- P.spawnInit' (simpleMessageReceiver3 0 True) node0
          putStrLn "Starting receiver processs 1..."
          receiverPid1 <- P.spawnInit' (simpleMessageReceiver3 1 False) node1
          putStrLn "Starting the sender process..."
          P.spawnInit' (simpleMessageSender3 receiverPid0 receiverPid1 address0)
            node1
          PN.waitShutdown node0
          putStrLn "Node 0 has shut down"
          PN.waitShutdown node1
          putStrLn "Node 1 has shut down"
        Nothing -> putStrLn "Did not find address 1"
    Nothing -> putStrLn "Did not find address 0"

-- | The ring repeater process.
ringRepeater0 :: Int -> P.Process ()
ringRepeater0 count = do
  liftIO $ putStrLn "Getting process Id..."
  pid <- P.receive [\_ _ header payload ->
                      if header == encode ("otherProcess" :: T.Text)
                      then Just $ return $ (decode payload :: P.ProcessId)
                      else Nothing]
  liftIO $ putStrLn "Starting to receive messages..."
  loop pid count
  where loop pid count = do
          P.receive [\_ _ header payload ->
                        if header == encode ("textMessage" :: T.Text)
                        then Just $ do
                          liftIO $ printf "Got text: %s\n"
                            (decode payload :: T.Text)
                          P.send (P.ProcessDest pid) header payload
                        else Nothing]
          if count > 1
            then loop pid $ count - 1
            else do nid <- P.myNodeId
                    P.shutdown' nid
                    P.quit'

-- | The ring sender process.
ringSender0 :: NS.SockAddr -> P.ProcessId -> Int -> P.Process ()
ringSender0 address pid count = do
  liftIO $ putStrLn "Connecting to node 0..."
  P.connectRemote 0 address Nothing
  liftIO . printf "Listening for process %s termination...\n" $ show pid
  P.listenEnd $ P.ProcessDest pid
  myPid <- P.myProcessId
  let header = encode ("otherProcess" :: T.Text)
      payload = encode myPid
  P.send (P.ProcessDest pid) header payload
  forM ([0..count - 1] :: [Int]) $ \i -> do
    let header = encode ("textMessage" :: T.Text)
        payload = encode . T.pack $ printf "%d" i
    P.send (P.ProcessDest pid) header payload
    liftIO $ printf "Sent: %d\n" i
  loop
  where loop = do
          P.receive [\_ _ header payload ->
                        if header == encode ("textMessage" :: T.Text)
                        then Just $ do
                          liftIO $ printf "Got text back: %s\n"
                            (decode payload :: T.Text)
                        else if header ==
                                encode ("remoteDisconnected" :: T.Text) ||
                                header == encode ("genericQuit" :: T.Text)
                        then Just $ do
                          liftIO $ putStrLn "Received end"
                          nid <- P.myNodeId
                          P.shutdown' nid
                          P.quit'
                        else Just $ do
                          liftIO $ printf "Got message: %s\n"
                            (decode header :: T.Text)]
          loop

-- | A ring messaging test.
ringMessagingTest0 :: IO ()
ringMessagingTest0 = do
  putStrLn "Getting address 0..."
  address0 <- getSockAddr 6660
  case address0 of
    Just address0 -> do
      putStrLn "Getting address 1..."
      address1 <- getSockAddr 6661
      case address1 of
        Just address1 -> do
          putStrLn "Starting node 0..."
          node0 <- PN.start 0 (Just address0) BS.empty
          putStrLn "Starting node 1..."
          node1 <- PN.start 1 (Just address1) BS.empty
          repeaterPid <- P.spawnInit' (ringRepeater0 50) node0
          P.spawnInit' (ringSender0 address0 repeaterPid 100) node1
          PN.waitShutdown node0
          putStrLn "Node 0 has shut down"
          PN.waitShutdown node1
          putStrLn "Node 1 has shut down"
        Nothing -> putStrLn "Did not find address 1"
    Nothing -> putStrLn "Did not find address 0"

-- | Another ring repeater.
ringRepeater1 :: T.Text -> T.Text -> P.Process ()
ringRepeater1 myName nextName = do
  myPid <- P.myProcessId
  liftIO $ printf "Assigning \"%s\"...\n" myName
  P.assign (encode myName) $ P.ProcessDest myPid
  liftIO $ printf "Looking up \"%s\"...\n" nextName
  did <- P.lookup $ encode nextName
  handleIncoming did
  where handleIncoming did = do
          P.receive [\_ _ header payload ->
                       if header == encode ("textMessage" :: T.Text)
                       then Just $ do
                         liftIO $ printf "Got text: %s\n"
                           (decode payload :: T.Text)
                         P.send did header payload
                       else if header == encode ("exit" :: T.Text)
                       then Just $ do
                         liftIO $ printf "Got exit\n"
                         let header = encode ("exit" :: T.Text)
                             payload = BS.empty
                         P.send did header payload
                         P.quit'
                       else Just $ do
                         liftIO $ printf "Got message: %s\n"
                           (decode header :: T.Text)]
          handleIncoming did

-- | Another ring sender.
ringSender1 :: T.Text -> S.Seq T.Text -> S.Seq NS.SockAddr -> Int ->
               P.Process ()
ringSender1 myName names addresses count = do
  myPid <- P.myProcessId
  liftIO $ printf "Assigning \"%s\"...\n" myName
  P.assign (encode myName) $ P.ProcessDest myPid
  let pairs = S.zip [1..(fromIntegral $ S.length addresses)] addresses
  forM_ pairs $ \(index, address) -> do
    liftIO $ printf "Connecting to %d...\n" index
    P.connectRemote index address Nothing
  dids <- forM names $ \name -> do
    liftIO $ printf "Looking up \"%s\"...\n" name
    did <- P.lookup $ encode name
    liftIO $ printf "Listening for \"%s\" end...\n" name
    P.listenEnd did
    return did
  case S.viewl dids of
    nextDid :< _ -> do
      forM_ ([0..count - 1] :: S.Seq Int) $ \i -> do
        let header = encode ("textMessage" :: T.Text)
            payload = encode . T.pack $ printf "%d" i
        P.send nextDid header payload
        liftIO $ printf "Sent %d\n" i
      let header = encode ("exit" :: T.Text)
          payload = BS.empty
      P.send nextDid header payload
      liftIO $ printf "Sent exit\n"
      let nids = foldl' (\prev did ->
                           case P.nodeIdOfDestId did of
                             Just nid -> prev |> nid
                             Nothing -> prev) S.empty dids
      handleIncoming dids nids
    EmptyL -> liftIO $ printf "Unexpected empty did list\n"
  where handleIncoming dids nids = do
          dids' <-
            P.receive [\sid _ header payload ->
                         if header == encode ("textMessage" :: T.Text)
                         then Just $ do
                           liftIO $ printf "Got text back: %s\n"
                             (decode payload :: T.Text)
                           return dids
                         else if header == encode ("exit" :: T.Text)
                         then Just $ do
                           liftIO $ printf "Got exit back\n"
                           return dids
                         else if header == encode ("genericQuit" :: T.Text)
                         then Just $ do
                           case P.processIdOfSourceId sid of
                             Just pid -> do
                               liftIO . printf "Got quit for %s\n" $ show pid
                               let dids' =
                                     S.filter (\did -> did /= P.ProcessDest pid)
                                     dids
                               if S.null dids'
                                 then do
                                   forM_ nids $ \nid -> P.shutdown' nid
                                   myNid <- P.myNodeId
                                   P.shutdown' myNid
                                   P.quit'
                                 else return ()
                               return dids'
                             Nothing -> return dids
                         else Just $ do
                           liftIO $ printf "Got message: %s\n"
                             (decode header :: T.Text)
                           return dids]
          handleIncoming dids' nids

-- | Another ring messaging test.
ringMessagingTest1 :: IO ()
ringMessagingTest1 = do
  addresses <- getAddresses ([6660..6663] :: S.Seq Word16)
  case addresses of
    Just addresses@[address0, address1, address2, address3] -> do
      nodes@[node0, node1, node2, node3] <- startNodes addresses BS.empty
      P.spawnInit' (ringSender1 "main" ["repeater1", "repeater2", "repeater3"]
                    (S.drop 1 addresses) 100)
        node0
      P.spawnInit' (ringRepeater1 "repeater1" "repeater2") node1
      P.spawnInit' (ringRepeater1 "repeater2" "repeater3") node2
      P.spawnInit' (ringRepeater1 "repeater3" "main") node3
      waitNodes nodes
    _ -> putStrLn "Could not find addresses"
            
-- | Start nodes at addresses.
startNodes :: Foldable t => t NS.SockAddr -> BS.ByteString -> IO (S.Seq PN.Node)
startNodes addresses key = do
  (_, nodes) <- foldM startNode (0, S.empty) addresses
  return nodes
  where startNode (index, nodes) address = do
          printf "Starting node %d...\n" index
          node <- PN.start index (Just address) key
          return $ (index + 1, nodes |> node)

-- | Wait for nodes to shut down.
waitNodes :: Foldable t => t PN.Node -> IO ()
waitNodes nodes = (foldM waitNode 0 nodes) >> return ()
  where waitNode index node = do
          PN.waitShutdown node
          printf "Node %d has shut down\n" (index :: Integer)
          return $ index + 1

-- | Get socket address for localhost and port.
getSockAddr :: Word16 -> IO (Maybe NS.SockAddr)
getSockAddr port = do
  let hints = NS.defaultHints { NS.addrFlags = [NS.AI_NUMERICSERV],
                                NS.addrSocketType = NS.Stream }
  addresses <- NS.getAddrInfo (Just hints) (Just "127.0.0.1")
               (Just $ printf "%d" port)
  case addresses of
    address : _ -> return . Just $ NS.addrAddress address
    [] -> return Nothing

-- | Get addresses.
getAddresses :: Foldable t => t Word16 -> IO (Maybe (S.Seq NS.SockAddr))
getAddresses = foldM getAddress (Just S.empty)
  where getAddress Nothing _ = return Nothing
        getAddress (Just addresses) port = do
          address <- getSockAddr port
          case address of
            Just address -> return . Just $ addresses |> address
            Nothing -> return Nothing

-- | The entry point.
main :: IO ()
main = do
  --simpleMessagingTest0
  --simpleMessagingTest1
  --simpleMessagingTest2
  --simpleMessagingTest3
  --ringMessagingTest0
  ringMessagingTest1

-- | Decode data from a strict ByteString.
decode :: B.Binary a => BS.ByteString -> a
decode = B.decode . BSL.fromStrict

-- | Encode data to a strict ByteString
encode :: B.Binary a => a -> BS.ByteString
encode = BSL.toStrict . B.encode
