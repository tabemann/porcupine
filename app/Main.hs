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
import qualified Data.ByteString.Lazy as BS
import qualified Network.Socket as NS
import qualified Data.HashMap.Lazy as M
import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad (forM,
                      forM_,
                      replicateM,
                      (=<<))
import Data.Functor ((<$>),
                     fmap)
import Data.Word (Word16)
import Text.Printf (printf)
import Prelude hiding (putStrLn)

-- | A simple message receiver.
simpleMessageReceiver :: P.Process ()
simpleMessageReceiver = do
  liftIO $ putStrLn "Starting to receive messages..."
  loop
  where loop = do
          P.receive [\sid did header payload ->
                       if (B.decode header :: T.Text) == "messageText"
                       then Just . liftIO . putStrLn . T.pack $
                            printf "Received %s" (B.decode payload :: T.Text)
                       else Nothing,
                     \sid did header payload ->
                       if (B.decode header :: T.Text) == "normalQuit"
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
    P.send (P.ProcessDest pid) (B.encode ("messageText" :: T.Text))
      (B.encode . T.pack $ printf "%d" n)
  liftIO $ putStrLn "Sending message requesting quit..."
  P.send (P.ProcessDest pid) (B.encode ("normalQuit" :: T.Text)) BS.empty
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
    P.send (P.ProcessDest pid) (B.encode ("messageText" :: T.Text))
      (B.encode . T.pack $ printf "%d" n)
  liftIO $ putStrLn "Sending message requesting quit..."
  P.send (P.ProcessDest pid) (B.encode ("normalQuit" :: T.Text)) BS.empty
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
    P.send (P.GroupDest gid) (B.encode ("messageText" :: T.Text))
      (B.encode . T.pack $ printf "%d" n)
  liftIO $ putStrLn "Sending message requesting quit..."
  P.send (P.GroupDest gid) (B.encode ("normalQuit" :: T.Text)) BS.empty
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
                       if (B.decode header :: T.Text) == "messageText"
                       then do
                         Just . liftIO $ printf "Received %s for %d\n"
                           (B.decode payload :: T.Text) index
                       else Nothing,
                     \sid did header payload ->
                       if (B.decode header :: T.Text) == "normalQuit"
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
    P.send (P.GroupDest gid) (B.encode ("messageText" :: T.Text))
      (B.encode . T.pack $ printf "%d" n)
  liftIO $ putStrLn "Sending message requesting quit..."
  P.send (P.GroupDest gid) (B.encode ("normalQuit" :: T.Text)) BS.empty
  liftIO $ putStrLn "Waiting for termination..."
  replicateM 2 $ do
    P.receive [\sid did header payload ->
                 if header == B.encode ("remoteDisconnected" :: T.Text) ||
                    header == B.encode ("genericQuit" :: T.Text)
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
  
-- | The entry point.
main :: IO ()
main = do
  --simpleMessagingTest0
  --simpleMessagingTest1
  --simpleMessagingTest2
  simpleMessagingTest3
