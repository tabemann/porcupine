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
import qualified Control.Concurrent.Porcupine.Utility as U
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

-- | Message text header
textHeader :: P.Header
textHeader = P.makeHeader ("text" :: T.Text)

-- | Normal quit header
normalQuitHeader :: P.Header
normalQuitHeader = P.makeHeader ("normalQuit" :: T.Text)

-- | A simple message receiver.
simpleMessageReceiver :: Integer -> Bool -> P.Process ()
simpleMessageReceiver index shutdownOnQuit = do
  liftIO $ putStrLn "Starting to receive messages..."
  loop
  where loop = do
          P.receive [\msg ->
                       if U.matchHeader msg textHeader
                       then case U.getPayload msg of
                              Right text ->
                                Just . liftIO $ printf "Received %s for %d\n"
                                (text :: T.Text) index
                       else Nothing,
                     \msg ->
                       if U.matchHeader msg normalQuitHeader
                       then Just $ do
                         liftIO $ putStrLn "Exiting receive process..."
                         if shutdownOnQuit
                           then P.shutdown' =<< P.myNodeId
                           else return ()
                         P.quit'
                       else Nothing]
          loop

-- | Yet another simple mssage sender.
simpleMessageSender :: P.ProcessId -> P.ProcessId -> NS.SockAddr ->
                        P.Process ()
simpleMessageSender pid0 pid1 address = do
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
  forM_ ([1..100] :: [Integer]) $ \n -> do
    liftIO . putStrLn . T.pack $ printf "Sending %d" n
    P.send (P.GroupDest gid) textHeader . T.pack $ printf "%d" n
  liftIO $ putStrLn "Sending message requesting quit..."
  P.send (P.GroupDest gid) normalQuitHeader BS.empty
  liftIO $ putStrLn "Waiting for termination..."
  replicateM 2 $ do
    P.receive [\msg ->
                 if U.isEnd msg
                 then Just . liftIO $ putStrLn "Received end"
                 else Nothing]
  liftIO $ putStrLn "Shutting down..."
  P.shutdown' =<< P.myNodeId

-- | Yet another simple messaging test.
simpleMessagingTest = do
  putStrLn "Getting address 0..."
  address0 <- getSockAddr 6660
  case address0 of
    Just address0 -> do
      putStrLn "Getting address 1..."
      address1 <- getSockAddr 6661
      case address1 of
        Just address1 -> do
          putStrLn "Starting node 0..."
          node0 <- PN.start 0 (Just address0) (P.makeKey BS.empty)
          putStrLn "Starting node 1..."
          node1 <- PN.start 1 (Just address1) (P.makeKey BS.empty)
          putStrLn "Starting receiver process 0..."
          receiverPid0 <- P.spawnInit' (simpleMessageReceiver 0 True) node0
          putStrLn "Starting receiver processs 1..."
          receiverPid1 <- P.spawnInit' (simpleMessageReceiver 1 False) node1
          putStrLn "Starting the sender process..."
          P.spawnInit' (simpleMessageSender receiverPid0 receiverPid1 address0)
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
main = simpleMessagingTest
