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
import qualified Control.Concurrent.Porcupine.SocketPort as SP
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

-- | Receiver state type
data ReceiverState = ReceiverState { recvPortEndCount :: Integer,
                                     recvListenerEnded :: Bool,
                                     recvListener :: SP.SocketListener,
                                     recvIncoming :: S.Seq SP.SocketPort }

-- | Message text header
textHeader :: P.Header
textHeader = P.makeHeader ("text" :: T.Text)

-- | A socket port receiver
receiver :: NS.SockAddr -> Integer -> P.Process ()
receiver sockAddr count = do
  myDestId <- P.ProcessDest <$> P.myProcessId
  liftIO $ printf "Starting listening...\n"
  listener <- SP.listen sockAddr BS.empty [myDestId] [myDestId] [myDestId]
              [myDestId]
  liftIO $ printf "Listener has been started...\n";
  receiverLoop $ ReceiverState { recvPortEndCount = 0,
                                 recvListenerEnded = False,
                                 recvListener = listener,
                                 recvIncoming = S.empty }
  where receiverLoop state = do
          state <- P.receive [handleAccepted state,
                              handleText state,
                              handlePortEnd state,
                              handleListenerEnd state]
          if recvPortEndCount state >= count || recvListenerEnded state
            then do
              liftIO $ printf "Shutting down node...\n"
              myNid <- P.myNodeId
              P.shutdown' myNid
              P.quit'
            else receiverLoop state
        handleAccepted state msg =
          case SP.accept (recvListener state) msg of
            Just port ->
              Just $ do
                liftIO $ printf "Accepted incoming port: %s\n" $ show port
                return $ state { recvIncoming = recvIncoming state |> port }
            Nothing -> Nothing
        handleText state msg
          | U.matchHeader msg textHeader =
            case U.tryDecodeMessage msg :: Either T.Text T.Text of
              Right text ->
                Just $ do
                  liftIO $ printf "Got message from %s: %s\n"
                    (show $ P.messageSourceId msg) text
                  return state
              Left _ -> Just $ return state
          | True = Nothing
        handlePortEnd state msg
          | any (\port -> SP.isEnd port msg) $ recvIncoming state =
            Just $ do
              liftIO $ printf "Got port end from %s\n" .
                show $ P.messageSourceId msg
              return $ state { recvPortEndCount = recvPortEndCount state + 1 }
          | True = Nothing
        handleListenerEnd state msg
          | SP.listenerIsEnd (recvListener state) msg =
            Just $ do
              liftIO $ printf "Listener ended\n"
              return $ state { recvListenerEnded = True }
          | True = Nothing

-- | Open a socket port and send messages to it
sender :: NS.SockAddr -> Integer -> P.Process ()
sender sockAddr count = do
  myPid <- P.myProcessId
  monitorPid <- P.spawn' monitor
  liftIO $ printf "Connecting to port...\n"
  port <- SP.connect sockAddr BS.empty [P.ProcessDest myPid]
    [P.ProcessDest monitorPid]
  liftIO $ printf "Socket port has been started...\n"
  forM_ ([0..count - 1] :: [Integer]) $ \index -> do
    SP.send port textHeader . U.encode . T.pack $ printf "%d" index
    liftIO $ printf "Sending: %d\n" index
  liftIO $ printf "Shutting down socket port...\n"
  SP.disconnect port
  liftIO $ printf "Shutting down node...\n"
  myNid <- P.myNodeId
  P.shutdown' myNid
  P.quit'

-- | Monitor for process exit
monitor :: P.Process ()
monitor =
  P.receive [\msg ->
               if U.isEnd msg
               then Just $ do
                 liftIO . printf "Process exited: %s" .
                   show $ P.messageSourceId msg
               else Nothing]

-- | A socket port ring messaging test.
portMessagingTest :: IO ()
portMessagingTest = do
  addresses <- getAddresses ([6660..6663] :: S.Seq Word16)
  case addresses of
    Just addresses -> do
      bindAddress <- getSockAddr 7770
      case bindAddress of
        Just bindAddress -> do
          nodes@[node0, node1, node2, node3] <- startNodes addresses BS.empty
          P.spawnInit' (receiver bindAddress 3) node0
          liftIO $ threadDelay 100000
          P.spawnInit' (sender bindAddress 50) node1
          P.spawnInit' (sender bindAddress 50) node2
          P.spawnInit' (sender bindAddress 50) node3
          waitNodes nodes
        Nothing -> putStrLn "Could not find address"
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
main = portMessagingTest
