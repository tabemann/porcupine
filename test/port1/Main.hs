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

-- | Exit header
exitHeader :: P.Header
exitHeader = P.makeHeader ("exit" :: T.Text)

-- | Text header
textHeader :: P.Header
textHeader = P.makeHeader ("text" :: T.Text)

-- | Repeat incoming messages.
repeater :: NS.SockAddr -> P.Process ()
repeater sockAddr = do
  myDestId <- P.ProcessDest <$> P.myProcessId
  liftIO $ printf "Listening on port...\n"
  listener <- SP.listen sockAddr BS.empty [myDestId] [myDestId] [myDestId]
              [myDestId]
  repeaterLoop listener

-- | Repeater loop.
repeaterLoop :: SP.SocketListener -> P.Process ()
repeaterLoop listener = do
  P.receive [handleAccepted listener,
             handleRepeaterEnd,
             handleRepeaterTextMessage,
             handleRepeaterExit]
  repeaterLoop listener

-- | Handle repeater accepted message.
handleAccepted :: SP.SocketListener -> P.Message -> Maybe (P.Process ())
handleAccepted listener msg =
  case SP.accept listener msg of
    Just port ->
      Just . liftIO . printf "Accepted socket port for %s\n" $ show port
    Nothing -> Nothing

-- | Handle end messages for repeaters.
handleRepeaterEnd :: P.Message -> Maybe (P.Process ())
handleRepeaterEnd msg
  | U.isEnd msg =
    Just $ do
      liftIO . printf "Received end for %s\n" . show $ P.messageSourceId msg
      liftIO . printf "Shutting down for %s\n" . show $ P.messageSourceId msg
      P.shutdown' =<< P.myNodeId
      P.quit'
  | True = Nothing

-- | Handle text messages for repeaters.
handleRepeaterTextMessage :: P.Message -> Maybe (P.Process ())
handleRepeaterTextMessage msg
  | U.matchHeader msg textHeader =
    case U.tryDecodeMessage msg :: Either T.Text T.Text of
      Right text ->
        Just $ do
          liftIO $ printf "Received text for %s: %s\n"
            (show $ P.messageSourceId msg) text
          U.reply msg textHeader $ U.encode text
      Left _ -> Just $ return ()
  | True = Nothing

-- | Handle exit message for repeaters.
handleRepeaterExit :: P.Message -> Maybe (P.Process ())
handleRepeaterExit msg
  | U.matchHeader msg exitHeader =
    Just $ do
      liftIO . printf "Shutting down for %s\n" . show $ P.messageSourceId msg
      U.reply msg exitHeader BS.empty
      liftIO $ threadDelay 100000
      P.shutdown' =<< P.myNodeId
      P.quit'
  | True = Nothing

-- | Start send and receive processes.
sendReceive :: NS.SockAddr -> Integer -> P.Process ()
sendReceive sockAddr count = do
  receiverPid <- P.spawn' receiver
  liftIO $ printf "Connecting to port...\n"
  port <- SP.connect sockAddr BS.empty [P.ProcessDest receiverPid]
          [P.ProcessDest receiverPid]
  forM ([0..count - 1] :: [Integer]) $ \i -> do
    SP.send port textHeader . U.encode . T.pack $ printf "%d" i
    liftIO $ printf "Sending: %d\n" i
  SP.send port exitHeader BS.empty

-- | Receiver procoress
receiver :: P.Process ()
receiver = do
  P.receive [handleReceiverEnd, handleReceiverTextMessage, handleReceiverExit]
  receiver

-- | Handle end messages for receivers.
handleReceiverEnd :: P.Message -> Maybe (P.Process ())
handleReceiverEnd msg
  | U.isEnd msg =
    Just $ do
      liftIO . printf "Received end for %s\n" . show $ P.messageSourceId msg
      liftIO . printf "Shutting down for %s\n" . show $ P.messageSourceId msg
      P.shutdown' =<< P.myNodeId
      P.quit'
  | True = Nothing

-- | Handle text messages for receivers.
handleReceiverTextMessage :: P.Message -> Maybe (P.Process ())
handleReceiverTextMessage msg
  | U.matchHeader msg textHeader =
    case U.tryDecodeMessage msg :: Either T.Text T.Text of
      Right text ->
        Just $ do
          liftIO $ printf "Received text back for %s: %s\n"
            (show $ P.messageSourceId msg) text
      Left _ -> Just $ return ()
  | True = Nothing

-- | Handle exit message for receivers.
handleReceiverExit :: P.Message -> Maybe (P.Process ())
handleReceiverExit msg
  | U.matchHeader msg exitHeader =
    Just $ do
      liftIO . printf "Shutting down for %s\n" . show $ P.messageSourceId msg
      P.shutdown' =<< P.myNodeId
      P.quit'
  | True = Nothing

-- | A socket port ring messaging test.
portMessagingTest :: IO ()
portMessagingTest = do
  addresses <- getAddresses ([6660..6661] :: S.Seq Word16)
  case addresses of
    Just addresses@[address0, address1] -> do
      bindAddress <- getSockAddr 7770
      case bindAddress of
        Just bindAddress -> do
          nodes@[node0, node1] <- startNodes addresses BS.empty
          P.spawnInit' (repeater bindAddress) node1
          liftIO $ threadDelay 100000
          P.spawnInit' (sendReceive bindAddress 50) node0
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
