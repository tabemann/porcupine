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

-- | The other process header.
otherProcessHeader :: P.Header
otherProcessHeader = P.makeHeader ("otherProcess" :: T.Text)

-- | Message text header
textHeader :: P.Header
textHeader = P.makeHeader ("text" :: T.Text)

-- | The ring repeater process.
ringRepeater :: Int -> P.Process ()
ringRepeater count = do
  liftIO $ putStrLn "Getting process Id..."
  pid <- P.receive
    [\msg ->
        if U.matchHeader msg otherProcessHeader
        then case U.tryDecodeMessage msg :: Either T.Text P.ProcessId of
               Right pid -> Just $ return pid
               Left errorText -> error $ T.unpack errorText
        else Nothing]
  liftIO $ putStrLn "Starting to receive messages..."
  loop pid count
  where loop pid count = do
          P.receive
            [\msg ->
                if U.matchHeader msg textHeader
                then case U.tryDecodeMessage msg :: Either T.Text T.Text of
                       Right text ->
                         Just $ do
                           liftIO $ printf "Got text: %s\n" text
                           U.reply msg textHeader $ P.messagePayload msg
                       Left errorText -> error $ T.unpack errorText
                else Nothing]
          if count > 1
            then loop pid $ count - 1
            else do nid <- P.myNodeId
                    P.shutdown' nid
                    P.quit'

-- | The ring sender process.
ringSender :: NS.SockAddr -> P.ProcessId -> Integer -> P.Process ()
ringSender address pid count = do
  liftIO $ putStrLn "Connecting to node 0..."
  P.connectRemote 0 address Nothing
  liftIO . printf "Listening for process %s termination...\n" $ show pid
  P.listenEnd $ P.ProcessDest pid
  myPid <- P.myProcessId
  P.send (P.ProcessDest pid) otherProcessHeader $ U.encode myPid
  uids <- forM ([0..count - 1] :: S.Seq Integer) $ \i -> do
    uid <- P.newUniqueId
    U.sendWithUniqueId (P.ProcessDest pid) uid textHeader . U.encode . T.pack $
      printf "%d" i
    liftIO $ printf "Sent: %d\n" i
    return uid
  loop uids
  where loop uids = do
          uids <-P.receive
            [\msg ->
                if U.matchHeader msg textHeader &&
                   any (U.matchUniqueId msg) uids
                then case U.tryDecodeMessage msg :: Either T.Text T.Text of
                       Right text ->
                         Just $ do
                           liftIO $ printf "Got text back: %s\n" text
                           return $ S.filter (not . U.matchUniqueId msg) uids
                       Left errorText -> error $ T.unpack errorText
                else if U.isEnd msg
                then Just $ do
                  liftIO $ putStrLn "Received end"
                  nid <- P.myNodeId
                  P.shutdown' nid
                  P.quit'
                  return uids
                else Just $ return uids]
          loop uids

-- | A ring messaging test.
ringMessagingTest :: IO ()
ringMessagingTest = do
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
          repeaterPid <- P.spawnInit' (ringRepeater 50) node0
          P.spawnInit' (ringSender address0 repeaterPid 100) node1
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
main = ringMessagingTest
