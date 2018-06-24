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

-- | Another ring repeater.
ringRepeater :: T.Text -> T.Text -> P.Process ()
ringRepeater myName nextName = do
  myPid <- P.myProcessId
  liftIO $ printf "Assigning \"%s\"...\n" myName
  P.assign (U.encode myName) $ P.ProcessDest myPid
  liftIO $ printf "Looking up \"%s\"...\n" nextName
  did <- P.lookup $ U.encode nextName
  handleIncoming did
  where handleIncoming did = do
          P.receive [\_ _ header payload ->
                       if header == U.encode ("textMessage" :: T.Text)
                       then Just $ do
                         liftIO $ printf "Got text: %s\n"
                           (U.decode payload :: T.Text)
                         P.send did header payload
                       else if header == U.encode ("exit" :: T.Text)
                       then Just $ do
                         liftIO $ printf "Got exit\n"
                         let header = U.encode ("exit" :: T.Text)
                             payload = BS.empty
                         P.send did header payload
                         P.quit'
                       else Just $ do
                         liftIO $ printf "Got message: %s\n"
                           (U.decode header :: T.Text)]
          handleIncoming did

-- | Another ring sender.
ringSender :: T.Text -> S.Seq T.Text -> S.Seq NS.SockAddr -> Int ->
               P.Process ()
ringSender myName names addresses count = do
  myPid <- P.myProcessId
  liftIO $ printf "Assigning \"%s\"...\n" myName
  P.assign (U.encode myName) $ P.ProcessDest myPid
  let pairs = S.zip [1..(fromIntegral $ S.length addresses)] addresses
  forM_ pairs $ \(index, address) -> do
    liftIO $ printf "Connecting to %d...\n" index
    P.connectRemote index address Nothing
  dids <- forM names $ \name -> do
    liftIO $ printf "Looking up \"%s\"...\n" name
    did <- P.lookup $ U.encode name
    liftIO $ printf "Listening for \"%s\" end...\n" name
    P.listenEnd did
    return did
  case S.viewl dids of
    nextDid :< _ -> do
      forM_ ([0..count - 1] :: S.Seq Int) $ \i -> do
        let header = U.encode ("textMessage" :: T.Text)
            payload = U.encode . T.pack $ printf "%d" i
        P.send nextDid header payload
        liftIO $ printf "Sent %d\n" i
      let header = U.encode ("exit" :: T.Text)
          payload = BS.empty
      P.send nextDid header payload
      liftIO $ printf "Sent exit\n"
      let nids = foldl' (\prev did ->
                           case U.nodeIdOfDestId did of
                             Just nid -> prev |> nid
                             Nothing -> prev) S.empty dids
      handleIncoming dids nids
    EmptyL -> liftIO $ printf "Unexpected empty did list\n"
  where handleIncoming dids nids = do
          dids' <-
            P.receive [\sid _ header payload ->
                         if header == U.encode ("textMessage" :: T.Text)
                         then Just $ do
                           liftIO $ printf "Got text back: %s\n"
                             (U.decode payload :: T.Text)
                           return dids
                         else if header == U.encode ("exit" :: T.Text)
                         then Just $ do
                           liftIO $ printf "Got exit back\n"
                           return dids
                         else if U.isEnd header
                         then Just $ do
                           case U.processIdOfSourceId sid of
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
                             (U.decode header :: T.Text)
                           return dids]
          handleIncoming dids' nids

-- | Another ring messaging test.
ringMessagingTest :: IO ()
ringMessagingTest = do
  addresses <- getAddresses ([6660..6663] :: S.Seq Word16)
  case addresses of
    Just addresses@[address0, address1, address2, address3] -> do
      nodes@[node0, node1, node2, node3] <- startNodes addresses BS.empty
      P.spawnInit' (ringSender "main" ["repeater1", "repeater2", "repeater3"]
                    (S.drop 1 addresses) 100)
        node0
      P.spawnInit' (ringRepeater "repeater1" "repeater2") node1
      P.spawnInit' (ringRepeater "repeater2" "repeater3") node2
      P.spawnInit' (ringRepeater "repeater3" "main") node3
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
main = ringMessagingTest
