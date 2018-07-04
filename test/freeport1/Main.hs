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
import qualified Control.Concurrent.Porcupine.FreeSocketPort as FP
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
exitHeader = U.encode ("exit" :: T.Text)

-- | Text header
textHeader :: P.Header
textHeader = U.encode ("text" :: T.Text)

-- | Repeater group name
repeaterGroupName :: P.Name
repeaterGroupName = U.encode ("repeaterGroup" :: T.Text)

-- | Repeater
repeater :: P.Process ()
repeater = do
  handleMessages
  where handleMessages = forever $ P.receive [handleText, handleExit]
        handleText msg
          | U.matchHeader msg textHeader =
            Just $ do
              case U.tryDecodeMessage msg :: Either T.Text T.Text of
                Right text -> do
                  case U.tryDecodeProxySourceId msg of
                    Right (Just sid) -> do
                      liftIO . printf "Received \"%s\" from %s\n" text $
                        show sid
                      U.reply msg textHeader $ U.encode text
                    _ -> return ()
                _ -> return ()
          | True = Nothing
        handleExit msg
          | U.matchHeader msg exitHeader =
            Just $ do
              U.reply msg exitHeader BS.empty
              liftIO . printf "Quitting %s\n" . show =<< P.myProcessId
              P.quit'
          | True = Nothing

-- | Start repeaters
startRepeaters :: Integer -> P.Process ()
startRepeaters count = do
  gid <- P.newGroupId
  myPid <- P.myProcessId
  pids <- fmap S.fromList . forM ([0..count - 1] :: [Integer]) $ \i -> do
    pid <- P.spawnListenEnd' repeater . S.singleton $ P.ProcessDest myPid
    P.subscribeAsProxy gid pid
    return pid
  P.assign repeaterGroupName $ P.GroupDest gid
  handleMessages pids
    where handleMessages pids = handleMessages =<< P.receive [handleEnd pids]
          handleEnd pids msg
            | any (U.isEndForProcessId msg) pids =
              Just $ do
                case U.processIdOfSourceId $ P.messageSourceId msg of
                  Just pid ->
                    let pids' = S.filter (\pid' -> pid /= pid') pids
                    in if S.length pids' > 0
                       then do
                         liftIO $ threadDelay 10000
                         nid <- P.myNodeId
                         liftIO . printf "Shutting down %s\n" $ show nid
                         P.shutdown' nid
                         P.quit'
                         return pids'
                       else return pids'
                  Nothing -> return pids
            | True = Nothing

-- | Listener
startListener :: NS.SockAddr -> P.Process ()
startListener address = do
  myPid <- P.myProcessId
  listener <- FP.listen address BS.empty [P.ProcessDest myPid] [] []
  handleMessages listener
  where handleMessages listener = forever $ P.receive [handleAccept listener]
        handleAccept listener msg =
          case FP.accept listener msg of
            Just port -> Just . liftIO . printf "Accepted %s\n" $ show port
            Nothing -> Nothing

-- | Send and receive messages
sendReceive :: NS.SockAddr -> Integer -> Integer -> P.Process ()
sendReceive address count remoteCount = do
  myPid <- P.myProcessId
  port <- FP.connect address BS.empty [P.ProcessDest myPid]
  liftIO $ printf "Connecting to port\n"
  remoteDid <- FP.lookupRemote port repeaterGroupName 2000000
  case remoteDid of
    Right remoteDid -> do
      liftIO $ printf "Looked up repeater\n"
      forM_ ([0..count - 1] :: [Integer]) $ \i -> do
        FP.send port remoteDid textHeader . U.encode . T.pack $ printf "%d" i
        liftIO . printf  "Sent \"%d\" to %s\n" i $ show port
      FP.send port remoteDid exitHeader BS.empty
      handleMessages port 0
    Left _ -> do
      liftIO $ printf "Failed to look up name!\n"
      nid <- P.myNodeId
      P.shutdown' nid
      P.quit'
  where handleMessages port exitCount = do
          exitCount <- P.receive [handleText exitCount, handleExit exitCount,
                                  handleEnd port exitCount]
          handleMessages port exitCount
        handleText exitCount msg
          | U.matchHeader msg textHeader =
            Just $ do
              case U.tryDecodeMessage msg :: Either T.Text T.Text of
                Right text -> do
                  case U.tryDecodeProxySourceId msg of
                    Right (Just sid) -> do
                      liftIO . printf "Received \"%s\" back from %s\n" text $
                        show sid
                      U.reply msg textHeader $ U.encode text
                      return exitCount
                    _ -> return exitCount
                _ -> return exitCount
          | True = Nothing
        handleExit exitCount msg
          | U.matchHeader msg exitHeader =
            Just $ do
              liftIO . printf "Got exit from %s\n" . show $
                P.messageSourceId msg
              let exitCount' = exitCount + 1
              if exitCount' >= remoteCount
                then do
                  nid <- P.myNodeId
                  liftIO . printf "Shutting down %s\n" $ show nid
                  P.shutdown' nid
                  P.quit'
                  return exitCount'
                else return exitCount'
          | True = Nothing
        handleEnd port exitCount msg
          | FP.isEnd port msg =
            Just $ do
              liftIO . printf "Received end for %s\n" $ show port
              return exitCount
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
          P.spawnInit' (startListener bindAddress) node1
          liftIO $ threadDelay 100000
          P.spawnInit' (sendReceive bindAddress 50 3) node0
          liftIO $ threadDelay 500000
          P.spawnInit' (startRepeaters 3) node1
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
