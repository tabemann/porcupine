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

module Control.Concurrent.Porcupine.GenericServer

  (GenericServer,
   Handler (..),
   Match,
   Action,
   QuitOnEnd,
   start,
   stop,
   send,
   lookup,
   tryLookup,
   listenEnd)

where

import qualified Control.Concurrent.Porcupine.Process as P
import qualified Control.Concurrent.Porcupine.Utility as U
import qualified Data.Text as T
import qualified Data.Sequence as S
import qualified Data.Binary as B
import qualified Data.ByteString as BS
import Data.Sequence ((><))
import Data.Functor ((<$>))
import Control.Monad (mapM_,
                      (=<<))
import Text.Printf (printf)
import Prelude hiding (lookup)

-- | Generic server type
newtype GenericServer = GenericServer P.ProcessId
  deriving (Eq, Ord, B.Binary)

-- | Generic server type Show instance
instance Show GenericServer where
  show (GenericServer pid) = printf "genericServer:%s" $ show pid

-- | Generic server handler type
data Handler a = Handler (Match a) (Action a)

-- | Generic server handler match type
type Match a = a -> P.Message -> Bool

-- | Generic server handler action type
type Action a = a -> P.Message -> P.Process a

-- | Whether to quit on detecting process end
data QuitOnEnd = QuitOnEnd | QuitOnFail | NotQuitOnEnd

-- | Generic server state type
data State a = State { stName :: Maybe P.Name,
                       stQuitOnEnd :: QuitOnEnd,
                       stGroups :: S.Seq P.GroupId,
                       stListened :: S.Seq P.DestId,
                       stHandlers :: S.Seq (Handler a),
                       stState :: a }

-- | Start generic server.
start :: Maybe P.Name -> QuitOnEnd -> S.Seq P.GroupId -> S.Seq P.DestId ->
         S.Seq (Handler a) -> a -> P.Process GenericServer
start name quitOnEnd groups listened handlers state = do
  let state' = State { stName = name,
                       stQuitOnEnd = quitOnEnd,
                       stGroups = groups,
                       stListened = listened,
                       stHandlers = handlers,
                       stState = state }
  GenericServer <$> (P.spawn' $ startRun state')

-- | Stop generic server.
stop :: GenericServer -> P.Process ()
stop (GenericServer pid) =
  P.send (P.ProcessDest pid) genericServerExitHeader BS.empty

-- | Send a message to a generic server.
send :: B.Binary a => GenericServer -> P.Header -> a -> P.Process ()
send (GenericServer pid) header payload =
  P.send (P.ProcessDest pid) header payload

-- | Look up a generic server.
lookup :: P.Name -> P.Process (Maybe GenericServer)
lookup name = do
  did <- P.lookup name
  case did of
    P.ProcessDest pid -> return . Just $ GenericServer pid
    P.GroupDest _ -> return Nothing

-- | Try to look up a generic server.
tryLookup :: P.Name -> P.Process (Maybe GenericServer)
tryLookup name = do
  did <- P.tryLookup name
  case did of
    Just (P.ProcessDest pid) -> return . Just $ GenericServer pid
    _ -> return Nothing

-- | Listen for generic server termination.
listenEnd :: GenericServer -> P.Process ()
listenEnd (GenericServer pid) = P.listenEnd $ P.ProcessDest pid

-- | Start actually running the generic server.
startRun :: State a -> P.Process ()
startRun state = do
  case stName state of
    Just name -> do
      myPid <- P.myProcessId
      P.assign name $ P.ProcessDest myPid
    Nothing -> return ()
  mapM_ P.subscribe $ stGroups state
  mapM_ P.listenEnd $ stListened state
  run state

-- | Generic server exit header
genericServerExitHeader :: P.Header
genericServerExitHeader = P.makeHeader ("genericServerExit" :: T.Text)

-- | Run the generic server.
run :: State a -> P.Process ()
run state = do
  let handlers =
        case stQuitOnEnd state of
          QuitOnEnd -> [onNormalEndDoQuit, onFailDoQuit, doExit]
          QuitOnFail -> [onFailDoQuit, doExit]
          NotQuitOnEnd -> [doExit]
  stState <- P.receive $ handlers >< (handleCase <$> stHandlers state)
  run $ state { stState = stState }
  where onNormalEndDoQuit msg
          | U.isNormalEnd msg = Just $ quit state
          | True = Nothing
        onFailDoQuit msg
          | U.isFail msg = Just $ quit state
          | True = Nothing
        doExit msg
          | U.matchHeader msg genericServerExitHeader = Just $ quit state
          | True = Nothing
        handleCase (Handler match action) msg
          | match (stState state) msg = Just $ action (stState state) msg
          | True = Nothing

-- | Quit the generic server.
quit :: State a -> P.Process a
quit state = do
  mapM_ P.unlistenEnd $ stListened state
  mapM_ P.unsubscribe $ stGroups state
  case stName state of
    Just name -> do
      myPid <- P.myProcessId
      P.unassign name $ P.ProcessDest myPid
    Nothing -> return ()
  P.quit'
  return $ stState state
