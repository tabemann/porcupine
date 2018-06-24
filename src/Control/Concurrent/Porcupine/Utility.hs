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

module Control.Concurrent.Porcupine.Utility

  (isEnd,
   isNormalEnd,
   isFail,
   nodeIdOfSourceId,
   nodeIdOfDestId,
   processIdOfSourceId,
   quitHeader,
   endedHeader,
   killedHeader,
   diedHeader,
   remoteConnectFailedHeader,
   remoteDisconnectedHeader,
   encode,
   decode,
   tryDecode)

where

import qualified Control.Concurrent.Porcupine.Process as P
import qualified Data.Text as T
import qualified Data.Sequence as S
import qualified Data.Binary as B
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString as BS

-- | Quit header
quitHeader :: P.Header
quitHeader = encode ("quit" :: T.Text)

-- | Ended header
endedHeader :: P.Header
endedHeader = encode ("ended" :: T.Text)

-- | Killed header
killedHeader :: P.Header
killedHeader = encode ("killed" :: T.Text)

-- | Died header
diedHeader :: P.Header
diedHeader = encode ("died" :: T.Text)

-- | Remote connect failed header
remoteConnectFailedHeader :: P.Header
remoteConnectFailedHeader = encode ("remoteConnectFailed" :: T.Text)

-- | Remote disconnected header
remoteDisconnectedHeader :: P.Header
remoteDisconnectedHeader = encode ("remoteDisconnected" :: T.Text)

-- | Get whether a header indicates end.
isEnd :: P.Header -> Bool
isEnd header = isNormalEnd header || isFail header

-- | Get whether a header indicates normal end.
isNormalEnd :: P.Header -> Bool
isNormalEnd header = header == quitHeader ||
                     header == endedHeader ||
                     header == killedHeader

-- | Get whether a hehader indicates a failure.
isFail :: P.Header -> Bool
isFail header = header == diedHeader ||
                header == remoteConnectFailedHeader ||
                header == remoteDisconnectedHeader

-- | Get node Id of destination Id
nodeIdOfDestId :: P.DestId -> Maybe P.NodeId
nodeIdOfDestId (P.ProcessDest pid) = Just $ P.nodeIdOfProcessId pid
nodeIdOfDestId (P.GroupDest _) = Nothing

-- | Get node Id of source Id
nodeIdOfSourceId :: P.SourceId -> Maybe P.NodeId
nodeIdOfSourceId P.NoSource = Nothing
nodeIdOfSourceId (P.NormalSource pid) = Just $ P.nodeIdOfProcessId pid
nodeIdOfSourceId P.CauseSource{..} = Just $ P.nodeIdOfProcessId causeSourceId

-- | Process id of source id
processIdOfSourceId :: P.SourceId -> Maybe P.ProcessId
processIdOfSourceId P.NoSource = Nothing
processIdOfSourceId (P.NormalSource pid) = Just pid
processIdOfSourceId P.CauseSource{..} = Just causeSourceId

-- | Decode data from a strict ByteString.
decode :: B.Binary a => BS.ByteString -> a
decode = B.decode . BSL.fromStrict

-- | Encode data to a strict ByteString.
encode :: B.Binary a => a -> BS.ByteString
encode = BSL.toStrict . B.encode

-- | Attempt to decode data from a strict ByteString.
tryDecode :: B.Binary a => BS.ByteString -> Either T.Text a
tryDecode bytes =
  case B.decodeOrFail $ BSL.fromStrict bytes of
    Right (_, bytesConsumed, value)
      | bytesConsumed == fromIntegral (BS.length bytes) -> Right value
      | True -> Left "bytes are not all consumed"
    Left (_, _, message) -> Left $ T.pack message