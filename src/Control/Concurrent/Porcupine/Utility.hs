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

  (matchHeader,
   matchProcessId,
   matchHeaderAndProcessId,
   matchUniqueId,
   matchHeaderAndUniqueId,
   excludeHeader,
   excludeProcessId,
   excludeHeaderAndProcessId,
   excludeUniqueId,
   excludeHeaderAndUniqueId,
   isEnd,
   isNormalEnd,
   isFail,
   isEndForProcessId,
   isNormalEndForProcessId,
   isFailForProcessId,
   getAnnotation,
   tryDecodeMessage,
   tryDecodeAnnotation,
   tryDecodeUniqueId,
   tryDecodeProxySourceId,
   tryDecodeProxyDestId,
   processIdOfMessage,
   sendWithUniqueId,
   sendWithUniqueIdAsProxy,
   reply,
   nodeIdOfSourceId,
   nodeIdOfDestId,
   processIdOfSourceId,
   uniqueIdTag,
   proxySourceIdTag,
   proxyDestIdTag,
   quitHeader,
   endedHeader,
   killedHeader,
   diedHeader,
   assignedHeader,
   unassignedHeader,
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
import Data.Binary.Get (Get)
import Data.Binary.Put (Put)
import Data.Hashable (Hashable (..))
import Data.Foldable (Foldable (..))
import GHC.Generics (Generic)
import Data.Sequence ((><))
import Control.Monad.Fail (fail)
import Text.Printf (printf)
import Prelude hiding (fail)

-- | Unique Id tag
uniqueIdTag :: P.AnnotationTag
uniqueIdTag = encode ("uniqueId" :: T.Text)

-- | Proxy source Id tag
proxySourceIdTag :: P.AnnotationTag
proxySourceIdTag = encode ("proxySourceId" :: T.Text)

-- | Proxy destination Id tag
proxyDestIdTag :: P.AnnotationTag
proxyDestIdTag = encode ("proxyDestId" :: T.Text)

-- | Quit header
quitHeader :: P.Header
quitHeader = P.makeHeader ("quit" :: T.Text)

-- | Ended header
endedHeader :: P.Header
endedHeader = P.makeHeader ("ended" :: T.Text)

-- | Killed header
killedHeader :: P.Header
killedHeader = P.makeHeader ("killed" :: T.Text)

-- | Died header
diedHeader :: P.Header
diedHeader = P.makeHeader ("died" :: T.Text)

-- | Remote connect failed header
remoteConnectFailedHeader :: P.Header
remoteConnectFailedHeader = P.makeHeader ("remoteConnectFailed" :: T.Text)

-- | Remote disconnected header
remoteDisconnectedHeader :: P.Header
remoteDisconnectedHeader = P.makeHeader ("remoteDisconnected" :: T.Text)

-- | Assigned header.
assignedHeader :: P.Header
assignedHeader = P.makeHeader ("assigned" :: T.Text)

-- | Unassigned header.
unassignedHeader :: P.Header
unassignedHeader = P.makeHeader ("unassigned" :: T.Text)

-- | Match a message header.
matchHeader :: P.Message -> P.Header -> Bool
matchHeader message header = P.messageHeader message == header

-- | Match a message source process Id.
matchProcessId :: P.Message -> P.ProcessId -> Bool
matchProcessId message pid =
  processIdOfSourceId (P.messageSourceId message) == Just pid

-- | Match a message header and source process Id.
matchHeaderAndProcessId :: P.Message -> P.Header -> P.ProcessId -> Bool
matchHeaderAndProcessId message header pid =
  matchHeader message header && matchProcessId message pid

-- | Match a unique Id.
matchUniqueId :: P.Message -> P.UniqueId -> Bool
matchUniqueId message uid =
  case tryDecodeUniqueId message of
    Right (Just uid') -> uid == uid'
    _ -> False

-- | Match a message header and a unique Id.
matchHeaderAndUniqueId :: P.Message -> P.Header -> P.UniqueId -> Bool
matchHeaderAndUniqueId message header uid
  | matchHeader message header = matchUniqueId message uid
  | True = False

-- | Exclude a message header.
excludeHeader :: P.Message -> P.Header -> Bool
excludeHeader message header = P.messageHeader message /= header

-- | Exclude a message source process Id.
excludeProcessId :: P.Message -> P.ProcessId -> Bool
excludeProcessId message pid =
  processIdOfSourceId (P.messageSourceId message) /= Just pid

-- | Exclude a message header and source process Id.
excludeHeaderAndProcessId :: P.Message -> P.Header -> P.ProcessId -> Bool
excludeHeaderAndProcessId message header pid =
  excludeHeader message header && excludeProcessId message pid

-- | Exclude a unique Id.
excludeUniqueId :: P.Message -> P.UniqueId -> Bool
excludeUniqueId message uid =
  case tryDecodeUniqueId message of
    Right (Just uid') -> uid /= uid'
    _ -> True

-- | Exclude a message header and a unique Id.
excludeHeaderAndUniqueId :: P.Message -> P.Header -> P.UniqueId -> Bool
excludeHeaderAndUniqueId message header uid
  | matchHeader message header = not $ matchUniqueId message uid
  | True = True

-- | Get whether a message indicates end.
isEnd :: P.Message -> Bool
isEnd message = isNormalEnd message || isFail message

-- | Get whether a message indicates normal end.
isNormalEnd :: P.Message -> Bool
isNormalEnd message = matchHeader message quitHeader ||
                      matchHeader message endedHeader ||
                      matchHeader message killedHeader

-- | Get whether a hehader indicates a failure.
isFail :: P.Message -> Bool
isFail message = matchHeader message diedHeader ||
                 matchHeader message remoteConnectFailedHeader ||
                 matchHeader message remoteDisconnectedHeader

-- | Get whether a message indicates end for a particular process Id.
isEndForProcessId :: P.Message -> P.ProcessId -> Bool
isEndForProcessId message pid = (isNormalEnd message || isFail message) &&
                                matchProcessId message pid

-- | Get whether a message indicates normal end for a particular process Id.
isNormalEndForProcessId :: P.Message -> P.ProcessId -> Bool
isNormalEndForProcessId message pid = (matchHeader message quitHeader ||
                                       matchHeader message endedHeader ||
                                       matchHeader message killedHeader) &&
                                      matchProcessId message pid

-- | Get whether a hehader indicates a failure for a particular process Id.
isFailForProcessId :: P.Message -> P.ProcessId -> Bool
isFailForProcessId message pid = (matchHeader message diedHeader ||
                                  matchHeader message
                                   remoteConnectFailedHeader ||
                                  matchHeader message
                                   remoteDisconnectedHeader) &&
                                 matchProcessId message pid

-- | Get an annotation for a message.
getAnnotation :: P.Message -> P.AnnotationTag -> Maybe P.AnnotationValue
getAnnotation message tag =
  case S.findIndexL (\(P.Annotation tag' _) -> tag == tag') $
       P.messageAnnotations message of
    Just index ->
      case S.lookup index $ P.messageAnnotations message of
        Just (P.Annotation _ value) -> Just value
        Nothing -> error "impossible"
    Nothing -> Nothing

-- | Try to decode a message payload.
tryDecodeMessage :: B.Binary a => P.Message -> Either T.Text a
tryDecodeMessage = tryDecode . P.messagePayload

-- | Try to decode a message annotation.
tryDecodeAnnotation :: B.Binary a => P.Message -> P.AnnotationTag ->
                       Either T.Text (Maybe a)
tryDecodeAnnotation message tag =
  case getAnnotation message tag of
    Just value ->
      case tryDecode value of
        Right value -> Right $ Just value
        Left errorText -> Left errorText
    Nothing -> Right Nothing

-- | Try to decode a message unique Id.
tryDecodeUniqueId :: P.Message -> Either T.Text (Maybe P.UniqueId)
tryDecodeUniqueId = (flip tryDecodeAnnotation) uniqueIdTag

-- | Try to decode a message proxy source Id.
tryDecodeProxySourceId :: P.Message -> Either T.Text (Maybe P.SourceId)
tryDecodeProxySourceId = (flip tryDecodeAnnotation) proxySourceIdTag

-- | Try to decode a message proxy destination Id.
tryDecodeProxyDestId :: P.Message -> Either T.Text (Maybe P.DestId)
tryDecodeProxyDestId = (flip tryDecodeAnnotation) proxyDestIdTag

-- | Get the process Id of a message.
processIdOfMessage = processIdOfSourceId . P.messageSourceId

-- | Send a message with a unique Id.
sendWithUniqueId :: P.DestId -> P.UniqueId -> P.Header -> P.Payload ->
                    P.Process ()
sendWithUniqueId did uid header payload =
  P.sendAnnotated did header payload [P.Annotation uniqueIdTag $ encode uid]

-- | Send a message as a proxy with a unique Id.
sendWithUniqueIdAsProxy :: P.DestId -> P.SourceId -> P.UniqueId -> P.Header ->
                           P.Payload -> P.Process ()
sendWithUniqueIdAsProxy did sid uid header payload =
  P.sendAnnotatedAsProxy did sid header payload
    [P.Annotation uniqueIdTag $ encode uid]

-- | Reply to a message.
reply :: P.Message -> P.Header -> P.Payload -> P.Process ()
reply msg header payload = do
  let uniqueIdAnnotation =
        case tryDecodeUniqueId msg of
          Right (Just uid) -> [P.Annotation uniqueIdTag $ encode uid]
          _ -> []
      proxyDestIdAnnotation =
        case tryDecodeProxySourceId msg of
          Right (Just sid) ->
            case processIdOfSourceId sid of
              Just pid ->
                [P.Annotation proxyDestIdTag . encode $ P.ProcessDest pid]
              Nothing -> []
          _ -> []
      annotations = uniqueIdAnnotation >< proxyDestIdAnnotation
  case processIdOfMessage msg of
    Just pid -> P.sendAnnotated (P.ProcessDest pid) header payload annotations
    Nothing -> return ()

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
