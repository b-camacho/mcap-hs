{-# LANGUAGE ScopedTypeVariables, NamedFieldPuns, BlockArguments #-}

module Main where

import Data.Word
import System.IO.Unsafe
import Data.Binary.Put
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import Data.ByteString.Lazy (fromChunks, ByteString)
import Data.Char
import Debug.Trace
import Text.Printf

import Data.Binary.Get
import Data.Map (Map)
import qualified Data.Map as Map
import Control.Monad (unless)
import Control.Monad.State (evalState, get, put, runStateT, StateT)
import Data.Binary.Builder (toLazyByteString)

import Data.Time.Clock (UTCTime, NominalDiffTime, addUTCTime, diffTimeToPicoseconds)
import Data.Time.Clock.POSIX (posixSecondsToUTCTime)

import Codec.Compression.Zstd (decompress, Decompress(..))
import Data.Sequence (chunksOf)

-- parsers for primitive types
-- ik this should be Text but we're taking things one at a time
getString :: Get String
getString = do
  strLen <- getInt32le            -- Read the length as a Word32 (little-endian)
  rawString <- getByteString $ fromIntegral strLen -- Read the specified number of bytes
  let unpacked = BS.unpack rawString
    in
    pure $ map (chr . fromEnum) unpacked


getTimestamp :: Get UTCTime
getTimestamp = do
    nanoseconds <- getWord64le
    let seconds = fromIntegral nanoseconds / 1e9 :: NominalDiffTime
        epoch = posixSecondsToUTCTime 0 -- Unix epoch start
    pure $ addUTCTime seconds epoch


getBytes' :: Get BS.ByteString
getBytes' = do
  bytesLen <- getInt32le
  getByteString $ fromIntegral bytesLen


data Header = Header { profile :: String, library :: String }
    deriving (Read, Show, Eq);

data Schema = Schema { sid :: Word16, name :: String, encoding :: String, s_data :: BS.ByteString }
    deriving (Read, Show, Eq);
    
data Channel = Channel { cid :: Word16, schema_id :: Word16, topic:: String, message_encoding :: String, rest :: BS.ByteString} -- no metadata for now, brain too small
    deriving (Read, Show, Eq);

data Message = Message { channel_id :: Word16, log_time :: UTCTime, publish_time :: UTCTime, sequence :: Word32, c_data :: BS.ByteString }
    deriving (Read, Show, Eq);

data MessageIndex = MessageIndex { mi_channel_id :: Word16, mi_data :: BS.ByteString } -- TODO: read the Array<Tuple<Timestamp, uint64>> index
    deriving (Read, Show, Eq);

data Footer = Footer
    { summaryStart        :: Word64
    , summaryOffsetStart  :: Word64
    , summaryCrc          :: Word32
    } deriving (Read, Show, Eq)

data Chunk = Chunk
    { messageStartTime    :: UTCTime
    , messageEndTime      :: UTCTime
    , uncompressedSize    :: Word64
    , uncompressedCrc     :: Word32
    , compression         :: String
    , compressedData      :: BL.ByteString
    } deriving (Read, Show, Eq)

data DataEnd = DataEnd {
    de_crc :: Word32
} deriving (Read, Show, Eq)

data Statistics = Statistics
    { messageCount             :: Word64
    , schemaCount              :: Word16
    , channelCount             :: Word32
    , attachmentCount          :: Word32
    , metadataCount            :: Word32
    , chunkCount               :: Word32
    , s_messageStartTime       :: UTCTime
    , s_messageEndTime         :: UTCTime
    , channelMessageCounts     :: Map.Map Word16 Word64
    } deriving (Read, Show, Eq)



data Record = 
        RecordHeader Header |
        RecordFooter Footer |
        RecordChannel Channel |
        RecordSchema Schema |
        RecordMessage Message |
        RecordMessageIndex MessageIndex |
        RecordChunk Chunk |
        RecordStatistics Statistics |
        RecordDataEnd DataEnd |
        RecordUnknown BS.ByteString 
    deriving (Read, Show, Eq);

fromS = map (fromInteger . toInteger . ord)
startsWith :: Eq a => [a] -> [a] -> Bool
startsWith as bs =
    let pre = take (length bs) as in pre == bs

isMcap :: BS.ByteString -> Bool
isMcap bs =
    let magic_exp :: [Word8] = 0x89 : fromS "MCAP" ++ [0x30] ++ fromS "\r\n"  -- BS.pack [ 0x89 ] --, 'M', 'C', 'A', 'P', 0x30, '\r', '\n' ]
        magic_got =  BS.unpack bs
        magic_show :: [String] = map (printf "%x" . toInteger) magic_exp
    in
    startsWith magic_got magic_exp


-- this function composition style is horribly unreadable
-- but kinda fun also!
getHeader :: Get Header
getHeader = Header <$> getString <*> getString

getSchema :: Get Schema
getSchema = Schema <$> getWord16le <*> getString <*> getString <*> getBytes'

getChannel :: Get Channel
getChannel = Channel <$> getWord16le <*> getWord16le <*> getString <*> getString <*> getBytes'

getMessage :: Int -> Get Message
getMessage size = do
    channel_id <- getWord16le
    sequence <- getWord32le
    log_time <- getTimestamp
    pub_time <- getTimestamp
    _data <- getByteString (size - 2 - 4 - 8 - 8)
    pure $ Message channel_id log_time pub_time sequence _data

getMessageIndex :: Get MessageIndex
getMessageIndex = do
    channel_id <- getWord16le
    size <- getInt32le
    _data <- getByteString $ fromIntegral size
    pure $ MessageIndex channel_id _data


getChunk :: Get Chunk
getChunk = do
    message_start_time <- getTimestamp
    message_end_time <- getTimestamp
    uncompressed_size <- getWord64le
    uncompressed_crc <- getWord32le
    compression <- getString
    compressed_size <- getInt64le
    compressed_data <- getLazyByteString compressed_size
    pure $ Chunk message_start_time message_end_time uncompressed_size uncompressed_crc compression compressed_data


getFooter :: Get Footer
getFooter = do
    summary_start <- getWord64le       
    summary_offset_start <- getWord64le
    summary_crc <- getWord32le         
    pure $ Footer summary_start summary_offset_start summary_crc

getStatistics :: Get Statistics
getStatistics = do
    message_count <- getWord64le
    schema_count <- getWord16le
    channel_count <- getWord32le
    attachment_count <- getWord32le
    metadata_count <- getWord32le
    chunk_count <- getWord32le
    message_start_time <- getTimestamp
    message_end_time <- getTimestamp
    mapSize <- getWord32le
    channel_message_counts <- getChannelMessageCounts mapSize
    pure $ Statistics message_count schema_count channel_count attachment_count metadata_count chunk_count message_start_time message_end_time channel_message_counts

getChannelMessageCounts :: Word32 -> Get (Map.Map Word16 Word64)
getChannelMessageCounts size = if size == 0
                               then pure Map.empty
                               else getMapEntries (fromIntegral size)

getMapEntries :: Int -> Get (Map.Map Word16 Word64)
getMapEntries 0 = pure Map.empty
getMapEntries n = do
    channel_id <- getWord16le
    message_count <- getWord64le
    rest <- getMapEntries (n - 2 - 8)
    pure $ Map.insert channel_id message_count rest

getDataEnd :: Get DataEnd
getDataEnd = DataEnd <$> getWord32le
-- decompress works on strict chunks
-- but Data.Binary.Get works on lazy chunks
-- so there's weird and probably slow convertions here
decompressChunk :: Chunk -> [Record]
decompressChunk c = case decompress $ BS.concat $ BL.toChunks $ compressedData c 
    of {
        Skip -> [];
        Error e -> error e;
        Decompress bs -> runGet getRecords $ BL.fromChunks [bs]
    }
    

decompressRecords :: [Record] -> [Record]
decompressRecords [] = []
decompressRecords (r:rs) = case r of 
    RecordChunk c -> decompressChunk c ++ decompressRecords rs
    nonChunk -> nonChunk : decompressRecords rs
    

getRecord :: Get Record
getRecord = do
    opcode <- getWord8
    if opcode == 0x89
        then
            trace "magic opcode" RecordUnknown <$> getByteString 7;
        else
            do
            sizeBytes <- getWord64le
            let
                size = fromIntegral sizeBytes
                in
                case trace ("opcode: " ++ show opcode ++ " size " ++ show size) opcode of {
                    0x01 -> RecordHeader <$> getHeader;
                    0x02 -> RecordFooter <$> getFooter;
                    0x03 -> RecordSchema <$> getSchema;
                    0x04 -> RecordChannel <$> getChannel;
                    0x05 -> RecordMessage <$> getMessage size;
                    0x06 -> RecordChunk <$> getChunk;
                    0x07 -> RecordMessageIndex <$> getMessageIndex;
                    0x0A -> RecordDataEnd <$> getDataEnd;
                    0x0B -> RecordStatistics <$> getStatistics;
                    0x89 -> RecordUnknown <$> getByteString 8;
                    _ ->  RecordUnknown <$> getByteString size;
                }

getRecords :: Get [Record]
getRecords = do
    empty <- isEmpty
    if empty then return []
        else
            (do
                record <- getRecord
                records <- getRecords
                return $ record : records)

parseMcap :: BL.ByteString -> [Record]
parseMcap = runGet getRecords


onlyKnowns (RecordUnknown a) = False
onlyKnowns _ = True

main :: IO ()
main = do
    input <- BL.readFile "a.mcap"
    let records = parseMcap input
        in print $ filter onlyKnowns $ decompressRecords records


