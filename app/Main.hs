{-# LANGUAGE ScopedTypeVariables, NamedFieldPuns, BlockArguments #-}

module Main where

import Data.Word
import Data.Binary.Put
import qualified Data.ByteString as BS
import Data.Char
import Debug.Trace
import Text.Printf

import Data.Binary.Get
import Data.Map (Map)
import qualified Data.Map as Map
import Control.Monad (unless)
import Control.Monad.State (evalState, get, put, runStateT, StateT)
import Data.Binary.Builder (toLazyByteString)

data Header = Header { profile :: String, library :: String }
    deriving (Read, Show, Eq);
data Schema = Schema { sid :: Word16, name :: String, encoding :: String, s_data :: BS.ByteString }
    deriving (Read, Show, Eq);
data Channel = Channel { cid :: Word16, schema_id :: Word16, topic:: String, message_encoding :: String, rest :: BS.ByteString} -- no metadata for now, brain too small
    deriving (Read, Show, Eq);
data Message = Message { channel_id :: Word16, sequence :: Word32, c_data :: BS.ByteString } -- no timestamps for now
    deriving (Read, Show, Eq);
data Record = RecordHeader Header | RecordChannel Channel | RecordSchema Schema | RecordMessage Message
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

-- ik this should be Text but we're taking things one at a time
getString :: Get String
getString = do
  strLen <- getInt32le            -- Read the length as a Word32 (little-endian)
  rawString <- getByteString $ fromIntegral strLen -- Read the specified number of bytes
  let unpacked = BS.unpack rawString
    in
    pure $ map (chr . fromEnum) unpacked

getBytes' :: Get BS.ByteString
getBytes' = do
  bytesLen <- getInt32le
  getByteString $ fromIntegral bytesLen

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
    log_time <- getWord64le
    pub_time <- getWord64le
    _data <- getByteString (size - 2 - 4 - 8 - 8)
    pure $ Message channel_id sequence _data


getRecord :: Get Record
getRecord = do
    opcode <- getWord8
    size <- fromIntegral getWord64le
    let recfunc = case opcode of {
        0x01 -> RecordHeader <$> getHeader;
        0x03 -> RecordSchema <$> getSchema;
        0x04 -> RecordChannel <$> getChannel;
        0x05 -> RecordMessage <$> getMessage size;
    }
    recfunc

getRecords :: Get [Record]
getRecords = do
    empty <-isEmpty
    if empty
        then return []
        else
            (do
                record <- getRecord
                records <- getRecords
                return $ record : records)


parseMcap :: BS.ByteString -> [Record]
parseMcap = Data.Binary.Get.runGet getRecords


main :: IO ()
main = do
    input <- BS.readFile "a.mcap"
    let (magic, rest) = BS.splitAt 8 input
        in
        if not $ isMcap magic then putStrLn "not an mcap file: bad magic"
        else let records = parseMcap rest
            in print $ head records









