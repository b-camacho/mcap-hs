# mcap-hs

very broken repo for learning `Data.Binary.Get`

# todo
record types:

 - [x]    Header (op=0x01)
 - [x]    Footer (op=0x02)
 - [x]    Schema (op=0x03)
 - [ ]    Channel (op=0x04) [needs metadata map]
 - [ ]    Message (op=0x05) [needs body deserialization]
 - [x]    Chunk (op=0x06)
 - [x]    Message Index (op=0x07)
 - [ ]    Chunk Index (op=0x08)
 - [ ]    Attachment (op=0x09)
 - [ ]    Metadata (op=0x0C)
 - [x]    Data End (op=0x0F)
 - [ ]    Attachment Index (op=0x0A)
 - [ ]    Metadata Index (op=0x0D)
 - [x]    Statistics (op=0x0B)
 - [ ]    Summary Offset (op=0x0E)

