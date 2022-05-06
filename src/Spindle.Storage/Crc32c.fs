namespace Spindle.Storage

open System

/// High-performance Castagnoli CRC32C (polynomial 0x82F63B78) implementation for WAL frame integrity.
module Crc32c =

    let private Poly: uint32 = 0x82F63B78u

    /// Precomputed 256-entry lookup table for Castagnoli CRC32C.
    let private Table: uint32[] =
        let tbl = Array.zeroCreate<uint32> 256
        for i = 0 to 255 do
            let mutable entry = uint32 i
            for _ = 0 to 7 do
                if (entry &&& 1u) = 1u then
                    entry <- (entry >>> 1) ^^^ Poly
                else
                    entry <- entry >>> 1
            tbl.[i] <- entry
        tbl

    /// Computes the CRC32C checksum of a byte buffer slice.
    let compute (buffer: byte[]) (offset: int) (count: int) : uint32 =
        if buffer = null then invalidArg "buffer" "Buffer cannot be null"
        if offset < 0 || count < 0 || offset + count > buffer.Length then
            invalidArg "count" "Invalid buffer range specification"

        let mutable crc = 0xFFFFFFFFu
        for i = offset to offset + count - 1 do
            let index = byte (crc ^^^ uint32 buffer.[i])
            crc <- (crc >>> 8) ^^^ Table.[int index]
        crc ^^^ 0xFFFFFFFFu

    /// Computes the CRC32C checksum over an entire byte array.
    let computeAll (buffer: byte[]) : uint32 =
        compute buffer 0 buffer.Length
