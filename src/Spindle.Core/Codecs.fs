namespace Spindle.Core

open System
open System.IO
open System.Text
open System.Text.Json
open System.Text.Json.Serialization
open Spindle.Core.Domain
open Spindle.Core.Metadata
open Spindle.Core.Commands
open Spindle.Core.Events

/// High-performance binary and JSON codecs for workflow events and command payloads.
module Codecs =

    /// Binary format magic identifier: 'S' 'P' 'N' 'D' (0x53504E44).
    let MagicBytes: uint32 = 0x53504E44u
    let CurrentFormatVersion: byte = 1uy

    /// Standard System.Text.Json options configured with camelCase and enum converters.
    let jsonOptions =
        let opts = JsonSerializerOptions()
        opts.PropertyNamingPolicy <- JsonNamingPolicy.CamelCase
        opts.WriteIndented <- false
        opts.Converters.Add(JsonStringEnumConverter(JsonNamingPolicy.CamelCase))
        opts

    let jsonOptionsIndented =
        let opts = JsonSerializerOptions()
        opts.PropertyNamingPolicy <- JsonNamingPolicy.CamelCase
        opts.WriteIndented <- true
        opts.Converters.Add(JsonStringEnumConverter(JsonNamingPolicy.CamelCase))
        opts

    /// Helper methods for JSON string and byte serialization.
    module Json =
        let serialize<'T> (value: 'T) : string =
            JsonSerializer.Serialize<'T>(value, jsonOptions)

        let serializePretty<'T> (value: 'T) : string =
            JsonSerializer.Serialize<'T>(value, jsonOptionsIndented)

        let deserialize<'T> (json: string) : 'T =
            JsonSerializer.Deserialize<'T>(json, jsonOptions)

        let serializeToBytes<'T> (value: 'T) : byte[] =
            JsonSerializer.SerializeToUtf8Bytes<'T>(value, jsonOptions)

        let deserializeFromBytes<'T> (bytes: byte[]) : 'T =
            JsonSerializer.Deserialize<'T>(ReadOnlySpan<byte>(bytes), jsonOptions)

    /// Binary frame encoder and decoder for fast append-only segment storage.
    module Binary =

        /// Serializes a JournalRecord into a framed byte array with header and checksum.
        let encodeRecord (record: JournalRecord) : byte[] =
            use ms = new MemoryStream()
            use writer = new BinaryWriter(ms, Encoding.UTF8)

            // Header: Magic (4) + Version (1) + Seq (8) + Ticks (8) + PrevHash (4) + RecordHash (4)
            writer.Write(MagicBytes)
            writer.Write(CurrentFormatVersion)
            writer.Write(record.SequenceNumber.Value)
            writer.Write(record.Timestamp.Ticks)
            writer.Write(record.PrevHash)
            writer.Write(record.RecordHash)

            // Event payload serialized via JSON envelope for schema evolution stability
            let eventJson = Json.serialize record.Event
            let jsonBytes = Encoding.UTF8.GetBytes(eventJson)
            writer.Write(jsonBytes.Length)
            writer.Write(jsonBytes)

            // Trailer checksum: Murmur3 over entire stream content so far
            writer.Flush()
            let rawBytes = ms.ToArray()
            let checksum = Murmur3.hashString 0u (Convert.ToBase64String(rawBytes))

            use fullMs = new MemoryStream()
            use fullWriter = new BinaryWriter(fullMs)
            fullWriter.Write(rawBytes)
            fullWriter.Write(checksum)
            fullWriter.Flush()
            fullMs.ToArray()

        /// Decodes a framed byte array back into a JournalRecord, validating magic and checksums.
        let decodeRecord (frame: byte[]) : Result<JournalRecord, string> =
            if frame.Length < 33 then
                Error "Frame underflow: length is less than minimal header and trailer size"
            else
                let payloadLength = frame.Length - 4
                let rawBytes = Array.sub frame 0 payloadLength
                let expectedChecksum = BitConverter.ToUInt32(frame, payloadLength)
                let actualChecksum = Murmur3.hashString 0u (Convert.ToBase64String(rawBytes))

                if expectedChecksum <> actualChecksum then
                    Error (sprintf "Checksum mismatch: expected 0x%08X, got 0x%08X" expectedChecksum actualChecksum)
                else
                    use ms = new MemoryStream(rawBytes)
                    use reader = new BinaryReader(ms, Encoding.UTF8)

                    let magic = reader.ReadUInt32()
                    if magic <> MagicBytes then
                        Error (sprintf "Invalid magic header: expected 0x%08X, got 0x%08X" MagicBytes magic)
                    else
                        let version = reader.ReadByte()
                        if version <> CurrentFormatVersion then
                            Error (sprintf "Unsupported codec format version: %d" version)
                        else
                            let seqVal = reader.ReadInt64()
                            let ticks = reader.ReadInt64()
                            let prevHash = reader.ReadUInt32()
                            let recordHash = reader.ReadUInt32()
                            let jsonLength = reader.ReadInt32()

                            if jsonLength < 0 || int64 (ms.Position + int64 jsonLength) > int64 rawBytes.Length then
                                Error "Corrupt payload length field in binary frame"
                            else
                                let jsonBytes = reader.ReadBytes(jsonLength)
                                let eventJson = Encoding.UTF8.GetString(jsonBytes)
                                try
                                    let evt = Json.deserialize<WorkflowEvent>(eventJson)
                                    Ok {
                                        SequenceNumber = SequenceNumber seqVal
                                        Timestamp = Timestamp ticks
                                        Event = evt
                                        PrevHash = prevHash
                                        RecordHash = recordHash
                                    }
                                with ex ->
                                    Error (sprintf "Failed to deserialize event payload: %s" ex.Message)
