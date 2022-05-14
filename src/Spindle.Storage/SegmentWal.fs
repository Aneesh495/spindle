namespace Spindle.Storage

open System
open System.IO
open System.Text
open Spindle.Core.Domain
open Spindle.Core.Events
open Spindle.Core.Codecs

/// Framed append-only Write-Ahead Log on disk with CRC32C integrity and torn write recovery.
type SegmentWal(filePath: string) =

    let [<Literal>] WalMagic: uint32 = 0x57414C31u // "WAL1"
    let syncRoot = Object()

    do
        let dir = Path.GetDirectoryName(filePath)
        if not (String.IsNullOrEmpty dir) && not (Directory.Exists dir) then
            Directory.CreateDirectory dir |> ignore

    member _.FilePath = filePath

    /// Appends a journal record to the WAL segment file with CRC32C checksum framing.
    member _.Append(record: JournalRecord) =
        lock syncRoot (fun () ->
            use fs = new FileStream(filePath, FileMode.Append, FileAccess.Write, FileShare.Read)
            use writer = new BinaryWriter(fs, Encoding.UTF8)

            let eventBytes: byte[] = Binary.encodeRecord record
            let payloadLen: int32 = eventBytes.Length
            let seq: int64 = record.SequenceNumber.Value
            let ticks: int64 = record.Timestamp.Ticks

            // Compute CRC32C over header fields and payload
            use ms = new MemoryStream()
            use headerWriter = new BinaryWriter(ms)
            headerWriter.Write(payloadLen)
            headerWriter.Write(seq)
            headerWriter.Write(ticks)
            headerWriter.Write(eventBytes, 0, eventBytes.Length)
            headerWriter.Flush()
            let checkBytes: byte[] = ms.ToArray()
            let checksum = Crc32c.computeAll checkBytes

            // Write frame: Magic + Checksum + HeaderPayload
            writer.Write(WalMagic)
            writer.Write(checksum)
            writer.Write(checkBytes, 0, checkBytes.Length)
            writer.Flush()
            fs.Flush(true)
        )

    /// Reads all valid journal records from the segment file, verifying CRC32C checksums
    /// and automatically repairing torn writes at the file tail.
    member _.Recover() : JournalRecord list =
        lock syncRoot (fun () ->
            if not (File.Exists filePath) then []
            else
                let records = ResizeArray<JournalRecord>()
                let mutable lastValidPosition = 0L

                use fs = new FileStream(filePath, FileMode.Open, FileAccess.ReadWrite, FileShare.None)
                use reader = new BinaryReader(fs, Encoding.UTF8)

                let mutable hasError = false
                while fs.Position < fs.Length && not hasError do
                    let frameStartPos = fs.Position
                    if fs.Length - frameStartPos < 28L then // Magic (4) + CRC (4) + Len (4) + Seq (8) + Ticks (8)
                        // Incomplete frame at tail (torn write), halt and truncate
                        hasError <- true
                    else
                        let magic = reader.ReadUInt32()
                        if magic <> WalMagic then
                            // Corrupted magic at tail
                            hasError <- true
                        else
                            let expectedChecksum = reader.ReadUInt32()
                            let payloadLen = reader.ReadInt32()

                            if payloadLen < 0 || fs.Length - fs.Position < int64 (16 + payloadLen) then
                                // Truncated payload
                                hasError <- true
                            else
                                let seq = reader.ReadInt64()
                                let ticks = reader.ReadInt64()
                                let eventBytes = reader.ReadBytes(payloadLen)

                                // Verify checksum
                                use ms = new MemoryStream()
                                use hw = new BinaryWriter(ms)
                                hw.Write(payloadLen)
                                hw.Write(seq)
                                hw.Write(ticks)
                                hw.Write(eventBytes, 0, eventBytes.Length)
                                hw.Flush()
                                let actualChecksum = Crc32c.computeAll (ms.ToArray())

                                if actualChecksum <> expectedChecksum then
                                    // Checksum mismatch, torn tail
                                    hasError <- true
                                else
                                    match Binary.decodeRecord eventBytes with
                                    | Ok recItem ->
                                        records.Add(recItem)
                                        lastValidPosition <- fs.Position
                                    | Error _ ->
                                        hasError <- true

                if hasError && lastValidPosition < fs.Length then
                    // Safely truncate torn write from tail of file
                    fs.SetLength(lastValidPosition)
                    fs.Flush(true)

                Seq.toList records
        )
