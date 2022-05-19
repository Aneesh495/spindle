namespace Spindle.Tests.Storage

open System
open System.IO
open System.Text
open Xunit
open Spindle.Core.Domain
open Spindle.Core.Metadata
open Spindle.Core.Events
open Spindle.Engine.StateMachine
open Spindle.Storage

module Crc32cTests =

    [<Fact>]
    let ``Crc32c matches standard Castagnoli 32-bit test vector`` () =
        let bytes = Encoding.ASCII.GetBytes("123456789")
        let checksum = Crc32c.computeAll bytes
        Assert.Equal(0xE3069283u, checksum)

    [<Fact>]
    let ``Crc32c detects single bit alterations`` () =
        let data = Encoding.UTF8.GetBytes("The quick brown fox jumps over the lazy dog")
        let originalCrc = Crc32c.computeAll data

        let tampered = Array.copy data
        tampered.[10] <- tampered.[10] ^^^ 1uy
        let tamperedCrc = Crc32c.computeAll tampered

        Assert.NotEqual(originalCrc, tamperedCrc)

module MemoryStorageTests =

    [<Fact>]
    let ``MemoryStorage detects concurrency conflicts on sequence mismatch`` () =
        let store = MemoryStorage() :> IEventStore
        let wfId = WorkflowId "wf-mem-conflict"
        let runId = RunId (Guid.NewGuid())
        let ts = Timestamp.FromUnixTimeMilliseconds 1000L

        let r1 = JournalRecord.Create(SequenceNumber 1L, ts, WorkflowEvent.WorkflowExecutionStarted("Flow", [||], "main", None, None, HeaderMap.empty, 1), 0u)
        let appendRes1 = store.AppendRecords(wfId, runId, SequenceNumber.Zero, [ r1 ]) |> Async.RunSynchronously
        Assert.True(appendRes1 |> Result.isOk)

        // Try to append with wrong expected sequence (Zero instead of 1L)
        let r2 = JournalRecord.Create(SequenceNumber 2L, ts, WorkflowEvent.WorkflowExecutionCompleted([||]), r1.RecordHash)
        let appendRes2 = store.AppendRecords(wfId, runId, SequenceNumber.Zero, [ r2 ]) |> Async.RunSynchronously

        match appendRes2 with
        | Error (StorageError.ConcurrencyConflict(expected, actual)) ->
            Assert.Equal(SequenceNumber.Zero, expected)
            Assert.Equal(SequenceNumber 1L, actual)
        | other ->
            Assert.Fail(sprintf "Expected ConcurrencyConflict, got %A" other)

    [<Fact>]
    let ``MemoryStorage saves and loads snapshots`` () =
        let store = MemoryStorage() :> ISnapshotStore
        let wfId = WorkflowId "wf-snap"
        let runId = RunId (Guid.NewGuid())
        let ts = Timestamp.FromUnixTimeMilliseconds 1000L

        let snap = {
            WorkflowInstanceSnapshot.Create(wfId, runId, "SnapFlow", ts) with
                Status = ExecutionStatus.Running
                CurrentSequence = SequenceNumber 42L
        }

        store.SaveSnapshot(snap) |> Async.RunSynchronously
        let loaded = store.LoadSnapshot(wfId, runId) |> Async.RunSynchronously

        Assert.True(loaded.IsSome)
        Assert.Equal(SequenceNumber 42L, loaded.Value.CurrentSequence)
        Assert.Equal(ExecutionStatus.Running, loaded.Value.Status)

module SegmentWalTests =

    [<Fact>]
    let ``SegmentWal appends and recovers records cleanly`` () =
        let tempFile = Path.Combine(Path.GetTempPath(), sprintf "spindle-test-%s.wal" (Guid.NewGuid().ToString("N")))
        try
            let wal = SegmentWal(tempFile)
            let ts = Timestamp.FromUnixTimeMilliseconds 1000L

            let r1 = JournalRecord.Create(SequenceNumber 1L, ts, WorkflowEvent.WorkflowExecutionStarted("WalFlow", [||], "main", None, None, HeaderMap.empty, 1), 0u)
            let r2 = JournalRecord.Create(SequenceNumber 2L, ts.AddMilliseconds 50L, WorkflowEvent.ActivityTaskScheduled(ActivityId "act-1", "Step1", [||], ActivityOptions.Create("main", Duration.FromSeconds 5L), HeaderMap.empty), r1.RecordHash)

            wal.Append(r1)
            wal.Append(r2)

            let recovered = wal.Recover()
            Assert.Equal(2, recovered.Length)
            Assert.Equal(SequenceNumber 1L, recovered.[0].SequenceNumber)
            Assert.Equal(SequenceNumber 2L, recovered.[1].SequenceNumber)
        finally
            if File.Exists tempFile then File.Delete tempFile

    [<Fact>]
    let ``SegmentWal detects torn write at tail and safely truncates file`` () =
        let tempFile = Path.Combine(Path.GetTempPath(), sprintf "spindle-torn-%s.wal" (Guid.NewGuid().ToString("N")))
        try
            let wal = SegmentWal(tempFile)
            let ts = Timestamp.FromUnixTimeMilliseconds 1000L

            let r1 = JournalRecord.Create(SequenceNumber 1L, ts, WorkflowEvent.WorkflowExecutionStarted("TornFlow", [||], "main", None, None, HeaderMap.empty, 1), 0u)
            wal.Append(r1)

            let validLength = FileInfo(tempFile).Length

            // Simulate torn write: append 12 garbage bytes at tail
            use fs = new FileStream(tempFile, FileMode.Append, FileAccess.Write)
            fs.Write([| 0x57uy; 0x41uy; 0x4Cuy; 0x31uy; 0xFFuy; 0xFFuy; 0x01uy; 0x02uy; 0x03uy; 0x04uy |], 0, 10)
            fs.Flush(true)
            fs.Close()

            let corruptedLength = FileInfo(tempFile).Length
            Assert.True(corruptedLength > validLength)

            // Recover: should detect torn tail, truncate to validLength, and return valid r1
            let recovered = wal.Recover()
            Assert.Single(recovered) |> ignore
            Assert.Equal(SequenceNumber 1L, recovered.[0].SequenceNumber)

            let repairedLength = FileInfo(tempFile).Length
            Assert.Equal(validLength, repairedLength)
        finally
            if File.Exists tempFile then File.Delete tempFile

module SqliteStorageTests =

    [<Fact>]
    let ``SqliteStorage handles transactional append and read`` () =
        let dbFile = Path.Combine(Path.GetTempPath(), sprintf "spindle-sqlite-%s.db" (Guid.NewGuid().ToString("N")))
        let connStr = sprintf "Data Source=%s;" dbFile
        try
            let store = SqliteStorage(connStr) :> IEventStore
            let wfId = WorkflowId "wf-sqlite"
            let runId = RunId (Guid.NewGuid())
            let ts = Timestamp.FromUnixTimeMilliseconds 1000L

            let r1 = JournalRecord.Create(SequenceNumber 1L, ts, WorkflowEvent.WorkflowExecutionStarted("SqliteFlow", [||], "main", None, None, HeaderMap.empty, 1), 0u)
            let r2 = JournalRecord.Create(SequenceNumber 2L, ts.AddMilliseconds 100L, WorkflowEvent.WorkflowExecutionCompleted([| 42uy |]), r1.RecordHash)

            let appendRes = store.AppendRecords(wfId, runId, SequenceNumber.Zero, [ r1; r2 ]) |> Async.RunSynchronously
            Assert.True(appendRes |> Result.isOk)

            let readRecords = store.ReadRecords(wfId, runId, SequenceNumber.Initial) |> Async.RunSynchronously
            Assert.Equal(2, readRecords.Length)
            Assert.Equal(SequenceNumber 1L, readRecords.[0].SequenceNumber)
            Assert.Equal(SequenceNumber 2L, readRecords.[1].SequenceNumber)

            // Read starting from sequence 2: only r2
            let readFrom2 = store.ReadRecords(wfId, runId, SequenceNumber 2L) |> Async.RunSynchronously
            Assert.Single(readFrom2) |> ignore
            Assert.Equal(SequenceNumber 2L, readFrom2.[0].SequenceNumber)
        finally
            if File.Exists dbFile then File.Delete dbFile
