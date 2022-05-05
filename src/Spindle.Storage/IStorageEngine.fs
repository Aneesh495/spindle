namespace Spindle.Storage

open System
open Spindle.Core.Domain
open Spindle.Core.Events
open Spindle.Engine.StateMachine

/// Storage error categories and failure diagnostics.
[<RequireQualifiedAccess>]
type StorageError =
    | ConcurrencyConflict of expected: SequenceNumber * actual: SequenceNumber
    | RecordCorrupted of details: string
    | RecordNotFound of details: string
    | IoError of message: string

/// Storage contract for append-only workflow event journals.
type IEventStore =
    /// Appends records to the journal for a specific workflow execution run.
    /// Fails with ConcurrencyConflict if the expected sequence number does not match current state.
    abstract AppendRecords:
        WorkflowId * RunId * SequenceNumber * JournalRecord list -> Async<Result<unit, StorageError>>

    /// Reads all records for a workflow execution run starting from the given sequence number.
    abstract ReadRecords:
        WorkflowId * RunId * SequenceNumber -> Async<JournalRecord list>

/// Storage contract for persisting point-in-time workflow instance snapshots.
type ISnapshotStore =
    abstract SaveSnapshot: WorkflowInstanceSnapshot -> Async<unit>
    abstract LoadSnapshot: WorkflowId * RunId -> Async<WorkflowInstanceSnapshot option>

/// Storage contract for partitioned shard lease management and leader fencing.
type IShardLeaseStore =
    abstract AcquireLease:
        ShardId * string * FencingToken * Duration * Timestamp -> Async<Result<FencingToken, string>>

    abstract RenewLease:
        ShardId * FencingToken * Duration * Timestamp -> Async<bool>

    abstract ReleaseLease:
        ShardId * FencingToken -> Async<bool>
