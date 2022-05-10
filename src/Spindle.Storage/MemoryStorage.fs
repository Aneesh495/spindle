namespace Spindle.Storage

open System
open System.Collections.Concurrent
open Spindle.Core.Domain
open Spindle.Core.Events
open Spindle.Engine.StateMachine

/// In-memory storage engine implementing event store, snapshot store, and shard leasing.
type MemoryStorage() =

    let eventStore = ConcurrentDictionary<string, ResizeArray<JournalRecord>>()
    let snapshotStore = ConcurrentDictionary<string, WorkflowInstanceSnapshot>()
    let leaseStore = ConcurrentDictionary<int, string * FencingToken * Timestamp>()
    let syncRoot = Object()

    let key (wfId: WorkflowId) (runId: RunId) = sprintf "%s:%s" wfId.Value (runId.ToString())

    interface IEventStore with
        member _.AppendRecords(wfId, runId, expectedSeq, records) = async {
            return lock syncRoot (fun () ->
                let k = key wfId runId
                let list = eventStore.GetOrAdd(k, fun _ -> ResizeArray<JournalRecord>())
                let currentSeq =
                    if list.Count = 0 then SequenceNumber.Zero
                    else list.[list.Count - 1].SequenceNumber

                if currentSeq <> expectedSeq then
                    Error (StorageError.ConcurrencyConflict(expectedSeq, currentSeq))
                else
                    list.AddRange(records)
                    Ok ()
            )
        }

        member _.ReadRecords(wfId, runId, fromSequence) = async {
            return lock syncRoot (fun () ->
                let k = key wfId runId
                match eventStore.TryGetValue(k) with
                | true, list ->
                    list
                    |> Seq.filter (fun r -> r.SequenceNumber.Value >= fromSequence.Value)
                    |> Seq.toList
                | false, _ ->
                    []
            )
        }

    interface ISnapshotStore with
        member _.SaveSnapshot(snapshot) = async {
            let k = key snapshot.WorkflowId snapshot.RunId
            snapshotStore.[k] <- snapshot
        }

        member _.LoadSnapshot(wfId, runId) = async {
            let k = key wfId runId
            match snapshotStore.TryGetValue(k) with
            | true, snap -> return Some snap
            | false, _ -> return None
        }

    interface IShardLeaseStore with
        member _.AcquireLease(shardId, nodeAddr, proposedToken, duration, now) = async {
            return lock syncRoot (fun () ->
                let sId = shardId.Value
                match leaseStore.TryGetValue(sId) with
                | true, (_, existingToken, expiresAt) when now.Ticks < expiresAt.Ticks ->
                    Error (sprintf "Shard %d is currently held by lease %s until %s" sId (existingToken.ToString()) (expiresAt.ToDateTimeOffset().ToString("O")))
                | _ ->
                    let expiry = now.AddMilliseconds(int64 duration.TotalMilliseconds)
                    leaseStore.[sId] <- (nodeAddr, proposedToken, expiry)
                    Ok proposedToken
            )
        }

        member _.RenewLease(shardId, token, duration, now) = async {
            return lock syncRoot (fun () ->
                let sId = shardId.Value
                match leaseStore.TryGetValue(sId) with
                | true, (nodeAddr, currentToken, _) when currentToken = token ->
                    let expiry = now.AddMilliseconds(int64 duration.TotalMilliseconds)
                    leaseStore.[sId] <- (nodeAddr, currentToken, expiry)
                    true
                | _ -> false
            )
        }

        member _.ReleaseLease(shardId, token) = async {
            return lock syncRoot (fun () ->
                let sId = shardId.Value
                match leaseStore.TryGetValue(sId) with
                | true, (_, currentToken, _) when currentToken = token ->
                    leaseStore.TryRemove(sId) |> ignore
                    true
                | _ -> false
            )
        }
