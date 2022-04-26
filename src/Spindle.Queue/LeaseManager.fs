namespace Spindle.Queue

open System
open System.Collections.Concurrent
open Spindle.Core.Domain
open Spindle.Core.Metadata

/// Distributed task lease representation with fencing tokens and heartbeat tracking.
type TaskLease = {
    LeaseId: string
    ActivityId: ActivityId
    WorkerId: string
    FencingToken: FencingToken
    AcquiredAt: Timestamp
    HeartbeatTimeout: Duration
    mutable LastHeartbeat: Timestamp
} with
    member this.IsExpired(now: Timestamp) : bool =
        let elapsedTicks = now.Ticks - this.LastHeartbeat.Ticks
        elapsedTicks > this.HeartbeatTimeout.Ticks

/// Thread-safe lease manager overseeing activity execution leases and dead-man timeouts.
type LeaseManager() =

    let activeLeases = ConcurrentDictionary<string, TaskLease>()
    let activityToLease = ConcurrentDictionary<string, string>()
    let mutable nextFencingToken = 1L
    let syncRoot = Object()

    member _.ActiveLeaseCount = activeLeases.Count

    /// Attempts to acquire an execution lease on an activity task.
    member _.AcquireLease(actId: ActivityId, workerId: string, timeout: Duration, now: Timestamp) : Result<TaskLease, string> =
        lock syncRoot (fun () ->
            match activityToLease.TryGetValue(actId.Value) with
            | true, existingLeaseId ->
                match activeLeases.TryGetValue(existingLeaseId) with
                | true, lease when not (lease.IsExpired now) ->
                    Error (sprintf "Activity %s is already leased to worker %s until %s" actId.Value lease.WorkerId (lease.LastHeartbeat.ToDateTimeOffset().ToString("O")))
                | _ ->
                    // Existing lease expired, reclaim
                    activeLeases.TryRemove(existingLeaseId) |> ignore
                    activityToLease.TryRemove(actId.Value) |> ignore
                    let fencing = FencingToken nextFencingToken
                    nextFencingToken <- nextFencingToken + 1L
                    let leaseId = sprintf "lease-%s-%d" actId.Value fencing.Value
                    let lease = {
                        LeaseId = leaseId
                        ActivityId = actId
                        WorkerId = workerId
                        FencingToken = fencing
                        AcquiredAt = now
                        HeartbeatTimeout = timeout
                        LastHeartbeat = now
                    }
                    activeLeases.[leaseId] <- lease
                    activityToLease.[actId.Value] <- leaseId
                    Ok lease
            | false, _ ->
                let fencing = FencingToken nextFencingToken
                nextFencingToken <- nextFencingToken + 1L
                let leaseId = sprintf "lease-%s-%d" actId.Value fencing.Value
                let lease = {
                    LeaseId = leaseId
                    ActivityId = actId
                    WorkerId = workerId
                    FencingToken = fencing
                    AcquiredAt = now
                    HeartbeatTimeout = timeout
                    LastHeartbeat = now
                }
                activeLeases.[leaseId] <- lease
                activityToLease.[actId.Value] <- leaseId
                Ok lease
        )

    /// Records a heartbeat for an active lease, extending its validity window.
    member _.RecordHeartbeat(leaseId: string, now: Timestamp) : Result<unit, string> =
        match activeLeases.TryGetValue(leaseId) with
        | true, lease ->
            if lease.IsExpired now then
                Error "Cannot heartbeat an already expired lease"
            else
                lease.LastHeartbeat <- now
                Ok ()
        | false, _ ->
            Error "Lease not found or already released"

    /// Releases a lease upon task completion or terminal failure.
    member _.Release(leaseId: string) : bool =
        lock syncRoot (fun () ->
            match activeLeases.TryRemove(leaseId) with
            | true, lease ->
                activityToLease.TryRemove(lease.ActivityId.Value) |> ignore
                true
            | false, _ -> false
        )

    /// Scans for and revokes all expired leases whose heartbeats have lapsed.
    member _.ReclaimExpired(now: Timestamp) : TaskLease list =
        lock syncRoot (fun () ->
            let expiredList = ResizeArray<TaskLease>()
            for kvp in activeLeases do
                if kvp.Value.IsExpired now then
                    expiredList.Add(kvp.Value)

            for exp in expiredList do
                activeLeases.TryRemove(exp.LeaseId) |> ignore
                activityToLease.TryRemove(exp.ActivityId.Value) |> ignore

            Seq.toList expiredList
        )
