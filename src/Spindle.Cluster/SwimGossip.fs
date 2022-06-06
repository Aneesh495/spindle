namespace Spindle.Cluster

open System
open System.Collections.Concurrent
open Spindle.Core.Domain
open Spindle.Cluster.NodeId

/// Protocol messages for SWIM failure detection and membership gossip dissemination.
[<RequireQualifiedAccess>]
type SwimMessage =
    | Ping of sender: NodeId * incarnation: int64 * updates: MemberInfo list
    | Ack of sender: NodeId * incarnation: int64 * updates: MemberInfo list
    | PingReq of target: NodeId * originalSender: NodeId * updates: MemberInfo list

/// SWIM membership state machine tracking peer nodes, suspicion windows, and gossip infection.
type SwimMembershipView(localNode: MemberInfo) =

    let members = ConcurrentDictionary<string, MemberInfo>()
    let lastProbeTimes = ConcurrentDictionary<string, Timestamp>()
    let suspicionStartTimes = ConcurrentDictionary<string, Timestamp>()
    let syncRoot = Object()

    do members.[localNode.NodeId.Value] <- localNode

    member _.LocalNode = localNode

    member _.AllMembers : MemberInfo list =
        members.Values |> Seq.toList

    member _.AliveMembers : MemberInfo list =
        members.Values
        |> Seq.filter (fun m -> m.Status = NodeStatus.Alive)
        |> Seq.toList

    /// Merges an incoming member update according to SWIM precedence rules:
    /// 1. Higher incarnation always supersedes lower incarnation.
    /// 2. If incarnations match: Dead > Suspect > Alive.
    member _.ApplyUpdate(update: MemberInfo, now: Timestamp) : bool =
        lock syncRoot (fun () ->
            let id = update.NodeId.Value
            match members.TryGetValue id with
            | true, current ->
                let shouldApply =
                    if update.Incarnation > current.Incarnation then true
                    elif update.Incarnation = current.Incarnation then
                        match current.Status, update.Status with
                        | NodeStatus.Alive, NodeStatus.Suspect
                        | NodeStatus.Alive, NodeStatus.Dead
                        | NodeStatus.Suspect, NodeStatus.Dead -> true
                        | _ -> false
                    else false

                if shouldApply then
                    members.[id] <- update
                    if update.Status = NodeStatus.Suspect then
                        suspicionStartTimes.[id] <- now
                    elif update.Status = NodeStatus.Alive || update.Status = NodeStatus.Dead then
                        suspicionStartTimes.TryRemove(id) |> ignore
                    true
                else
                    false
            | false, _ ->
                members.[id] <- update
                if update.Status = NodeStatus.Suspect then
                    suspicionStartTimes.[id] <- now
                true
        )

    /// Marks a peer node as Suspect following a failed direct and indirect probe.
    member this.MarkSuspect(nodeId: NodeId, now: Timestamp) =
        lock syncRoot (fun () ->
            match members.TryGetValue nodeId.Value with
            | true, m when m.Status = NodeStatus.Alive ->
                let updated = { m with Status = NodeStatus.Suspect }
                this.ApplyUpdate(updated, now) |> ignore
            | _ -> ()
        )

    /// Marks a suspect node as Dead after its suspicion period expires without rebuttal.
    member this.MarkDead(nodeId: NodeId, now: Timestamp) =
        lock syncRoot (fun () ->
            match members.TryGetValue nodeId.Value with
            | true, m when m.Status <> NodeStatus.Dead ->
                let updated = { m with Status = NodeStatus.Dead }
                this.ApplyUpdate(updated, now) |> ignore
            | _ -> ()
        )

    /// Scans suspect nodes and transitions those exceeding suspicionTimeout to Dead.
    member this.EvaluateSuspicions(suspicionTimeout: Duration, now: Timestamp) : MemberInfo list =
        lock syncRoot (fun () ->
            let expired = ResizeArray<MemberInfo>()
            for kvp in suspicionStartTimes do
                let id = kvp.Key
                let startTime = kvp.Value
                if now.Ticks - startTime.Ticks >= suspicionTimeout.Ticks then
                    match members.TryGetValue id with
                    | true, m when m.Status = NodeStatus.Suspect ->
                        let dead = { m with Status = NodeStatus.Dead }
                        members.[id] <- dead
                        expired.Add(dead)
                    | _ -> ()

            for e in expired do
                suspicionStartTimes.TryRemove(e.NodeId.Value) |> ignore

            Seq.toList expired
        )
