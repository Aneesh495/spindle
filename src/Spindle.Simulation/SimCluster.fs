namespace Spindle.Simulation

open System
open System.Collections.Generic
open Spindle.Core.Domain
open Spindle.Cluster.NodeId
open Spindle.Cluster
open Spindle.Storage
open Spindle.Simulation.SimTypes
open Spindle.Simulation.ChaosEngine

/// Discrete-event simulation of a multi-node Spindle cluster with network partitions and node crashes.
module SimCluster =

    /// A queued network packet awaiting delivery in the discrete-event simulator.
    type ScheduledPacket = {
        DeliverAtTick: int64
        FromNode: NodeId
        ToNode: NodeId
        Payload: SwimMessage
    }

    /// State and components of an individual simulated cluster node.
    type SimNode(memberInfo: MemberInfo, storage: MemoryStorage) =
        let membership = SwimMembershipView(memberInfo)
        let inboxes = Queue<SwimMessage>()
        let mutable isCrashed = false

        member _.MemberInfo = memberInfo
        member _.NodeId = memberInfo.NodeId
        member _.Storage = storage
        member _.Membership = membership
        member _.IsCrashed with get() = isCrashed and set v = isCrashed <- v
        member _.Inbox = inboxes

        member _.EnqueueMessage(msg: SwimMessage) =
            if not isCrashed then
                inboxes.Enqueue msg

        member _.ProcessMessages(now: Timestamp) : SwimMessage list =
            if isCrashed then
                inboxes.Clear()
                []
            else
                let outgoing = ResizeArray<SwimMessage>()
                while inboxes.Count > 0 do
                    let msg = inboxes.Dequeue()
                    match msg with
                    | SwimMessage.Ping (_, _, updates) ->
                        // Apply updates received in gossip piggyback
                        for u in updates do
                            membership.ApplyUpdate(u, now) |> ignore
                        // Reply with Ack
                        outgoing.Add (SwimMessage.Ack (memberInfo.NodeId, memberInfo.Incarnation, membership.AllMembers))
                    | SwimMessage.Ack (_, _, updates) ->
                        for u in updates do
                            membership.ApplyUpdate(u, now) |> ignore
                    | SwimMessage.PingReq (target, _, updates) ->
                        for u in updates do
                            membership.ApplyUpdate(u, now) |> ignore
                        // Forward ping to target
                        outgoing.Add (SwimMessage.Ping (memberInfo.NodeId, memberInfo.Incarnation, membership.AllMembers))
                outgoing |> Seq.toList

    /// Deterministic discrete-event simulated cluster coordinating multiple nodes and chaos injection.
    type SimCluster(config: SimulationConfig) =
        let chaos = ChaosEngine(config)
        let nodes = Dictionary<string, SimNode>()
        let packetQueue = ResizeArray<ScheduledPacket>()
        let mutable currentTick = 0L
        let mutable partitionA = Set.empty<string>
        let mutable partitionB = Set.empty<string>
        let mutable messagesRouted = 0
        let mutable messagesDropped = 0

        do
            // Initialize cluster nodes
            for i in 1 .. config.NodeCount do
                let nid = NodeId (sprintf "node-%d" i)
                let info = {
                    NodeId = nid
                    Host = "127.0.0.1"
                    Port = 9000 + i
                    Status = NodeStatus.Alive
                    Incarnation = 1L
                }
                let memStorage = MemoryStorage()
                let simNode = SimNode(info, memStorage)
                nodes.[nid.Value] <- simNode

            // Seed full initial knowledge across nodes
            let allMembers = nodes.Values |> Seq.map (fun n -> n.MemberInfo) |> Seq.toList
            let t0 = Timestamp.FromUnixTimeMilliseconds 0L
            for n in nodes.Values do
                for m in allMembers do
                    n.Membership.ApplyUpdate(m, t0) |> ignore

        member _.CurrentTick = currentTick
        member _.Nodes = nodes.Values |> Seq.toList
        member _.MessagesRouted = messagesRouted
        member _.MessagesDropped = messagesDropped
        member _.Chaos = chaos

        /// Determine if two nodes can communicate given current network partition state.
        member private _.CanCommunicate(fromN: string, toN: string) : bool =
            if partitionA.IsEmpty && partitionB.IsEmpty then
                true
            else
                let inA1 = partitionA.Contains fromN
                let inB1 = partitionB.Contains fromN
                let inA2 = partitionA.Contains toN
                let inB2 = partitionB.Contains toN
                not ((inA1 && inB2) || (inB1 && inA2))

        /// Manually injects a specific fault into the simulation environment.
        member _.InjectFault(fault: SimulatedFault) =
            match fault with
            | SimulatedFault.PartitionCluster (gA, gB) ->
                partitionA <- Set.ofList gA
                partitionB <- Set.ofList gB
            | SimulatedFault.HealPartition ->
                partitionA <- Set.empty
                partitionB <- Set.empty
            | SimulatedFault.CrashNode nid ->
                match nodes.TryGetValue nid with
                | true, n -> n.IsCrashed <- true
                | _ -> ()
            | SimulatedFault.RestartNode nid ->
                match nodes.TryGetValue nid with
                | true, n -> n.IsCrashed <- false
                | _ -> ()
            | SimulatedFault.DropMessage _ -> ()
            | SimulatedFault.InjectDelay _ -> ()

        /// Advances the simulation clock by a single discrete tick.
        member this.Step() =
            currentTick <- currentTick + 1L
            let now = Timestamp.FromUnixTimeMilliseconds (currentTick * config.VirtualTickDurationMs)
            let activeNodeIds =
                nodes.Values
                |> Seq.filter (fun n -> not n.IsCrashed)
                |> Seq.map (fun n -> n.NodeId.Value)
                |> Seq.toList

            // 1. Evaluate and apply chaos faults
            let faults = chaos.EvaluateFaults(currentTick, activeNodeIds)
            for fault in faults do
                this.InjectFault fault

            // 2. Deliver scheduled packets whose delivery tick has arrived
            let readyPackets = ResizeArray<ScheduledPacket>()
            let remainingPackets = ResizeArray<ScheduledPacket>()

            for p in packetQueue do
                if p.DeliverAtTick <= currentTick then
                    readyPackets.Add p
                else
                    remainingPackets.Add p

            packetQueue.Clear()
            packetQueue.AddRange remainingPackets

            for p in readyPackets do
                let fromId = p.FromNode.Value
                let toId = p.ToNode.Value
                if not (this.CanCommunicate(fromId, toId)) then
                    messagesDropped <- messagesDropped + 1
                else
                    match nodes.TryGetValue toId with
                    | true, targetNode when not targetNode.IsCrashed ->
                        targetNode.EnqueueMessage p.Payload
                        messagesRouted <- messagesRouted + 1
                    | _ ->
                        messagesDropped <- messagesDropped + 1

            // 3. Process inbox messages on each active node and schedule responses
            for n in nodes.Values do
                if not n.IsCrashed then
                    let responses = n.ProcessMessages(now)
                    for r in responses do
                        let targetNodeId =
                            match r with
                            | SwimMessage.Ack (sender, _, _) ->
                                nodes.Values
                                |> Seq.filter (fun p -> p.NodeId <> sender && not p.IsCrashed)
                                |> Seq.tryHead
                                |> Option.map (fun p -> p.NodeId)
                            | SwimMessage.Ping (_, _, _) ->
                                nodes.Values
                                |> Seq.filter (fun p -> p.NodeId <> n.NodeId && not p.IsCrashed)
                                |> Seq.tryHead
                                |> Option.map (fun p -> p.NodeId)
                            | SwimMessage.PingReq (tgt, _, _) ->
                                Some tgt

                        match targetNodeId with
                        | Some tgt ->
                            packetQueue.Add {
                                DeliverAtTick = currentTick + 1L
                                FromNode = n.NodeId
                                ToNode = tgt
                                Payload = r
                            }
                        | None -> ()

        /// Send a message from one node to another through the simulated network switch.
        member _.RoutePacket(fromNode: NodeId, toNode: NodeId, msg: SwimMessage, delayTicks: int64) =
            packetQueue.Add {
                DeliverAtTick = currentTick + max 1L delayTicks
                FromNode = fromNode
                ToNode = toNode
                Payload = msg
            }

        /// Runs simulation until target tick count is reached.
        member this.RunTicks(tickCount: int64) =
            for _ in 1L .. tickCount do
                this.Step()
