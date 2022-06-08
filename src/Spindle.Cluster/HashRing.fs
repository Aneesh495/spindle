namespace Spindle.Cluster

open System
open Spindle.Core.Domain
open Spindle.Cluster.NodeId

/// A virtual node placement point on the consistent hash ring.
[<Struct>]
type VirtualNode = {
    Hash: uint32
    Node: MemberInfo
}

/// Consistent hash ring with virtual nodes for uniform workflow shard partitioning.
type ConsistentHashRing(vnodesPerNode: int) =

    let mutable ring: VirtualNode[] = [||]
    let syncRoot = Object()

    member _.NodeCount =
        lock syncRoot (fun () ->
            ring
            |> Seq.map (fun vn -> vn.Node.NodeId.Value)
            |> Seq.distinct
            |> Seq.length
        )

    /// Rebuilds the hash ring with the specified active cluster nodes.
    member _.UpdateNodes(nodes: MemberInfo list) =
        lock syncRoot (fun () ->
            let vnodes = ResizeArray<VirtualNode>()
            for node in nodes do
                if node.Status = NodeStatus.Alive then
                    for v = 0 to vnodesPerNode - 1 do
                        let vnodeKey = sprintf "%s#vnode%d" node.NodeId.Value v
                        let h = Murmur3.hashString 0u vnodeKey
                        vnodes.Add { Hash = h; Node = node }

            ring <- vnodes.ToArray() |> Array.sortBy (fun vn -> vn.Hash)
        )

    /// Maps a string partition key to the owning cluster node via consistent ring lookup.
    member _.GetNode(key: string) : MemberInfo option =
        lock syncRoot (fun () ->
            if ring.Length = 0 then None
            else
                let keyHash = Murmur3.hashString 0u key
                // Binary search for first vnode with Hash >= keyHash
                let mutable low = 0
                let mutable high = ring.Length - 1
                let mutable foundIdx = 0

                if keyHash > ring.[high].Hash then
                    foundIdx <- 0 // Ring wraparound
                else
                    while low <= high do
                        let mid = low + (high - low) / 2
                        if ring.[mid].Hash >= keyHash then
                            foundIdx <- mid
                            high <- mid - 1
                        else
                            low <- mid + 1

                Some ring.[foundIdx].Node
        )

    /// Returns a list of distinct physical nodes owning replicas for the given key.
    member _.GetReplicas(key: string, replicaCount: int) : MemberInfo list =
        lock syncRoot (fun () ->
            if ring.Length = 0 then []
            else
                let keyHash = Murmur3.hashString 0u key
                let distinctNodes = ResizeArray<MemberInfo>()
                let visitedIds = System.Collections.Generic.HashSet<string>()

                let mutable startIdx = 0
                while startIdx < ring.Length && ring.[startIdx].Hash < keyHash do
                    startIdx <- startIdx + 1
                if startIdx >= ring.Length then startIdx <- 0

                let mutable currentIdx = startIdx
                let mutable steps = 0
                while distinctNodes.Count < replicaCount && steps < ring.Length do
                    let candidate = ring.[currentIdx].Node
                    if visitedIds.Add(candidate.NodeId.Value) then
                        distinctNodes.Add(candidate)
                    currentIdx <- (currentIdx + 1) % ring.Length
                    steps <- steps + 1

                Seq.toList distinctNodes
        )
