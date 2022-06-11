namespace Spindle.Cluster

open System
open Spindle.Core.Domain
open Spindle.Cluster.NodeId

/// Routes workflow execution instances and shard partitions to cluster nodes.
type ShardRouter(ring: ConsistentHashRing, totalShards: int) =

    let total = if totalShards <= 0 then 1024 else totalShards

    member _.TotalShards = total

    /// Deterministically computes the ShardId partition for a workflow identifier.
    member _.GetShardId(wfId: WorkflowId) : ShardId =
        let h = Murmur3.hashString 0u wfId.Value
        let slot = int (h % uint32 total)
        ShardId slot

    /// Resolves the owner cluster node responsible for a workflow instance.
    member this.RouteWorkflow(wfId: WorkflowId) : MemberInfo option =
        let shard = this.GetShardId wfId
        let shardKey = sprintf "shard-%d" shard.Value
        ring.GetNode(shardKey)

    /// Determines whether a shard partition is owned by the local node.
    member _.IsLocal(shardId: ShardId, localNodeId: NodeId) : bool =
        let shardKey = sprintf "shard-%d" shardId.Value
        match ring.GetNode shardKey with
        | Some owner -> owner.NodeId = localNodeId
        | None -> false
