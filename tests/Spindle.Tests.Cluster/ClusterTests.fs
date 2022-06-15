namespace Spindle.Tests.Cluster

open System
open Xunit
open Spindle.Core.Domain
open Spindle.Cluster.NodeId
open Spindle.Cluster

module SwimGossipTests =

    [<Fact>]
    let ``SWIM accepts higher incarnation number updates`` () =
        let node1 = MemberInfo.Create(NodeId "node-1", "127.0.0.1", 7001, 1L, NodeStatus.Alive)
        let view = SwimMembershipView(node1)
        let now = Timestamp.FromUnixTimeMilliseconds 1000L

        let node2v1 = MemberInfo.Create(NodeId "node-2", "127.0.0.1", 7002, 1L, NodeStatus.Alive)
        Assert.True(view.ApplyUpdate(node2v1, now))

        // Incoming update with higher incarnation and suspect status: accepted!
        let node2v2 = MemberInfo.Create(NodeId "node-2", "127.0.0.1", 7002, 2L, NodeStatus.Suspect)
        Assert.True(view.ApplyUpdate(node2v2, now))

        let current = view.AllMembers |> List.find (fun m -> m.NodeId.Value = "node-2")
        Assert.Equal(2L, current.Incarnation)
        Assert.Equal(NodeStatus.Suspect, current.Status)

        // Stale update with lower incarnation: rejected!
        let node2Stale = MemberInfo.Create(NodeId "node-2", "127.0.0.1", 7002, 1L, NodeStatus.Alive)
        Assert.False(view.ApplyUpdate(node2Stale, now))

    [<Fact>]
    let ``SWIM enforces state precedence on identical incarnation`` () =
        let local = MemberInfo.Create(NodeId "local", "127.0.0.1", 7000, 1L, NodeStatus.Alive)
        let view = SwimMembershipView(local)
        let now = Timestamp.FromUnixTimeMilliseconds 1000L

        let peerAlive = MemberInfo.Create(NodeId "peer", "127.0.0.1", 7001, 5L, NodeStatus.Alive)
        view.ApplyUpdate(peerAlive, now) |> ignore

        // Suspect supersedes Alive on same incarnation
        let peerSuspect = MemberInfo.Create(NodeId "peer", "127.0.0.1", 7001, 5L, NodeStatus.Suspect)
        Assert.True(view.ApplyUpdate(peerSuspect, now))

        // Dead supersedes Suspect on same incarnation
        let peerDead = MemberInfo.Create(NodeId "peer", "127.0.0.1", 7001, 5L, NodeStatus.Dead)
        Assert.True(view.ApplyUpdate(peerDead, now))

        // Alive cannot override Dead on same incarnation
        Assert.False(view.ApplyUpdate(peerAlive, now))

    [<Fact>]
    let ``SWIM suspicion timeout transitions node to Dead`` () =
        let local = MemberInfo.Create(NodeId "local", "127.0.0.1", 7000, 1L, NodeStatus.Alive)
        let view = SwimMembershipView(local)
        let startTs = Timestamp.FromUnixTimeMilliseconds 1000L
        let peer = MemberInfo.Create(NodeId "peer-slow", "127.0.0.1", 7001, 1L, NodeStatus.Alive)
        view.ApplyUpdate(peer, startTs) |> ignore

        // Mark peer suspect
        view.MarkSuspect(peer.NodeId, startTs)
        let suspicionTimeout = Duration.FromSeconds 5L

        // After 2 seconds: still suspect
        let deadBefore = view.EvaluateSuspicions(suspicionTimeout, startTs.AddSeconds 2L)
        Assert.Empty(deadBefore)

        // After 6 seconds: suspicion period exceeded, transitioned to Dead!
        let deadAfter = view.EvaluateSuspicions(suspicionTimeout, startTs.AddSeconds 6L)
        Assert.Single(deadAfter) |> ignore
        Assert.Equal("peer-slow", deadAfter.[0].NodeId.Value)
        Assert.Equal(NodeStatus.Dead, deadAfter.[0].Status)

module ConsistentHashRingTests =

    [<Fact>]
    let ``HashRing maps keys deterministically to nodes`` () =
        let ring = ConsistentHashRing(64)
        let n1 = MemberInfo.Create(NodeId "node-alpha", "10.0.0.1", 8000, 1L, NodeStatus.Alive)
        let n2 = MemberInfo.Create(NodeId "node-beta", "10.0.0.2", 8000, 1L, NodeStatus.Alive)
        let n3 = MemberInfo.Create(NodeId "node-gamma", "10.0.0.3", 8000, 1L, NodeStatus.Alive)
        ring.UpdateNodes([ n1; n2; n3 ])

        let nodeA1 = ring.GetNode("order-wf-99")
        let nodeA2 = ring.GetNode("order-wf-99")
        Assert.Equal(nodeA1, nodeA2)
        Assert.True(nodeA1.IsSome)

    [<Fact>]
    let ``HashRing returns distinct replicas`` () =
        let ring = ConsistentHashRing(64)
        let n1 = MemberInfo.Create(NodeId "n1", "10.0.0.1", 8000, 1L, NodeStatus.Alive)
        let n2 = MemberInfo.Create(NodeId "n2", "10.0.0.2", 8000, 1L, NodeStatus.Alive)
        let n3 = MemberInfo.Create(NodeId "n3", "10.0.0.3", 8000, 1L, NodeStatus.Alive)
        ring.UpdateNodes([ n1; n2; n3 ])

        let replicas = ring.GetReplicas("important-ledger-key", 2)
        Assert.Equal(2, replicas.Length)
        Assert.True(replicas.[0].NodeId.Value <> replicas.[1].NodeId.Value)

module ShardRouterTests =

    [<Fact>]
    let ``ShardRouter partitions workflows deterministically`` () =
        let ring = ConsistentHashRing(64)
        let n1 = MemberInfo.Create(NodeId "n1", "10.0.0.1", 8000, 1L, NodeStatus.Alive)
        let n2 = MemberInfo.Create(NodeId "n2", "10.0.0.2", 8000, 1L, NodeStatus.Alive)
        ring.UpdateNodes([ n1; n2 ])

        let router = ShardRouter(ring, 1024)
        let s1 = router.GetShardId(WorkflowId "customer-331")
        let s2 = router.GetShardId(WorkflowId "customer-331")
        Assert.Equal(s1.Value, s2.Value)
        Assert.True(s1.Value >= 0 && s1.Value < 1024)

        let owner = router.RouteWorkflow(WorkflowId "customer-331")
        Assert.True(owner.IsSome)
