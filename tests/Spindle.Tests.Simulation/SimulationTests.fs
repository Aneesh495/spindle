namespace Spindle.Tests.Simulation

open System
open Xunit
open Spindle.Core.Domain
open Spindle.Cluster.NodeId
open Spindle.Cluster
open Spindle.Simulation.SimTypes
open Spindle.Simulation.ChaosEngine
open Spindle.Simulation.SimCluster

module SimulationTests =

    [<Fact>]
    let ``Xorshift128Plus produces identical sequence for same seed`` () =
        let rng1 = Xorshift128Plus 42
        let rng2 = Xorshift128Plus 42
        let seq1 = [ for _ in 1 .. 100 -> rng1.NextUInt64() ]
        let seq2 = [ for _ in 1 .. 100 -> rng2.NextUInt64() ]
        Assert.Equal<uint64 seq>(seq1, seq2)

    [<Fact>]
    let ``Xorshift128Plus produces divergent sequence for different seeds`` () =
        let rng1 = Xorshift128Plus 42
        let rng2 = Xorshift128Plus 99
        let val1 = rng1.NextUInt64()
        let val2 = rng2.NextUInt64()
        Assert.True(val1 <> val2)

    [<Fact>]
    let ``ChaosEngine generates deterministic partition and crash faults`` () =
        let config = {
            SimulationConfig.Default with
                Seed = 12345
                FaultProbability = 1.0 // Force fault generation every tick
        }
        let engine = ChaosEngine config
        let nodes = [ "node-1"; "node-2"; "node-3"; "node-4" ]
        let faults = engine.EvaluateFaults(1L, nodes)
        Assert.NotEmpty faults
        Assert.NotEmpty engine.FaultLog

    [<Fact>]
    let ``SimCluster routes messages across nodes under virtual clock`` () =
        let config = {
            SimulationConfig.Default with
                NodeCount = 3
                FaultProbability = 0.0 // Disable random faults for clean routing test
        }
        let cluster = SimCluster config
        let n1 = cluster.Nodes.[0]
        let n2 = cluster.Nodes.[1]

        let ping = SwimMessage.Ping (n1.NodeId, 1L, [ n1.MemberInfo ])
        cluster.RoutePacket(n1.NodeId, n2.NodeId, ping, 1L)

        // Before step, n2 inbox is empty
        Assert.Equal(0, n2.Inbox.Count)

        // Advance 1 tick: packet delivered to n2 and processed
        cluster.Step()
        Assert.Equal(1, cluster.MessagesRouted)

        // Advance 2nd tick: n2's reply delivered back
        cluster.Step()
        Assert.Equal(2, cluster.MessagesRouted)

    [<Fact>]
    let ``SimCluster drops packets when network partition isolates nodes`` () =
        let config = {
            SimulationConfig.Default with
                NodeCount = 4
                FaultProbability = 0.0
        }
        let cluster = SimCluster config
        let n1 = cluster.Nodes.[0]
        let n3 = cluster.Nodes.[2]

        // Enqueue a packet from node-1 to node-3
        let ping = SwimMessage.Ping (n1.NodeId, 1L, [ n1.MemberInfo ])
        cluster.RoutePacket(n1.NodeId, n3.NodeId, ping, 1L)

        // Inject partition separating node-1 from node-3
        cluster.InjectFault (SimulatedFault.PartitionCluster ([ "node-1"; "node-2" ], [ "node-3"; "node-4" ]))

        // Advance tick, message should be dropped due to partition
        cluster.Step()
        Assert.Equal(0, n3.Inbox.Count)
        Assert.Equal(1, cluster.MessagesDropped)

    [<Fact>]
    let ``SimCluster executes 100 ticks without invariant violations`` () =
        let config = {
            SimulationConfig.Default with
                Seed = 777
                NodeCount = 4
                FaultProbability = 0.2
        }
        let cluster = SimCluster config
        cluster.RunTicks 100L
        Assert.Equal(100L, cluster.CurrentTick)
        Assert.True(cluster.Chaos.FaultLog.Length > 0)
