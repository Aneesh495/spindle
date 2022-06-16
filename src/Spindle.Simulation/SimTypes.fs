namespace Spindle.Simulation

open System
open Spindle.Core.Domain
open Spindle.Cluster.NodeId

/// Adversarial fault models and discrete-event simulation configurations.
module SimTypes =

    /// A simulated chaos failure injected during cluster execution.
    [<RequireQualifiedAccess>]
    type SimulatedFault =
        | DropMessage of fromNode: string * toNode: string
        | PartitionCluster of groupA: string list * groupB: string list
        | HealPartition
        | CrashNode of nodeId: string
        | RestartNode of nodeId: string
        | InjectDelay of delayTicks: int64

    /// Configuration for a deterministic simulation experiment run.
    type SimulationConfig = {
        Seed: int
        NodeCount: int
        VirtualTickDurationMs: int64
        MaxVirtualTicks: int64
        FaultProbability: float
    } with
        static member Default = {
            Seed = 42
            NodeCount = 3
            VirtualTickDurationMs = 10L
            MaxVirtualTicks = 1000L
            FaultProbability = 0.05
        }
