namespace Spindle.Simulation

open System
open System.Collections.Generic
open Spindle.Simulation.SimTypes

/// High-performance deterministic pseudorandom number generator and chaos injection engine.
module ChaosEngine =

    /// 128-bit Xorshift+ pseudorandom number generator for reproducible chaos simulations.
    type Xorshift128Plus(seed: int) =
        let mutable s0 = uint64 (abs seed + 1) * 0x517cc1b727220a95UL
        let mutable s1 = uint64 (abs seed + 2) * 0x4f1bbcdcbfa54UL

        do
            // Warm up state to eliminate initial seed correlation
            for _ in 1 .. 16 do
                let mutable x = s0
                let y = s1
                s0 <- y
                x <- x ^^^ (x <<< 23)
                s1 <- x ^^^ y ^^^ (x >>> 17) ^^^ (y >>> 26)

        /// Generate next 64-bit pseudorandom unsigned integer.
        member _.NextUInt64() : uint64 =
            let mutable x = s0
            let y = s1
            s0 <- y
            x <- x ^^^ (x <<< 23)
            s1 <- x ^^^ y ^^^ (x >>> 17) ^^^ (y >>> 26)
            s1 + y

        /// Generate a uniform random float in [0.0, 1.0).
        member this.NextDouble() : float =
            let v = this.NextUInt64() >>> 11
            float v * (1.0 / float (1UL <<< 53))

        /// Generate a uniform random integer in [minInclusive, maxInclusive].
        member this.NextInt(minInclusive: int, maxInclusive: int) : int =
            if minInclusive >= maxInclusive then
                minInclusive
            else
                let range = int64 (maxInclusive - minInclusive + 1)
                let sample = int64 (this.NextUInt64() % uint64 range)
                minInclusive + int sample

    /// Deterministic chaos fault generator maintaining simulation invariants.
    type ChaosEngine(config: SimulationConfig) =
        let rng = Xorshift128Plus(config.Seed)
        let mutable isPartitioned = false
        let mutable crashedNodes = Set.empty<string>
        let faultLog = ResizeArray<int64 * SimulatedFault>()

        /// Retrieve complete record of injected faults.
        member _.FaultLog : (int64 * SimulatedFault) list =
            faultLog |> Seq.toList

        /// Evaluates whether a fault should be injected at the given virtual tick.
        member _.EvaluateFaults(tick: int64, activeNodes: string list) : SimulatedFault list =
            if activeNodes.Length < 2 then
                []
            else
                let roll = rng.NextDouble()
                if roll > config.FaultProbability then
                    []
                else
                    let faultTypeRoll = rng.NextInt(0, 4)
                    let generated =
                        match faultTypeRoll with
                        | 0 when not isPartitioned && activeNodes.Length >= 2 ->
                            // Split nodes into two partitions
                            let mid = max 1 (activeNodes.Length / 2)
                            let groupA = activeNodes |> List.take mid
                            let groupB = activeNodes |> List.skip mid
                            isPartitioned <- true
                            Some (SimulatedFault.PartitionCluster (groupA, groupB))
                        | 1 when isPartitioned ->
                            // Heal active partition
                            isPartitioned <- false
                            Some SimulatedFault.HealPartition
                        | 2 when crashedNodes.Count < (activeNodes.Length / 2) ->
                            // Crash a non-crashed node
                            let candidates = activeNodes |> List.filter (fun n -> not (crashedNodes.Contains n))
                            if candidates.IsEmpty then None
                            else
                                let idx = rng.NextInt(0, candidates.Length - 1)
                                let target = candidates.[idx]
                                crashedNodes <- crashedNodes.Add target
                                Some (SimulatedFault.CrashNode target)
                        | 3 when not crashedNodes.IsEmpty ->
                            // Recover a crashed node
                            let crashedList = crashedNodes |> Set.toList
                            let idx = rng.NextInt(0, crashedList.Length - 1)
                            let target = crashedList.[idx]
                            crashedNodes <- crashedNodes.Remove target
                            Some (SimulatedFault.RestartNode target)
                        | _ ->
                            // Drop packet between two distinct active nodes
                            let idx1 = rng.NextInt(0, activeNodes.Length - 1)
                            let idx2 = (idx1 + 1 + rng.NextInt(0, activeNodes.Length - 2)) % activeNodes.Length
                            let fromN = activeNodes.[idx1]
                            let toN = activeNodes.[idx2]
                            Some (SimulatedFault.DropMessage (fromN, toN))

                    match generated with
                    | Some f ->
                        faultLog.Add (tick, f)
                        [ f ]
                    | None -> []

        /// Deterministically reset chaos generator state.
        member _.Reset() =
            isPartitioned <- false
            crashedNodes <- Set.empty
            faultLog.Clear()
