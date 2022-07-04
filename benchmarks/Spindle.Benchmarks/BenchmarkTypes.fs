namespace Spindle.Benchmarks

open System
open BenchmarkDotNet.Attributes
open Spindle.Core.Domain
open Spindle.Timers
open Spindle.Timers.TimingWheelTypes
open Spindle.Queue

/// Performance benchmarks evaluating core computational primitives.
[<MemoryDiagnoser>]
type KernelBenchmarks() =

    let wheel = HierarchicalWheel(1L)
    let ringBuffer = RingBuffer<string>(1024)

    [<GlobalSetup>]
    member _.Setup() =
        // Pre-populate timing wheel with 1000 timers across various cascade levels
        for i in 1L .. 1000L do
            let t = TimerEntry.Create(
                TimerId (sprintf "bench-timer-%d" i),
                WorkflowId "bench-wf",
                RunId (Guid.NewGuid()),
                0L,
                i * 4L,
                (fun () -> ())
            )
            wheel.Schedule(t) |> ignore

    [<Benchmark(Baseline = true)>]
    member _.Murmur3_ShortString() : uint32 =
        Murmur3.hashString 0u "order-fulfillment-workflow-id-1024"

    [<Benchmark>]
    member _.Murmur3_LongString() : uint32 =
        Murmur3.hashString 0u "spindle.durable.execution.engine.distributed.event.source.journal.record.partition.key.shard.992"

    [<Benchmark>]
    member _.HierarchicalWheel_Tick() : int =
        let fired = wheel.Tick()
        fired.Length

    [<Benchmark>]
    member _.RingBuffer_EnqueueDequeue() : bool =
        let enq = ringBuffer.TryEnqueue "bench-item"
        match ringBuffer.TryDequeue() with
        | Some _ -> enq
        | None -> false
