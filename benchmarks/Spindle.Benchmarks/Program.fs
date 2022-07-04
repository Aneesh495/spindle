namespace Spindle.Benchmarks

open System
open BenchmarkDotNet.Running

module Program =

    [<EntryPoint>]
    let main args =
        printfn "Running Spindle Micro-Benchmarks..."
        let summary = BenchmarkRunner.Run<KernelBenchmarks>()
        printfn "Benchmark completed with %d reports." (summary.Reports.Length)
        0
