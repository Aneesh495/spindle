namespace Spindle.Cli

open System
open Spindle.Cli.AnsiConsole
open Spindle.Cli.CliCommands

module Program =

    let printHelp () =
        printHeader "Spindle CLI Usage"
        printfn "spindle [command] [options]\n"
        printfn "Commands:"
        printfn "  demo                 Run end-to-end e-commerce saga and compensation demo"
        printfn "  bench                Run micro-benchmarks for hashing and event journal replay"
        printfn "  cluster              Inspect distributed cluster topology and hash ring slots"
        printfn "  help                 Display command-line syntax and option reference\n"

    [<EntryPoint>]
    let main args =
        printBanner ()

        let cmd =
            if args.Length = 0 then "demo"
            else args.[0].ToLowerInvariant()

        match cmd with
        | "demo" ->
            runDemo ()
            0
        | "bench" | "benchmark" ->
            runBenchmark ()
            0
        | "cluster" | "topology" ->
            showClusterTopology ()
            0
        | "help" | "--help" | "-h" ->
            printHelp ()
            0
        | unknown ->
            printError (sprintf "Unknown command '%s'. Run 'spindle help' for usage." unknown)
            1
