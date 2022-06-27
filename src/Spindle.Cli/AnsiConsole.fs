namespace Spindle.Cli

open System

/// ANSI escape codes and terminal styling utilities for high-fidelity console rendering.
module AnsiConsole =

    let Reset = "\u001b[0m"
    let Bold = "\u001b[1m"
    let Dim = "\u001b[2m"
    let Underline = "\u001b[4m"

    let Red = "\u001b[31m"
    let Green = "\u001b[32m"
    let Yellow = "\u001b[33m"
    let Blue = "\u001b[34m"
    let Magenta = "\u001b[35m"
    let Cyan = "\u001b[36m"
    let White = "\u001b[37m"
    let Gray = "\u001b[90m"

    let colorize (color: string) (text: string) : string =
        sprintf "%s%s%s" color text Reset

    let printBanner () =
        let banner =
            """
   _____       _           _ _      
  / ____|     (_)         | | |     
 | (___  _ __  _ _ __   __| | | ___ 
  \___ \| '_ \| | '_ \ / _` | |/ _ \
  ____) | |_) | | | | | (_| | |  __/
 |_____/| .__/|_|_| |_|\__,_|_|\___|
        | |                         
        |_|                         
"""
        printfn "%s%s%s" Cyan banner Reset
        printfn "%s  Deterministic Durable Execution Kernel & Saga Engine%s\n" Gray Reset

    let printSuccess (msg: string) =
        printfn "%s[SUCCESS]%s %s" Green Reset msg

    let printError (msg: string) =
        printfn "%s[ERROR]%s %s" Red Reset msg

    let printInfo (msg: string) =
        printfn "%s[INFO]%s %s" Cyan Reset msg

    let printHeader (title: string) =
        printfn "\n%s%s=== %s ===%s\n" Bold Cyan title Reset
