namespace Spindle.Timers

open System
open Spindle.Core.Domain

/// Monotonic virtual clock abstraction supporting deterministic discrete-event simulation.
type VirtualClock(initialTimestamp: Timestamp) =

    let mutable currentTicks = initialTimestamp.Ticks
    let syncRoot = Object()

    member _.Now : Timestamp =
        lock syncRoot (fun () -> Timestamp currentTicks)

    member _.AdvanceMilliseconds(ms: int64) : Timestamp =
        lock syncRoot (fun () ->
            currentTicks <- currentTicks + (ms * 10000L)
            Timestamp currentTicks
        )

    member _.AdvanceSeconds(sec: int64) : Timestamp =
        lock syncRoot (fun () ->
            currentTicks <- currentTicks + (sec * 10000000L)
            Timestamp currentTicks
        )

    member _.SetTime(target: Timestamp) =
        lock syncRoot (fun () ->
            if target.Ticks < currentTicks then
                invalidOp "Virtual clock cannot move backward in time"
            currentTicks <- target.Ticks
        )

    static member CreateSystemUtc() =
        VirtualClock(Timestamp.FromDateTimeOffset DateTimeOffset.UtcNow)
