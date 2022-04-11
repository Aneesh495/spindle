namespace Spindle.Timers

open System
open System.Collections.Concurrent
open Spindle.Core.Domain
open Spindle.Timers.TimingWheelTypes

/// High-level durable timer queue coordinating hierarchical wheel execution and recovery.
type DurableTimerQueue(clock: VirtualClock, tickDurationMs: int64) =

    let wheel = HierarchicalWheel(0L)
    let activeTokens = ConcurrentDictionary<string, TimerToken>()
    let startTicks = clock.Now.Ticks
    let syncRoot = Object()

    let timestampToWheelTicks (ts: Timestamp) : int64 =
        let elapsedMs = (ts.Ticks - startTicks) / 10000L
        max 0L (elapsedMs / tickDurationMs)

    member _.Wheel = wheel
    member _.ActiveCount = activeTokens.Count

    /// Schedules a durable timer with a specified duration and firing callback.
    member _.Schedule(timerId: TimerId, wfId: WorkflowId, runId: RunId, duration: Duration, callback: unit -> unit) : TimerToken =
        let now = clock.Now
        let fireAt = now.AddMilliseconds(int64 duration.TotalMilliseconds)
        let schedTicks = timestampToWheelTicks now
        let expTicks = timestampToWheelTicks fireAt

        let wrappedCallback () =
            activeTokens.TryRemove(timerId.Value) |> ignore
            callback ()

        let entry = TimerEntry.Create(timerId, wfId, runId, schedTicks, expTicks, wrappedCallback)
        let token = wheel.Schedule(entry)
        activeTokens.[timerId.Value] <- token
        token

    /// Cancels an active timer by its TimerId handle.
    member _.Cancel(timerId: TimerId) : bool =
        match activeTokens.TryRemove(timerId.Value) with
        | true, token -> token.Cancel()
        | false, _ -> false

    /// Advances the timer queue by a given elapsed time span in milliseconds,
    /// triggering any callbacks whose deadlines have elapsed.
    member this.Advance(elapsedMs: int64) : TimerEntry list =
        lock syncRoot (fun () ->
            clock.AdvanceMilliseconds(elapsedMs) |> ignore
            let currentWheelTicks = timestampToWheelTicks clock.Now
            let expired = wheel.AdvanceTo(currentWheelTicks)
            for entry in expired do
                entry.Callback()
            expired
        )
