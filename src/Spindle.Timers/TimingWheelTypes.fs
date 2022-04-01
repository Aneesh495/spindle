namespace Spindle.Timers

open System
open Spindle.Core.Domain

/// Data types, constants, and token abstractions for the hierarchical timing wheel.
module TimingWheelTypes =

    /// Wheel architecture constants (Varghese and Lauck hierarchical scheme).
    let [<Literal>] Level0Bits = 8
    let [<Literal>] Level0Size = 256 // 1 <<< 8
    let [<Literal>] Level0Mask = 255L

    let [<Literal>] LevelNBits = 6
    let [<Literal>] LevelNSize = 64  // 1 <<< 6
    let [<Literal>] LevelNMask = 63L

    let [<Literal>] WheelLevels = 5
    let [<Literal>] DefaultTickDurationMs = 10L

    /// A single scheduled timer entry in the wheel.
    type TimerEntry = {
        TimerId: TimerId
        WorkflowId: WorkflowId
        RunId: RunId
        ScheduledAtTicks: int64
        ExpirationTicks: int64
        mutable IsCancelled: bool
        Callback: unit -> unit
    } with
        static member Create(tId: TimerId, wfId: WorkflowId, runId: RunId, schedTicks: int64, expTicks: int64, callback: unit -> unit) = {
            TimerId = tId
            WorkflowId = wfId
            RunId = runId
            ScheduledAtTicks = schedTicks
            ExpirationTicks = expTicks
            IsCancelled = false
            Callback = callback
        }

    /// Cancellation handle returned when a timer is scheduled.
    [<Struct>]
    type TimerToken = {
        TimerId: TimerId
        Entry: TimerEntry
    } with
        member this.Cancel() : bool =
            if not this.Entry.IsCancelled then
                this.Entry.IsCancelled <- true
                true
            else
                false

    /// Diagnostic statistics capturing wheel occupancy across tiers.
    type WheelMetrics = {
        CurrentTick: int64
        ActiveTimers: int
        Level0Count: int
        Level1Count: int
        Level2Count: int
        Level3Count: int
        Level4Count: int
    }
