namespace Spindle.Tests.Timers

open System
open Xunit
open Spindle.Core.Domain
open Spindle.Timers.TimingWheelTypes
open Spindle.Timers

module WheelBasicTests =

    [<Fact>]
    let ``Single timer fires at exact expiration tick`` () =
        let wheel = HierarchicalWheel(0L)
        let mutable fired = false
        let entry = TimerEntry.Create(
            TimerId "t-1",
            WorkflowId "wf-1",
            RunId (Guid.NewGuid()),
            0L,
            5L,
            fun () -> fired <- true
        )

        wheel.Schedule(entry) |> ignore
        Assert.Equal(1, wheel.ActiveCount)

        // Advance 5 ticks: slots 0, 1, 2, 3, 4
        for _ = 1 to 5 do
            let expired = wheel.Tick()
            Assert.Empty(expired)

        Assert.False(fired)

        // 6th call to Tick: processes slot 5!
        let expired5 = wheel.Tick()
        Assert.Single(expired5) |> ignore
        Assert.Equal("t-1", expired5.[0].TimerId.Value)

    [<Fact>]
    let ``Multiple timers in same slot fire together`` () =
        let wheel = HierarchicalWheel(10L)
        let mutable count = 0
        let cb () = count <- count + 1

        for i = 1 to 5 do
            let entry = TimerEntry.Create(TimerId (sprintf "t-%d" i), WorkflowId "wf", RunId (Guid.NewGuid()), 10L, 15L, cb)
            wheel.Schedule(entry) |> ignore

        let expired = wheel.AdvanceTo(15L)
        Assert.Equal(5, expired.Length)
        for e in expired do e.Callback()
        Assert.Equal(5, count)

module CascadeTests =

    [<Fact>]
    let ``Timer scheduled in Level 1 cascades to Level 0 and fires on exact tick`` () =
        let wheel = HierarchicalWheel(0L)
        let targetTick = 300L // Exceeds Level 0 (256 slots), places into Level 1
        let mutable fired = false
        let entry = TimerEntry.Create(
            TimerId "t-cascade",
            WorkflowId "wf-casc",
            RunId (Guid.NewGuid()),
            0L,
            targetTick,
            fun () -> fired <- true
        )

        wheel.Schedule(entry) |> ignore
        let metrics = wheel.GetMetrics()
        Assert.Equal(1, metrics.Level1Count)
        Assert.Equal(0, metrics.Level0Count)

        // Advance to 299: should cascade at 256, but not fire until 300
        let expBefore = wheel.AdvanceTo(299L)
        Assert.Empty(expBefore)

        let exp300 = wheel.Tick()
        Assert.Single(exp300) |> ignore
        Assert.Equal("t-cascade", exp300.[0].TimerId.Value)

module CancellationTests =

    [<Fact>]
    let ``Cancelled timer does not fire upon expiration`` () =
        let wheel = HierarchicalWheel(0L)
        let mutable fired = false
        let entry = TimerEntry.Create(
            TimerId "t-cancel",
            WorkflowId "wf",
            RunId (Guid.NewGuid()),
            0L,
            10L,
            fun () -> fired <- true
        )

        let token = wheel.Schedule(entry)
        let cancelledFirst = token.Cancel()
        let cancelledSecond = token.Cancel()

        Assert.True(cancelledFirst)
        Assert.False(cancelledSecond)

        let expired = wheel.AdvanceTo(10L)
        Assert.Empty(expired)
        Assert.False(fired)

module DurableTimerQueueTests =

    [<Fact>]
    let ``DurableTimerQueue schedules, advances, and fires callbacks`` () =
        let startTs = Timestamp.FromUnixTimeMilliseconds 1000L
        let clock = VirtualClock(startTs)
        let queue = DurableTimerQueue(clock, 10L) // 10ms per tick

        let mutable callbackFired = false
        let token = queue.Schedule(
            TimerId "t-q",
            WorkflowId "wf-q",
            RunId (Guid.NewGuid()),
            Duration.FromMilliseconds 500L,
            fun () -> callbackFired <- true
        )

        Assert.Equal(1, queue.ActiveCount)

        // Advance 200ms: should not fire
        queue.Advance(200L) |> ignore
        Assert.False(callbackFired)

        // Advance another 350ms: total 550ms, deadline elapsed!
        queue.Advance(350L) |> ignore
        Assert.True(callbackFired)
        Assert.Equal(0, queue.ActiveCount)

    [<Fact>]
    let ``DurableTimerQueue handles high volume of staggered timers`` () =
        let clock = VirtualClock.CreateSystemUtc()
        let queue = DurableTimerQueue(clock, 10L)
        let count = 500
        let mutable firedCounter = 0
        let syncLock = Object()

        for i = 1 to count do
            let durMs = int64 (i * 10)
            queue.Schedule(
                TimerId (sprintf "bulk-%d" i),
                WorkflowId "wf-bulk",
                RunId (Guid.NewGuid()),
                Duration.FromMilliseconds durMs,
                fun () -> lock syncLock (fun () -> firedCounter <- firedCounter + 1)
            ) |> ignore

        Assert.Equal(count, queue.ActiveCount)

        // Advance time through all deadlines
        queue.Advance(int64 (count * 10 + 50)) |> ignore
        Assert.Equal(count, firedCounter)
        Assert.Equal(0, queue.ActiveCount)
