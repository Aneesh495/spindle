namespace Spindle.Timers

open System
open System.Collections.Generic
open Spindle.Core.Domain
open Spindle.Timers.TimingWheelTypes

/// High-performance 5-level Varghese-Lauck hierarchical hashed timing wheel.
type HierarchicalWheel(startTick: int64) =

    let mutable currentTick = startTick
    let syncRoot = Object()

    // Wheel tier slots
    let level0 = Array.init Level0Size (fun _ -> ResizeArray<TimerEntry>())
    let level1 = Array.init LevelNSize (fun _ -> ResizeArray<TimerEntry>())
    let level2 = Array.init LevelNSize (fun _ -> ResizeArray<TimerEntry>())
    let level3 = Array.init LevelNSize (fun _ -> ResizeArray<TimerEntry>())
    let level4 = Array.init LevelNSize (fun _ -> ResizeArray<TimerEntry>())

    let mutable activeTimerCount = 0

    // Multi-tier tick spans
    let spanLevel0 = 256L                     // 1 <<< 8
    let spanLevel1 = 256L * 64L               // 16,384 ticks
    let spanLevel2 = 256L * 64L * 64L          // 1,048,576 ticks
    let spanLevel3 = 256L * 64L * 64L * 64L     // 67,108,864 ticks

    let insertInternal (entry: TimerEntry) =
        let delta = entry.ExpirationTicks - currentTick
        if delta < spanLevel0 then
            let slot = int (entry.ExpirationTicks &&& Level0Mask)
            level0.[slot].Add(entry)
        elif delta < spanLevel1 then
            let slot = int ((entry.ExpirationTicks >>> Level0Bits) &&& LevelNMask)
            level1.[slot].Add(entry)
        elif delta < spanLevel2 then
            let slot = int ((entry.ExpirationTicks >>> (Level0Bits + LevelNBits)) &&& LevelNMask)
            level2.[slot].Add(entry)
        elif delta < spanLevel3 then
            let slot = int ((entry.ExpirationTicks >>> (Level0Bits + 2 * LevelNBits)) &&& LevelNMask)
            level3.[slot].Add(entry)
        else
            let slot = int ((entry.ExpirationTicks >>> (Level0Bits + 3 * LevelNBits)) &&& LevelNMask)
            level4.[slot].Add(entry)

    let cascade (slotEntries: ResizeArray<TimerEntry>) =
        let items = slotEntries.ToArray()
        slotEntries.Clear()
        for item in items do
            if not item.IsCancelled then
                insertInternal item
            else
                activeTimerCount <- activeTimerCount - 1

    member _.CurrentTick = lock syncRoot (fun () -> currentTick)
    member _.ActiveCount = lock syncRoot (fun () -> activeTimerCount)

    /// Schedules a timer entry into the appropriate wheel level based on its expiration delta.
    member _.Schedule(entry: TimerEntry) : TimerToken =
        lock syncRoot (fun () ->
            if entry.ExpirationTicks <= currentTick then
                // Already expired, schedule into immediate next slot
                let immediateEntry = { entry with ExpirationTicks = currentTick }
                level0.[int (currentTick &&& Level0Mask)].Add(immediateEntry)
            else
                insertInternal entry
            activeTimerCount <- activeTimerCount + 1
            { TimerId = entry.TimerId; Entry = entry }
        )

    /// Advances the wheel by a single tick, cascades parent wheel tiers as revolutions occur,
    /// and collects all uncancelled expired timer entries.
    member _.Tick() : TimerEntry list =
        lock syncRoot (fun () ->
            let expiredList = ResizeArray<TimerEntry>()
            let slotIdx = int (currentTick &&& Level0Mask)

            // Check if Level 0 made a full turn, requiring cascade from higher levels BEFORE collecting Level 0
            if slotIdx = 0 && currentTick > startTick then
                let l1Slot = int ((currentTick >>> Level0Bits) &&& LevelNMask)
                cascade level1.[l1Slot]

                if l1Slot = 0 then
                    let l2Slot = int ((currentTick >>> (Level0Bits + LevelNBits)) &&& LevelNMask)
                    cascade level2.[l2Slot]

                    if l2Slot = 0 then
                        let l3Slot = int ((currentTick >>> (Level0Bits + 2 * LevelNBits)) &&& LevelNMask)
                        cascade level3.[l3Slot]

                        if l3Slot = 0 then
                            let l4Slot = int ((currentTick >>> (Level0Bits + 3 * LevelNBits)) &&& LevelNMask)
                            cascade level4.[l4Slot]

            // Collect expired timers from Level 0 for the current tick
            let slotEntries = level0.[slotIdx]
            if slotEntries.Count > 0 then
                for i = 0 to slotEntries.Count - 1 do
                    let entry = slotEntries.[i]
                    if not entry.IsCancelled then
                        expiredList.Add(entry)
                    activeTimerCount <- activeTimerCount - 1
                slotEntries.Clear()

            currentTick <- currentTick + 1L
            Seq.toList expiredList
        )

    /// Advances the wheel up to targetTick, collecting all expired timers across steps.
    member this.AdvanceTo(targetTick: int64) : TimerEntry list =
        lock syncRoot (fun () ->
            let allExpired = ResizeArray<TimerEntry>()
            while currentTick <= targetTick do
                let expired = this.Tick()
                allExpired.AddRange(expired)
            Seq.toList allExpired
        )

    /// Diagnostic inspection of wheel occupancy across all 5 tiers.
    member _.GetMetrics() : WheelMetrics =
        lock syncRoot (fun () ->
            let sumSlots (slots: ResizeArray<TimerEntry>[]) =
                slots |> Array.sumBy (fun s -> s.Count)

            {
                CurrentTick = currentTick
                ActiveTimers = activeTimerCount
                Level0Count = sumSlots level0
                Level1Count = sumSlots level1
                Level2Count = sumSlots level2
                Level3Count = sumSlots level3
                Level4Count = sumSlots level4
            }
        )
