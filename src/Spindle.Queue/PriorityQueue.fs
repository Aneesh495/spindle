namespace Spindle.Queue

open System
open System.Collections.Generic

/// Prioritized task item with FIFO tie-breaking sequence.
[<CLIMutable>]
type PrioritizedTask<'T> = {
    Priority: int
    Sequence: int64
    Item: 'T
} with
    static member Create(prio: int, seq: int64, item: 'T) = {
        Priority = prio
        Sequence = seq
        Item = item
    }

/// Thread-safe min-heap/max-heap priority queue with deterministic FIFO tie-breaking.
type PriorityQueue<'T>() =

    let heap = ResizeArray<PrioritizedTask<'T>>()
    let syncRoot = Object()
    let mutable sequenceCounter = 0L

    let compareTasks (a: PrioritizedTask<'T>) (b: PrioritizedTask<'T>) : int =
        // Higher priority first; if equal, lower sequence first (FIFO)
        if a.Priority <> b.Priority then
            b.Priority.CompareTo(a.Priority)
        else
            a.Sequence.CompareTo(b.Sequence)

    let swap i j =
        let temp = heap.[i]
        heap.[i] <- heap.[j]
        heap.[j] <- temp

    let rec siftUp i =
        if i > 0 then
            let parent = (i - 1) / 2
            if compareTasks heap.[i] heap.[parent] < 0 then
                swap i parent
                siftUp parent

    let rec siftDown i =
        let left = 2 * i + 1
        let right = 2 * i + 2
        let mutable smallest = i

        if left < heap.Count && compareTasks heap.[left] heap.[smallest] < 0 then
            smallest <- left
        if right < heap.Count && compareTasks heap.[right] heap.[smallest] < 0 then
            smallest <- right

        if smallest <> i then
            swap i smallest
            siftDown smallest

    member _.Count = lock syncRoot (fun () -> heap.Count)
    member _.IsEmpty = lock syncRoot (fun () -> heap.Count = 0)

    /// Enqueues an item with an integer priority value.
    member _.Enqueue(item: 'T, priority: int) =
        lock syncRoot (fun () ->
            sequenceCounter <- sequenceCounter + 1L
            let task = PrioritizedTask<'T>.Create(priority, sequenceCounter, item)
            heap.Add(task)
            siftUp (heap.Count - 1)
        )

    /// Dequeues the highest priority item, using FIFO ordering for tie-breaking.
    member _.TryDequeue() : 'T option =
        lock syncRoot (fun () ->
            if heap.Count = 0 then None
            else
                let root = heap.[0].Item
                let lastIdx = heap.Count - 1
                heap.[0] <- heap.[lastIdx]
                heap.RemoveAt(lastIdx)
                if heap.Count > 0 then
                    siftDown 0
                Some root
        )

    /// Peeks at the highest priority item without removing it.
    member _.TryPeek() : 'T option =
        lock syncRoot (fun () ->
            if heap.Count = 0 then None
            else Some heap.[0].Item
        )

    /// Clears all items from the priority queue.
    member _.Clear() =
        lock syncRoot (fun () -> heap.Clear())
