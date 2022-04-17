namespace Spindle.Queue

open System
open System.Threading

/// High-throughput lock-free bounded Multi-Producer Single-Consumer (MPSC) ring buffer.
type RingBuffer<'T when 'T: null>(powerOfTwoCapacity: int) =

    let capacity =
        if powerOfTwoCapacity < 2 || (powerOfTwoCapacity &&& (powerOfTwoCapacity - 1)) <> 0 then
            invalidArg "powerOfTwoCapacity" "Capacity must be a power of two greater than or equal to 2"
        powerOfTwoCapacity

    let mask = int64 (capacity - 1)
    let buffer = Array.zeroCreate<'T> capacity

    let mutable head = 0L // Read pointer (Single Consumer)
    let mutable tail = 0L // Write pointer (Multi Producer)

    member _.Capacity = capacity

    member _.Count : int =
        let t = Volatile.Read(&tail)
        let h = Volatile.Read(&head)
        int (max 0L (t - h))

    member this.IsEmpty : bool =
        this.Count = 0

    /// Attempts to enqueue an item into the ring buffer.
    /// Returns false if the buffer is saturated.
    member _.TryEnqueue(item: 'T) : bool =
        if isNull (box item) then
            invalidArg "item" "Item cannot be null"
        let rec loop () =
            let currentTail = Volatile.Read(&tail)
            let currentHead = Volatile.Read(&head)
            if currentTail - currentHead >= int64 capacity then
                false
            else
                let nextTail = currentTail + 1L
                if Interlocked.CompareExchange(&tail, nextTail, currentTail) = currentTail then
                    let slot = int (currentTail &&& mask)
                    Volatile.Write(&buffer.[slot], item)
                    true
                else
                    loop ()
        loop ()

    /// Attempts to dequeue an item from the ring buffer.
    /// Returns None if the buffer is empty.
    member _.TryDequeue() : 'T option =
        let currentHead = Volatile.Read(&head)
        let currentTail = Volatile.Read(&tail)
        if currentHead >= currentTail then
            None
        else
            let slot = int (currentHead &&& mask)
            let item = Volatile.Read(&buffer.[slot])
            if isNull (box item) then
                // Producer claimed slot but has not finished writing yet
                None
            else
                buffer.[slot] <- null
                Volatile.Write(&head, currentHead + 1L)
                Some item

    /// Clears all remaining items from the ring buffer.
    member this.Clear() =
        while this.TryDequeue().IsSome do ()
