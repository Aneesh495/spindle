namespace Spindle.Core

open System
open System.Text

/// Core domain identifiers and timestamp primitives for the Spindle durable execution kernel.
module Domain =

    /// Strongly-typed identifier for a logical workflow definition and instance.
    [<Struct>]
    type WorkflowId =
        | WorkflowId of string
        member this.Value = match this with WorkflowId v -> v
        override this.ToString() = this.Value

    /// Strongly-typed identifier for a single execution run of a workflow.
    /// Workflows may have multiple runs across retries or continuous cron executions.
    [<Struct>]
    type RunId =
        | RunId of Guid
        member this.Value = match this with RunId v -> v
        override this.ToString() = this.Value.ToString("D")

    /// Strongly-typed identifier for an activity execution task within a workflow run.
    [<Struct>]
    type ActivityId =
        | ActivityId of string
        member this.Value = match this with ActivityId v -> v
        override this.ToString() = this.Value

    /// Strongly-typed identifier for a durable timer.
    [<Struct>]
    type TimerId =
        | TimerId of string
        member this.Value = match this with TimerId v -> v
        override this.ToString() = this.Value

    /// Strongly-typed identifier for a distributed workflow shard partition.
    [<Struct>]
    type ShardId =
        | ShardId of int
        member this.Value = match this with ShardId v -> v
        override this.ToString() = string this.Value

    /// Strongly-typed identifier for a multi-tenant isolation boundary.
    [<Struct>]
    type TenantId =
        | TenantId of string
        member this.Value = match this with TenantId v -> v
        override this.ToString() = this.Value

    /// Monotonically increasing 64-bit sequence number representing the position in an event journal.
    [<Struct>]
    type SequenceNumber =
        | SequenceNumber of int64
        member this.Value = match this with SequenceNumber v -> v
        static member Zero = SequenceNumber 0L
        static member Initial = SequenceNumber 1L
        member this.Next() = SequenceNumber (this.Value + 1L)
        override this.ToString() = string this.Value

    /// Deterministic virtual or physical timestamp in ticks (100-nanosecond intervals since Unix epoch).
    [<Struct>]
    type Timestamp =
        | Timestamp of int64
        member this.Ticks = match this with Timestamp t -> t
        member this.ToDateTimeOffset() = DateTimeOffset(this.Ticks, TimeSpan.Zero)
        static member FromDateTimeOffset(dto: DateTimeOffset) = Timestamp dto.UtcTicks
        static member FromUnixTimeMilliseconds(ms: int64) = Timestamp (DateTimeOffset.UnixEpoch.UtcTicks + ms * 10000L)
        static member Zero = Timestamp 0L
        member this.AddMilliseconds(ms: int64) = Timestamp (this.Ticks + ms * 10000L)
        member this.AddSeconds(sec: int64) = Timestamp (this.Ticks + sec * 10000000L)
        override this.ToString() = this.ToDateTimeOffset().ToString("O")

    /// Duration span expressed in 100-nanosecond tick intervals.
    [<Struct>]
    type Duration =
        | Duration of int64
        member this.Ticks = match this with Duration d -> d
        member this.TotalMilliseconds = float this.Ticks / 10000.0
        member this.TotalSeconds = float this.Ticks / 10000000.0
        static member Zero = Duration 0L
        static member FromTimeSpan(ts: TimeSpan) = Duration ts.Ticks
        static member FromMilliseconds(ms: int64) = Duration (ms * 10000L)
        static member FromSeconds(sec: int64) = Duration (sec * 10000000L)
        member this.ToTimeSpan() = TimeSpan.FromTicks(this.Ticks)
        override this.ToString() = sprintf "%.2fms" this.TotalMilliseconds

    /// Fencing token issued during shard lease acquisition to prevent split-brain dual writing.
    [<Struct>]
    type FencingToken =
        | FencingToken of int64
        member this.Value = match this with FencingToken v -> v
        member this.Next() = FencingToken (this.Value + 1L)
        override this.ToString() = sprintf "fencing#%d" this.Value

    /// Cryptographic 32-bit checksum or hash tag.
    [<Struct>]
    type HashTag =
        | HashTag of uint32
        member this.Value = match this with HashTag v -> v
        override this.ToString() = sprintf "0x%08X" this.Value

    /// Murmur3 32-bit hash implementation for deterministic partitioning and shard mapping.
    module Murmur3 =
        let hashString (seed: uint32) (input: string) : uint32 =
            if String.IsNullOrEmpty(input) then seed
            else
                let bytes = Encoding.UTF8.GetBytes(input)
                let length = bytes.Length
                let nblocks = length / 4
                let mutable h1 = seed
                let c1 = 0xcc9e2d51u
                let c2 = 0x1b873593u

                for i = 0 to nblocks - 1 do
                    let kIndex = i * 4
                    let mutable k1 =
                        uint32 bytes.[kIndex] |||
                        (uint32 bytes.[kIndex + 1] <<< 8) |||
                        (uint32 bytes.[kIndex + 2] <<< 16) |||
                        (uint32 bytes.[kIndex + 3] <<< 24)

                    k1 <- k1 * c1
                    k1 <- (k1 <<< 15) ||| (k1 >>> 17)
                    k1 <- k1 * c2

                    h1 <- h1 ^^^ k1
                    h1 <- (h1 <<< 13) ||| (h1 >>> 19)
                    h1 <- h1 * 5u + 0xe6546b64u

                let tailIndex = nblocks * 4
                let remaining = length - tailIndex
                let mutable kTail = 0u
                if remaining >= 3 then kTail <- kTail ^^^ (uint32 bytes.[tailIndex + 2] <<< 16)
                if remaining >= 2 then kTail <- kTail ^^^ (uint32 bytes.[tailIndex + 1] <<< 8)
                if remaining >= 1 then
                    kTail <- kTail ^^^ uint32 bytes.[tailIndex]
                    kTail <- kTail * c1
                    kTail <- (kTail <<< 15) ||| (kTail >>> 17)
                    kTail <- kTail * c2
                    h1 <- h1 ^^^ kTail

                h1 <- h1 ^^^ uint32 length
                h1 <- h1 ^^^ (h1 >>> 16)
                h1 <- h1 * 0x85ebca6bu
                h1 <- h1 ^^^ (h1 >>> 13)
                h1 <- h1 * 0xc2b2ae35u
                h1 <- h1 ^^^ (h1 >>> 16)
                h1
