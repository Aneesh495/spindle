namespace Spindle.Tests.Queue

open System
open System.Threading.Tasks
open Xunit
open Spindle.Core.Domain
open Spindle.Core.Metadata
open Spindle.Queue

module RingBufferTests =

    [<Fact>]
    let ``RingBuffer enforces power of two capacity`` () =
        Assert.Throws<ArgumentException>(fun () -> RingBuffer<string>(3) |> ignore) |> ignore
        Assert.Throws<ArgumentException>(fun () -> RingBuffer<string>(0) |> ignore) |> ignore
        let valid = RingBuffer<string>(8)
        Assert.Equal(8, valid.Capacity)

    [<Fact>]
    let ``RingBuffer maintains FIFO order and handles saturation`` () =
        let rb = RingBuffer<string>(4)
        Assert.True(rb.IsEmpty)
        Assert.True(rb.TryEnqueue("item-1"))
        Assert.True(rb.TryEnqueue("item-2"))
        Assert.True(rb.TryEnqueue("item-3"))
        Assert.True(rb.TryEnqueue("item-4"))
        Assert.Equal(4, rb.Count)

        // Buffer full: 5th item rejected
        Assert.False(rb.TryEnqueue("item-5"))

        Assert.Equal(Some "item-1", rb.TryDequeue())
        Assert.Equal(Some "item-2", rb.TryDequeue())

        // Now can enqueue again (wraparound)
        Assert.True(rb.TryEnqueue("item-5"))
        Assert.Equal(Some "item-3", rb.TryDequeue())
        Assert.Equal(Some "item-4", rb.TryDequeue())
        Assert.Equal(Some "item-5", rb.TryDequeue())
        Assert.True(rb.IsEmpty)

    [<Fact>]
    let ``RingBuffer supports concurrent multi-producer enqueue`` () =
        let rb = RingBuffer<string>(1024)
        let producerCount = 8
        let itemsPerProducer = 100

        Parallel.For(0, producerCount, fun p ->
            for i = 1 to itemsPerProducer do
                let item = sprintf "p%d-item%d" p i
                while not (rb.TryEnqueue item) do
                    Threading.Thread.Yield() |> ignore
        ) |> ignore

        Assert.Equal(producerCount * itemsPerProducer, rb.Count)
        let dequeued = ResizeArray<string>()
        while not rb.IsEmpty do
            match rb.TryDequeue() with
            | Some it -> dequeued.Add(it)
            | None -> ()

        Assert.Equal(producerCount * itemsPerProducer, dequeued.Count)

module PriorityQueueTests =

    [<Fact>]
    let ``PriorityQueue dequeues higher priority items first`` () =
        let pq = PriorityQueue<string>()
        pq.Enqueue("low", 1)
        pq.Enqueue("critical", 100)
        pq.Enqueue("medium", 10)

        Assert.Equal(Some "critical", pq.TryDequeue())
        Assert.Equal(Some "medium", pq.TryDequeue())
        Assert.Equal(Some "low", pq.TryDequeue())
        Assert.True(pq.IsEmpty)

    [<Fact>]
    let ``PriorityQueue uses FIFO tie breaking for equal priorities`` () =
        let pq = PriorityQueue<string>()
        pq.Enqueue("alpha", 5)
        pq.Enqueue("beta", 5)
        pq.Enqueue("gamma", 5)

        Assert.Equal(Some "alpha", pq.TryDequeue())
        Assert.Equal(Some "beta", pq.TryDequeue())
        Assert.Equal(Some "gamma", pq.TryDequeue())

module RetryEvaluatorTests =

    [<Fact>]
    let ``Exponential backoff scales geometrically and respects ceiling`` () =
        let policy = {
            Strategy = BackoffStrategy.Exponential(Duration.FromMilliseconds 100L, 2.0, Duration.FromMilliseconds 1000L)
            MaximumAttempts = Some 5
            NonRetryableErrors = Set.empty
        }
        let rng _ = 0.0

        let b1 = RetryEvaluator.calculateBackoff policy 1 (Duration.FromMilliseconds 0L) rng
        let b2 = RetryEvaluator.calculateBackoff policy 2 (Duration.FromMilliseconds 0L) rng
        let b3 = RetryEvaluator.calculateBackoff policy 3 (Duration.FromMilliseconds 0L) rng
        let b4 = RetryEvaluator.calculateBackoff policy 4 (Duration.FromMilliseconds 0L) rng
        let b5 = RetryEvaluator.calculateBackoff policy 5 (Duration.FromMilliseconds 0L) rng

        Assert.Equal(100.0, b1.TotalMilliseconds)
        Assert.Equal(200.0, b2.TotalMilliseconds)
        Assert.Equal(400.0, b3.TotalMilliseconds)
        Assert.Equal(800.0, b4.TotalMilliseconds)
        Assert.Equal(1000.0, b5.TotalMilliseconds) // Capped at 1000ms

    [<Fact>]
    let ``DLQ router identifies non-retryable and max-attempt exhausted failures`` () =
        let policy = {
            Strategy = BackoffStrategy.Fixed (Duration.FromSeconds 1L)
            MaximumAttempts = Some 3
            NonRetryableErrors = Set.ofList [ "ValidationError" ]
        }

        Assert.False(RetryEvaluator.shouldRouteToDlq policy "NetworkError" 1)
        Assert.False(RetryEvaluator.shouldRouteToDlq policy "NetworkError" 2)
        Assert.True(RetryEvaluator.shouldRouteToDlq policy "NetworkError" 3)
        Assert.True(RetryEvaluator.shouldRouteToDlq policy "ValidationError" 1)

module LeaseManagerTests =

    [<Fact>]
    let ``LeaseManager assigns increasing fencing tokens and prevents double lease`` () =
        let lm = LeaseManager()
        let actId = ActivityId "act-charge-99"
        let ts = Timestamp.FromUnixTimeMilliseconds 1000L
        let timeout = Duration.FromSeconds 5L

        let leaseRes1 = lm.AcquireLease(actId, "worker-1", timeout, ts)
        Assert.True(leaseRes1 |> Result.isOk)
        let lease1 = leaseRes1 |> Result.defaultWith (fun _ -> failwith "unreachable")
        Assert.Equal(1L, lease1.FencingToken.Value)

        // Second worker attempts while lease is valid: fails!
        let leaseRes2 = lm.AcquireLease(actId, "worker-2", timeout, ts.AddMilliseconds 1000L)
        Assert.True(leaseRes2 |> Result.isError)

    [<Fact>]
    let ``LeaseManager dead-man switch reclaims expired leases`` () =
        let lm = LeaseManager()
        let actId = ActivityId "act-render-video"
        let ts = Timestamp.FromUnixTimeMilliseconds 1000L
        let timeout = Duration.FromSeconds 2L

        let leaseRes = lm.AcquireLease(actId, "worker-alpha", timeout, ts)
        Assert.True(leaseRes |> Result.isOk)

        // Fast forward beyond timeout
        let expiredTime = ts.AddSeconds 3L
        let reclaimed = lm.ReclaimExpired expiredTime
        Assert.Single(reclaimed) |> ignore
        Assert.Equal(actId.Value, reclaimed.[0].ActivityId.Value)

        // Now another worker can acquire it with higher fencing token!
        let leaseRes2 = lm.AcquireLease(actId, "worker-beta", timeout, expiredTime)
        Assert.True(leaseRes2 |> Result.isOk)
        let lease2 = leaseRes2 |> Result.defaultWith (fun _ -> failwith "unreachable")
        Assert.Equal(2L, lease2.FencingToken.Value)

module ShardedDispatcherTests =

    [<Fact>]
    let ``ShardedDispatcher prioritizes critical tasks over normal tasks`` () =
        let lm = LeaseManager()
        let disp = ShardedDispatcher(lm)
        let qName = "payments"
        let ts = Timestamp.FromUnixTimeMilliseconds 1000L

        let taskNormal = {
            ActivityId = ActivityId "act-norm"
            WorkflowId = WorkflowId "wf-norm"
            RunId = RunId (Guid.NewGuid())
            ActivityType = "SendReceipt"
            InputPayload = [||]
            Options = ActivityOptions.Create(qName, Duration.FromSeconds 10L)
            Headers = HeaderMap.empty
            Attempt = 1
            ScheduledAt = ts
        }

        let taskCritical = {
            ActivityId = ActivityId "act-crit"
            WorkflowId = WorkflowId "wf-crit"
            RunId = RunId (Guid.NewGuid())
            ActivityType = "AuthorizeCard"
            InputPayload = [||]
            Options = ActivityOptions.Create(qName, Duration.FromSeconds 10L)
            Headers = HeaderMap.empty
            Attempt = 1
            ScheduledAt = ts
        }

        // Enqueue normal with priority 1, then critical with priority 100
        disp.Enqueue(taskNormal, 1) |> Async.RunSynchronously
        disp.Enqueue(taskCritical, 100) |> Async.RunSynchronously

        let depth = disp.GetDepth(qName) |> Async.RunSynchronously
        Assert.Equal(2, depth)

        // Polling should yield critical first!
        let polled1 = disp.Poll(qName, "worker-1", Duration.FromSeconds 5L, ts) |> Async.RunSynchronously
        Assert.True(polled1.IsSome)
        Assert.Equal("act-crit", polled1.Value.ActivityId.Value)

        let polled2 = disp.Poll(qName, "worker-1", Duration.FromSeconds 5L, ts) |> Async.RunSynchronously
        Assert.True(polled2.IsSome)
        Assert.Equal("act-norm", polled2.Value.ActivityId.Value)
