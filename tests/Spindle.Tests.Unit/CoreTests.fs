namespace Spindle.Tests.Unit

open System
open System.Text
open Xunit
open Spindle.Core.Domain
open Spindle.Core.Metadata
open Spindle.Core.Commands
open Spindle.Core.Events
open Spindle.Core.Codecs
open Spindle.Core.Invariants

module DomainTests =

    [<Fact>]
    let ``SequenceNumber increments monotonically`` () =
        let s0 = SequenceNumber.Zero
        let s1 = s0.Next()
        let s2 = s1.Next()
        Assert.Equal(0L, s0.Value)
        Assert.Equal(1L, s1.Value)
        Assert.Equal(2L, s2.Value)
        Assert.True(s2.Value > s1.Value)

    [<Fact>]
    let ``Timestamp converts accurately to and from DateTimeOffset`` () =
        let now = DateTimeOffset(2026, 10, 22, 12, 0, 0, TimeSpan.Zero)
        let ts = Timestamp.FromDateTimeOffset now
        let roundTripped = ts.ToDateTimeOffset()
        Assert.Equal(now, roundTripped)

    [<Fact>]
    let ``Timestamp addition preserves nanosecond tick resolution`` () =
        let baseTs = Timestamp.FromUnixTimeMilliseconds(1000L)
        let advanced = baseTs.AddMilliseconds(500L)
        let diffMs = (advanced.Ticks - baseTs.Ticks) / 10000L
        Assert.Equal(500L, diffMs)

    [<Fact>]
    let ``Duration calculations are consistent`` () =
        let d = Duration.FromMilliseconds(2500L)
        Assert.Equal(2500.0, d.TotalMilliseconds)
        Assert.Equal(2.5, d.TotalSeconds)
        let ts = d.ToTimeSpan()
        Assert.Equal(TimeSpan.FromMilliseconds(2500.0), ts)

    [<Fact>]
    let ``Murmur3 hash produces deterministic outputs across invocations`` () =
        let key = "spindle-workflow-partition-alpha"
        let hash1 = Murmur3.hashString 42u key
        let hash2 = Murmur3.hashString 42u key
        Assert.Equal(hash1, hash2)
        Assert.NotEqual(0u, hash1)

    [<Fact>]
    let ``Murmur3 empty string returns provided seed`` () =
        let seed = 1337u
        let hash = Murmur3.hashString seed ""
        Assert.Equal(seed, hash)

    [<Fact>]
    let ``Murmur3 avalanche effect distributes distinct keys`` () =
        let h1 = Murmur3.hashString 0u "test_workflow_key_a"
        let h2 = Murmur3.hashString 0u "test_workflow_key_b"
        Assert.NotEqual(h1, h2)

module MetadataTests =

    [<Fact>]
    let ``ExecutionStatus correctly distinguishes terminal states`` () =
        Assert.False(ExecutionStatus.Pending.IsTerminal)
        Assert.False(ExecutionStatus.Running.IsTerminal)
        Assert.False(ExecutionStatus.Compensating.IsTerminal)
        Assert.True(ExecutionStatus.Completed.IsTerminal)
        Assert.True(ExecutionStatus.Failed.IsTerminal)
        Assert.True(ExecutionStatus.Terminated.IsTerminal)
        Assert.True(ExecutionStatus.TimedOut.IsTerminal)
        Assert.True(ExecutionStatus.Compensated.IsTerminal)

    [<Fact>]
    let ``RetryPolicy respects maximum attempts`` () =
        let policy = RetryPolicy.WithMaxAttempts(3, RetryPolicy.Default)
        Assert.True(policy.IsRetryable("System.TimeoutException", 1))
        Assert.True(policy.IsRetryable("System.TimeoutException", 2))
        Assert.False(policy.IsRetryable("System.TimeoutException", 3))

    [<Fact>]
    let ``RetryPolicy excludes designated non-retryable errors`` () =
        let policy = {
            RetryPolicy.Default with
                NonRetryableErrors = Set.ofList [ "Spindle.ValidationError"; "System.ArgumentNullException" ]
        }
        Assert.False(policy.IsRetryable("Spindle.ValidationError", 1))
        Assert.False(policy.IsRetryable("System.ArgumentNullException", 1))
        Assert.True(policy.IsRetryable("System.Net.Http.HttpRequestException", 1))

module CommandTests =

    [<Fact>]
    let ``WorkflowCommand accurately tags terminal command variants`` () =
        let comp = WorkflowCommand.CompleteWorkflow [| 1uy; 2uy |]
        let fail = WorkflowCommand.FailWorkflow (FailureInfo.Create("Error", "failed"))
        let act = WorkflowCommand.ScheduleActivity(
            ActivityId "act1",
            "SendEmail",
            [||],
            ActivityOptions.Create("default", Duration.FromSeconds 30L),
            HeaderMap.empty
        )
        Assert.True(WorkflowCommand.isTerminal comp)
        Assert.True(WorkflowCommand.isTerminal fail)
        Assert.False(WorkflowCommand.isTerminal act)
        Assert.Equal("ScheduleActivity", WorkflowCommand.commandType act)

module EventTests =

    [<Fact>]
    let ``JournalRecord properly initializes and computes hash chain`` () =
        let ts = Timestamp.FromUnixTimeMilliseconds(1000L)
        let evt = WorkflowEvent.WorkflowExecutionStarted("OrderProcess", [||], "main", None, None, HeaderMap.empty, 1)
        let rec1 = JournalRecord.Create(SequenceNumber.Initial, ts, evt, 0u)
        Assert.Equal(SequenceNumber.Initial, rec1.SequenceNumber)
        Assert.Equal(0u, rec1.PrevHash)
        Assert.NotEqual(0u, rec1.RecordHash)

    [<Fact>]
    let ``History appends maintain chronological ordering and hash continuity`` () =
        let wfId = WorkflowId "order-101"
        let runId = RunId (Guid.NewGuid())
        let h0 = History.Empty(wfId, runId)
        let ts1 = Timestamp.FromUnixTimeMilliseconds(1000L)
        let ts2 = Timestamp.FromUnixTimeMilliseconds(2000L)

        let h1 = h0.Append(WorkflowEvent.WorkflowExecutionStarted("OrderProcess", [||], "main", None, None, HeaderMap.empty, 1), ts1)
        let h2 = h1.Append(WorkflowEvent.ActivityTaskScheduled(ActivityId "act-1", "ChargeCard", [||], ActivityOptions.Create("main", Duration.FromSeconds 10L), HeaderMap.empty), ts2)

        Assert.Equal(2, h2.Length)
        let records = h2.Chronological
        Assert.Equal(SequenceNumber.Initial, records.[0].SequenceNumber)
        Assert.Equal(SequenceNumber 2L, records.[1].SequenceNumber)
        Assert.Equal(records.[0].RecordHash, records.[1].PrevHash)

module CodecTests =

    [<Fact>]
    let ``Binary codec roundtrips journal records without data degradation`` () =
        let ts = Timestamp.FromUnixTimeMilliseconds(1500L)
        let payload = Encoding.UTF8.GetBytes("{\"amount\": 99.50}")
        let evt = WorkflowEvent.WorkflowExecutionCompleted(payload)
        let original = JournalRecord.Create(SequenceNumber 42L, ts, evt, 0x12345678u)

        let encoded = Binary.encodeRecord original
        let decodedRes = Binary.decodeRecord encoded

        match decodedRes with
        | Error err -> Assert.Fail(sprintf "Decoding failed: %s" err)
        | Ok decoded ->
            Assert.Equal(original.SequenceNumber, decoded.SequenceNumber)
            Assert.Equal(original.Timestamp.Ticks, decoded.Timestamp.Ticks)
            Assert.Equal(original.PrevHash, decoded.PrevHash)
            Assert.Equal(original.RecordHash, decoded.RecordHash)
            match decoded.Event with
            | WorkflowEvent.WorkflowExecutionCompleted resBytes ->
                Assert.Equal<byte>(payload, resBytes)
            | other ->
                Assert.Fail(sprintf "Unexpected event variant decoded: %A" other)

    [<Fact>]
    let ``Binary codec rejects corrupted frames with altered bytes`` () =
        let ts = Timestamp.FromUnixTimeMilliseconds(1500L)
        let evt = WorkflowEvent.WorkflowExecutionCompleted([| 1uy; 2uy; 3uy |])
        let original = JournalRecord.Create(SequenceNumber 1L, ts, evt, 0u)
        let encoded = Binary.encodeRecord original

        // Deliberately tamper with a byte in the payload
        let tampered = Array.copy encoded
        tampered.[10] <- tampered.[10] ^^^ 0xFFuy

        let decodedRes = Binary.decodeRecord tampered
        match decodedRes with
        | Error msg ->
            Assert.Contains("Checksum mismatch", msg)
        | Ok _ ->
            Assert.Fail("Codec should have rejected tampered binary payload")

module InvariantTests =

    [<Fact>]
    let ``Verifier approves compliant event sequence`` () =
        let wfId = WorkflowId "valid-wf"
        let runId = RunId (Guid.NewGuid())
        let ts = Timestamp.FromUnixTimeMilliseconds(1000L)
        let actId = ActivityId "act-charge"

        let hist =
            History.Empty(wfId, runId)
                .Append(WorkflowEvent.WorkflowExecutionStarted("Workflow", [||], "main", None, None, HeaderMap.empty, 1), ts)
                .Append(WorkflowEvent.ActivityTaskScheduled(actId, "Charge", [||], ActivityOptions.Create("main", Duration.FromSeconds 5L), HeaderMap.empty), ts.AddMilliseconds 100L)
                .Append(WorkflowEvent.ActivityTaskCompleted(actId, [| 1uy |]), ts.AddMilliseconds 200L)
                .Append(WorkflowEvent.WorkflowExecutionCompleted([||]), ts.AddMilliseconds 300L)

        let auditResult = Verifier.verifyAll hist.Chronological
        Assert.True(auditResult |> Result.isOk)

    [<Fact>]
    let ``Verifier detects sequence number skips`` () =
        let ts = Timestamp.FromUnixTimeMilliseconds(1000L)
        let r1 = JournalRecord.Create(SequenceNumber 1L, ts, WorkflowEvent.WorkflowExecutionStarted("Wf", [||], "q", None, None, HeaderMap.empty, 1), 0u)
        let r2 = JournalRecord.Create(SequenceNumber 3L, ts, WorkflowEvent.WorkflowExecutionCompleted([||]), r1.RecordHash)

        let res = Verifier.verifySequenceMonotonicity [ r1; r2 ]
        match res with
        | Error v ->
            Assert.Equal("SequenceMonotonicity", v.Rule)
            Assert.Equal("2", v.Expected)
            Assert.Equal("3", v.Actual)
        | Ok () ->
            Assert.Fail("Verifier should flag non-sequential sequence numbers")

    [<Fact>]
    let ``Verifier flags activities completed without prior schedule event`` () =
        let ts = Timestamp.FromUnixTimeMilliseconds(1000L)
        let actId = ActivityId "unregistered-activity"
        let r1 = JournalRecord.Create(SequenceNumber 1L, ts, WorkflowEvent.WorkflowExecutionStarted("Wf", [||], "q", None, None, HeaderMap.empty, 1), 0u)
        let r2 = JournalRecord.Create(SequenceNumber 2L, ts, WorkflowEvent.ActivityTaskCompleted(actId, [||]), r1.RecordHash)

        let res = Verifier.verifyCausalDependencies [ r1; r2 ]
        match res with
        | Error v ->
            Assert.Equal("ActivityCausality", v.Rule)
        | Ok () ->
            Assert.Fail("Verifier should flag unparented activity completion")
