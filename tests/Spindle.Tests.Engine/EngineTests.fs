namespace Spindle.Tests.Engine

open System
open System.Text
open Xunit
open Spindle.Core.Domain
open Spindle.Core.Metadata
open Spindle.Core.Commands
open Spindle.Core.Events
open Spindle.Core.Codecs
open Spindle.Engine.WorkflowContext
open Spindle.Engine.WorkflowBuilder
open Spindle.Engine.StateMachine
open Spindle.Engine.Determinism
open Spindle.Engine.WorkflowInstance
open Spindle.Engine.Interceptors

module BuilderTests =

    [<Fact>]
    let ``Simple workflow returns constant result immediately`` () =
        let myWorkflow () = workflow {
            return 42
        }

        let ctx = ExecutionContext.Create(WorkflowId "wf-simple", RunId (Guid.NewGuid()), History.Empty(WorkflowId "wf-simple", RunId (Guid.NewGuid())))
        let step = (myWorkflow ()) ctx

        match step with
        | WorkflowStep.Done(v, _) -> Assert.Equal(42, v)
        | other -> Assert.Fail(sprintf "Expected Done, got %A" other)

    [<Fact>]
    let ``Workflow yields activity command when not yet in history`` () =
        let activityWorkflow () = workflow {
            let! res = Workflow.activity<string, string> "FetchCustomer" "cust-123" None
            return sprintf "Customer: %s" res
        }

        let wfId = WorkflowId "wf-act"
        let runId = RunId (Guid.NewGuid())
        let ctx = ExecutionContext.Create(wfId, runId, History.Empty(wfId, runId))
        let step = (activityWorkflow ()) ctx

        match step with
        | WorkflowStep.Yield(cmd, cont) ->
            match cmd with
            | WorkflowCommand.ScheduleActivity(actId, actType, inputBytes, _, _) ->
                Assert.Equal("FetchCustomer", actType)
                let input = Json.deserializeFromBytes<string>(inputBytes)
                Assert.Equal("cust-123", input)
            | other -> Assert.Fail(sprintf "Expected ScheduleActivity command, got %A" other)
        | other -> Assert.Fail(sprintf "Expected Yield step, got %A" other)

    [<Fact>]
    let ``Workflow resumes when activity completion is in context`` () =
        let activityWorkflow () = workflow {
            let! res = Workflow.activity<string, string> "FetchCustomer" "cust-123" None
            return sprintf "Customer: %s" res
        }

        let wfId = WorkflowId "wf-act-res"
        let runId = RunId (Guid.NewGuid())
        let actId = ActivityId "act-1"
        let resultPayload = Json.serializeToBytes "Alice Wonderland"
        let ts = Timestamp.FromUnixTimeMilliseconds 1000L

        let history =
            History.Empty(wfId, runId)
                .Append(WorkflowEvent.WorkflowExecutionStarted("CustomerFlow", [||], "main", None, None, HeaderMap.empty, 1), ts)
                .Append(WorkflowEvent.ActivityTaskScheduled(actId, "FetchCustomer", [||], ActivityOptions.Create("main", Duration.FromSeconds 10L), HeaderMap.empty), ts.AddMilliseconds 50L)
                .Append(WorkflowEvent.ActivityTaskCompleted(actId, resultPayload), ts.AddMilliseconds 150L)

        let ctx = ExecutionContext.Create(wfId, runId, history)
        let step = (activityWorkflow ()) ctx

        match step with
        | WorkflowStep.Done(finalStr, _) ->
            Assert.Equal("Customer: Alice Wonderland", finalStr)
        | other -> Assert.Fail(sprintf "Expected Done step upon replay, got %A" other)

    [<Fact>]
    let ``Workflow handles durable sleep timer yields`` () =
        let timerWorkflow () = workflow {
            do! Workflow.sleep (Duration.FromSeconds 5L)
            return "Awake"
        }

        let wfId = WorkflowId "wf-timer"
        let runId = RunId (Guid.NewGuid())
        let ctx = ExecutionContext.Create(wfId, runId, History.Empty(wfId, runId))
        let step = (timerWorkflow ()) ctx

        match step with
        | WorkflowStep.Yield(WorkflowCommand.StartTimer(tId, dur, _), _) ->
            Assert.Equal(5.0, dur.TotalSeconds)
        | other -> Assert.Fail(sprintf "Expected StartTimer yield, got %A" other)

    [<Fact>]
    let ``Workflow generates deterministic GUIDs across runs`` () =
        let guidWorkflow () = workflow {
            let! g1 = Workflow.newGuid
            let! g2 = Workflow.newGuid
            return g1, g2
        }

        let runId = RunId (Guid.Parse("11111111-2222-3333-4444-555555555555"))
        let ctx1 = ExecutionContext.Create(WorkflowId "g-wf", runId, History.Empty(WorkflowId "g-wf", runId))
        let ctx2 = ExecutionContext.Create(WorkflowId "g-wf", runId, History.Empty(WorkflowId "g-wf", runId))

        let step1 = (guidWorkflow ()) ctx1
        let step2 = (guidWorkflow ()) ctx2

        match step1, step2 with
        | WorkflowStep.Done((g1a, g2a), _), WorkflowStep.Done((g1b, g2b), _) ->
            Assert.Equal(g1a, g1b)
            Assert.Equal(g2a, g2b)
            Assert.NotEqual(g1a, g2a)
        | _ -> Assert.Fail("Expected Done steps")

module DeterminismTests =

    [<Fact>]
    let ``Validator approves identical activity scheduling`` () =
        let wfId = WorkflowId "wf-det-1"
        let runId = RunId (Guid.NewGuid())
        let actId = ActivityId "act-1"
        let cmd = WorkflowCommand.ScheduleActivity(actId, "ProcessPayment", [||], ActivityOptions.Create("main", Duration.FromSeconds 10L), HeaderMap.empty)
        let evt = WorkflowEvent.ActivityTaskScheduled(actId, "ProcessPayment", [||], ActivityOptions.Create("main", Duration.FromSeconds 10L), HeaderMap.empty)

        let res = Validator.validateStep wfId runId 1 cmd evt
        Assert.True(res |> Result.isOk)

    [<Fact>]
    let ``Validator flags modified activity type divergence`` () =
        let wfId = WorkflowId "wf-det-2"
        let runId = RunId (Guid.NewGuid())
        let actId = ActivityId "act-1"
        let cmd = WorkflowCommand.ScheduleActivity(actId, "ProcessPaymentV2", [||], ActivityOptions.Create("main", Duration.FromSeconds 10L), HeaderMap.empty)
        let evt = WorkflowEvent.ActivityTaskScheduled(actId, "ProcessPayment", [||], ActivityOptions.Create("main", Duration.FromSeconds 10L), HeaderMap.empty)

        let res = Validator.validateStep wfId runId 1 cmd evt
        match res with
        | Error report ->
            Assert.Contains("ActivityType mismatch", report.Details)
            Assert.Equal(1, report.StepIndex)
        | Ok () -> Assert.Fail("Validator should flag divergent activity type")

module StateMachineTests =

    [<Fact>]
    let ``State machine tracks pending activities and completions`` () =
        let wfId = WorkflowId "wf-sm"
        let runId = RunId (Guid.NewGuid())
        let initial = WorkflowInstanceSnapshot.Create(wfId, runId, "OrderWorkflow", Timestamp.Zero)

        let ts1 = Timestamp.FromUnixTimeMilliseconds 1000L
        let r1 = JournalRecord.Create(SequenceNumber 1L, ts1, WorkflowEvent.WorkflowExecutionStarted("OrderWorkflow", [||], "main", None, None, HeaderMap.empty, 1), 0u)
        let s1 = transition initial r1
        Assert.Equal(ExecutionStatus.Running, s1.Status)

        let actId = ActivityId "act-auth"
        let r2 = JournalRecord.Create(SequenceNumber 2L, ts1.AddMilliseconds 50L, WorkflowEvent.ActivityTaskScheduled(actId, "Authorize", [||], ActivityOptions.Create("main", Duration.FromSeconds 5L), HeaderMap.empty), r1.RecordHash)
        let s2 = transition s1 r2
        Assert.True(s2.PendingActivities.ContainsKey actId.Value)

        let r3 = JournalRecord.Create(SequenceNumber 3L, ts1.AddMilliseconds 150L, WorkflowEvent.ActivityTaskCompleted(actId, [| 42uy |]), r2.RecordHash)
        let s3 = transition s2 r3
        Assert.False(s3.PendingActivities.ContainsKey actId.Value)
        Assert.Equal(3, s3.HistoryLength)

module ActorInstanceTests =

    [<Fact>]
    let ``WorkflowInstance executes end to end with actor mailbox`` () =
        let twoStepWorkflow () = workflow {
            let! res1 = Workflow.activity<int, int> "DoubleNumber" 21 None
            do! Workflow.sleep (Duration.FromSeconds 1L)
            return sprintf "Final: %d" (res1 * 2)
        }

        let wfId = WorkflowId "wf-actor-test"
        let runId = RunId (Guid.NewGuid())
        let mutable currentTicks = 100000000L
        let clock () = Timestamp currentTicks

        let instance = WorkflowInstance(wfId, runId, twoStepWorkflow, History.Empty(wfId, runId), clock)

        // Step 1: Initial step should schedule DoubleNumber
        let cmd1 = instance.Step() |> Async.RunSynchronously
        Assert.Single(cmd1) |> ignore
        match cmd1.[0] with
        | WorkflowCommand.ScheduleActivity(actId, actType, inputBytes, _, _) ->
            Assert.Equal("DoubleNumber", actType)
            let input = Json.deserializeFromBytes<int>(inputBytes)
            Assert.Equal(21, input)

            // Step 2: Resolve DoubleNumber with result 42
            currentTicks <- currentTicks + 1000000L
            let resBytes = Json.serializeToBytes 42
            let cmd2 = instance.ResolveActivity(actId, resBytes) |> Async.RunSynchronously
            Assert.Single(cmd2) |> ignore

            // Step 3: Now it yields StartTimer
            match cmd2.[0] with
            | WorkflowCommand.StartTimer(tId, dur, _) ->
                Assert.Equal(1.0, dur.TotalSeconds)

                // Step 4: Fire the timer
                currentTicks <- currentTicks + 10000000L
                let cmd3 = instance.FireTimer(tId) |> Async.RunSynchronously
                Assert.Single(cmd3) |> ignore

                // Step 5: Workflow completes!
                match cmd3.[0] with
                | WorkflowCommand.CompleteWorkflow resultBytes ->
                    let finalOutput = Json.deserializeFromBytes<string>(resultBytes)
                    Assert.Equal("Final: 84", finalOutput)
                | other -> Assert.Fail(sprintf "Expected CompleteWorkflow, got %A" other)

            | other -> Assert.Fail(sprintf "Expected StartTimer, got %A" other)
        | other -> Assert.Fail(sprintf "Expected ScheduleActivity, got %A" other)
