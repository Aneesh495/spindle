namespace Spindle.Core

open System
open Spindle.Core.Domain
open Spindle.Core.Metadata

/// Strongly-typed event sourcing journal schema for durable workflow history.
module Events =

    /// Discriminated union of all workflow lifecycle events recorded in history.
    [<RequireQualifiedAccess>]
    type WorkflowEvent =
        // Workflow lifecycle
        | WorkflowExecutionStarted of
            workflowType: string *
            input: byte[] *
            taskQueue: string *
            executionTimeout: Duration option *
            runTimeout: Duration option *
            headers: HeaderMap *
            attempt: int
        | WorkflowExecutionCompleted of
            result: byte[]
        | WorkflowExecutionFailed of
            failure: FailureInfo
        | WorkflowExecutionCancelled of
            reason: string
        | WorkflowExecutionTimedOut of
            timeoutType: string

        // Workflow task processing markers
        | WorkflowTaskScheduled of
            scheduledTime: Timestamp
        | WorkflowTaskStarted of
            startedTime: Timestamp
        | WorkflowTaskCompleted of
            completedTime: Timestamp

        // Activity lifecycle
        | ActivityTaskScheduled of
            activityId: ActivityId *
            activityType: string *
            input: byte[] *
            options: ActivityOptions *
            headers: HeaderMap
        | ActivityTaskStarted of
            activityId: ActivityId *
            attempt: int *
            workerId: string
        | ActivityTaskCompleted of
            activityId: ActivityId *
            result: byte[]
        | ActivityTaskFailed of
            activityId: ActivityId *
            failure: FailureInfo *
            attempt: int *
            retryable: bool
        | ActivityTaskTimedOut of
            activityId: ActivityId *
            timeoutType: string

        // Timer lifecycle
        | TimerStarted of
            timerId: TimerId *
            duration: Duration *
            fireAt: Timestamp
        | TimerFired of
            timerId: TimerId
        | TimerCancelled of
            timerId: TimerId

        // External signals and markers
        | WorkflowSignaled of
            signalName: string *
            payload: byte[]
        | MarkerRecorded of
            markerName: string *
            data: byte[] *
            details: string option

        // Saga compensations
        | SagaCompensationStarted of
            stepId: string *
            compensationType: string
        | SagaCompensationCompleted of
            stepId: string *
            result: byte[]
        | SagaCompensationFailed of
            stepId: string *
            failure: FailureInfo

        // Child workflows
        | ChildWorkflowInitiated of
            childWorkflowId: WorkflowId *
            workflowType: string *
            input: byte[]
        | ChildWorkflowCompleted of
            childWorkflowId: WorkflowId *
            result: byte[]
        | ChildWorkflowFailed of
            childWorkflowId: WorkflowId *
            failure: FailureInfo

    /// A single entry in the append-only workflow event history journal.
    [<CLIMutable>]
    type JournalRecord = {
        SequenceNumber: SequenceNumber
        Timestamp: Timestamp
        Event: WorkflowEvent
        PrevHash: uint32
        RecordHash: uint32
    } with
        static member Create(seq: SequenceNumber, ts: Timestamp, evt: WorkflowEvent, prevHash: uint32) =
            let hashSeed = prevHash ^^^ (uint32 seq.Value) ^^^ (uint32 ts.Ticks)
            let typeTag =
                match evt with
                | WorkflowEvent.WorkflowExecutionStarted _ -> "WorkflowExecutionStarted"
                | WorkflowEvent.WorkflowExecutionCompleted _ -> "WorkflowExecutionCompleted"
                | WorkflowEvent.WorkflowExecutionFailed _ -> "WorkflowExecutionFailed"
                | WorkflowEvent.WorkflowExecutionCancelled _ -> "WorkflowExecutionCancelled"
                | WorkflowEvent.WorkflowExecutionTimedOut _ -> "WorkflowExecutionTimedOut"
                | WorkflowEvent.WorkflowTaskScheduled _ -> "WorkflowTaskScheduled"
                | WorkflowEvent.WorkflowTaskStarted _ -> "WorkflowTaskStarted"
                | WorkflowEvent.WorkflowTaskCompleted _ -> "WorkflowTaskCompleted"
                | WorkflowEvent.ActivityTaskScheduled _ -> "ActivityTaskScheduled"
                | WorkflowEvent.ActivityTaskStarted _ -> "ActivityTaskStarted"
                | WorkflowEvent.ActivityTaskCompleted _ -> "ActivityTaskCompleted"
                | WorkflowEvent.ActivityTaskFailed _ -> "ActivityTaskFailed"
                | WorkflowEvent.ActivityTaskTimedOut _ -> "ActivityTaskTimedOut"
                | WorkflowEvent.TimerStarted _ -> "TimerStarted"
                | WorkflowEvent.TimerFired _ -> "TimerFired"
                | WorkflowEvent.TimerCancelled _ -> "TimerCancelled"
                | WorkflowEvent.WorkflowSignaled _ -> "WorkflowSignaled"
                | WorkflowEvent.MarkerRecorded _ -> "MarkerRecorded"
                | WorkflowEvent.SagaCompensationStarted _ -> "SagaCompensationStarted"
                | WorkflowEvent.SagaCompensationCompleted _ -> "SagaCompensationCompleted"
                | WorkflowEvent.SagaCompensationFailed _ -> "SagaCompensationFailed"
                | WorkflowEvent.ChildWorkflowInitiated _ -> "ChildWorkflowInitiated"
                | WorkflowEvent.ChildWorkflowCompleted _ -> "ChildWorkflowCompleted"
                | WorkflowEvent.ChildWorkflowFailed _ -> "ChildWorkflowFailed"
            let recHash = Murmur3.hashString hashSeed typeTag
            {
                SequenceNumber = seq
                Timestamp = ts
                Event = evt
                PrevHash = prevHash
                RecordHash = recHash
            }

    /// Event history container representing the full event log for a workflow run.
    type History = {
        WorkflowId: WorkflowId
        RunId: RunId
        Events: JournalRecord list
    } with
        static member Empty(wfId: WorkflowId, runId: RunId) = {
            WorkflowId = wfId
            RunId = runId
            Events = []
        }

        member this.Length = this.Events.Length

        member this.Append(evt: WorkflowEvent, ts: Timestamp) : History =
            let nextSeq =
                match this.Events with
                | [] -> SequenceNumber.Initial
                | last :: _ -> last.SequenceNumber.Next()
            let prevHash =
                match this.Events with
                | [] -> 0u
                | last :: _ -> last.RecordHash
            let record = JournalRecord.Create(nextSeq, ts, evt, prevHash)
            { this with Events = record :: this.Events }

        member this.Chronological = List.rev this.Events
