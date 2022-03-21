namespace Spindle.Engine

open System
open Spindle.Core.Domain
open Spindle.Core.Metadata
open Spindle.Core.Commands
open Spindle.Core.Events

/// Pure functional state machine reducer managing workflow instance state transitions.
module StateMachine =

    /// State of an in-flight activity task.
    type PendingActivityState = {
        ActivityId: ActivityId
        ActivityType: string
        Attempt: int
        ScheduledAt: Timestamp
        StartedAt: Timestamp option
    }

    /// State of an active durable timer.
    type ActiveTimerState = {
        TimerId: TimerId
        Duration: Duration
        FireAt: Timestamp
    }

    /// Comprehensive snapshot of a workflow instance state.
    type WorkflowInstanceSnapshot = {
        WorkflowId: WorkflowId
        RunId: RunId
        WorkflowType: string
        Status: ExecutionStatus
        CurrentSequence: SequenceNumber
        HistoryLength: int
        PendingActivities: Map<string, PendingActivityState>
        ActiveTimers: Map<string, ActiveTimerState>
        CompensationStack: (string * string * byte[]) list
        WorkflowResult: byte[] option
        WorkflowFailure: FailureInfo option
        LastUpdated: Timestamp
    } with
        static member Create(wfId: WorkflowId, runId: RunId, wfType: string, ts: Timestamp) = {
            WorkflowId = wfId
            RunId = runId
            WorkflowType = wfType
            Status = ExecutionStatus.Pending
            CurrentSequence = SequenceNumber.Zero
            HistoryLength = 0
            PendingActivities = Map.empty
            ActiveTimers = Map.empty
            CompensationStack = []
            WorkflowResult = None
            WorkflowFailure = None
            LastUpdated = ts
        }

    /// Pure reducer transitioning workflow state upon processing a journal record.
    let transition (state: WorkflowInstanceSnapshot) (record: JournalRecord) : WorkflowInstanceSnapshot =
        let updatedSeq = record.SequenceNumber
        let ts = record.Timestamp
        let updatedLen = state.HistoryLength + 1

        match record.Event with
        | WorkflowEvent.WorkflowExecutionStarted(wfType, _, _, _, _, _, _) ->
            { state with
                WorkflowType = wfType
                Status = ExecutionStatus.Running
                CurrentSequence = updatedSeq
                HistoryLength = updatedLen
                LastUpdated = ts }

        | WorkflowEvent.WorkflowExecutionCompleted(resultBytes) ->
            { state with
                Status = ExecutionStatus.Completed
                WorkflowResult = Some resultBytes
                CurrentSequence = updatedSeq
                HistoryLength = updatedLen
                LastUpdated = ts }

        | WorkflowEvent.WorkflowExecutionFailed(failure) ->
            { state with
                Status = ExecutionStatus.Failed
                WorkflowFailure = Some failure
                CurrentSequence = updatedSeq
                HistoryLength = updatedLen
                LastUpdated = ts }

        | WorkflowEvent.WorkflowExecutionCancelled(_) ->
            { state with
                Status = ExecutionStatus.Terminated
                CurrentSequence = updatedSeq
                HistoryLength = updatedLen
                LastUpdated = ts }

        | WorkflowEvent.WorkflowExecutionTimedOut(_) ->
            { state with
                Status = ExecutionStatus.TimedOut
                CurrentSequence = updatedSeq
                HistoryLength = updatedLen
                LastUpdated = ts }

        | WorkflowEvent.ActivityTaskScheduled(actId, actType, _, _, _) ->
            let actState = {
                ActivityId = actId
                ActivityType = actType
                Attempt = 1
                ScheduledAt = ts
                StartedAt = None
            }
            { state with
                PendingActivities = state.PendingActivities.Add(actId.Value, actState)
                CurrentSequence = updatedSeq
                HistoryLength = updatedLen
                LastUpdated = ts }

        | WorkflowEvent.ActivityTaskStarted(actId, attempt, _) ->
            match state.PendingActivities |> Map.tryFind actId.Value with
            | Some existing ->
                let updated = { existing with Attempt = attempt; StartedAt = Some ts }
                { state with
                    PendingActivities = state.PendingActivities.Add(actId.Value, updated)
                    CurrentSequence = updatedSeq
                    HistoryLength = updatedLen
                    LastUpdated = ts }
            | None ->
                { state with CurrentSequence = updatedSeq; HistoryLength = updatedLen; LastUpdated = ts }

        | WorkflowEvent.ActivityTaskCompleted(actId, _)
        | WorkflowEvent.ActivityTaskFailed(actId, _, _, _)
        | WorkflowEvent.ActivityTaskTimedOut(actId, _) ->
            { state with
                PendingActivities = state.PendingActivities.Remove(actId.Value)
                CurrentSequence = updatedSeq
                HistoryLength = updatedLen
                LastUpdated = ts }

        | WorkflowEvent.TimerStarted(tId, dur, fireAt) ->
            let tState = {
                TimerId = tId
                Duration = dur
                FireAt = fireAt
            }
            { state with
                ActiveTimers = state.ActiveTimers.Add(tId.Value, tState)
                CurrentSequence = updatedSeq
                HistoryLength = updatedLen
                LastUpdated = ts }

        | WorkflowEvent.TimerFired(tId)
        | WorkflowEvent.TimerCancelled(tId) ->
            { state with
                ActiveTimers = state.ActiveTimers.Remove(tId.Value)
                CurrentSequence = updatedSeq
                HistoryLength = updatedLen
                LastUpdated = ts }

        | WorkflowEvent.SagaCompensationStarted(stepId, compType) ->
            { state with
                Status = ExecutionStatus.Compensating
                CurrentSequence = updatedSeq
                HistoryLength = updatedLen
                LastUpdated = ts }

        | WorkflowEvent.SagaCompensationCompleted(stepId, _) ->
            let remaining = state.CompensationStack |> List.filter (fun (id, _, _) -> id <> stepId)
            let newStatus = if List.isEmpty remaining then ExecutionStatus.Compensated else ExecutionStatus.Compensating
            { state with
                Status = newStatus
                CompensationStack = remaining
                CurrentSequence = updatedSeq
                HistoryLength = updatedLen
                LastUpdated = ts }

        | WorkflowEvent.SagaCompensationFailed(_, failure) ->
            { state with
                Status = ExecutionStatus.Failed
                WorkflowFailure = Some failure
                CurrentSequence = updatedSeq
                HistoryLength = updatedLen
                LastUpdated = ts }

        | _ ->
            { state with
                CurrentSequence = updatedSeq
                HistoryLength = updatedLen
                LastUpdated = ts }

    /// Rebuilds workflow instance state by replaying an entire journal history from inception.
    let rebuildFromHistory (wfId: WorkflowId) (runId: RunId) (records: JournalRecord list) : WorkflowInstanceSnapshot =
        let initial = WorkflowInstanceSnapshot.Create(wfId, runId, "Unknown", Timestamp.Zero)
        records |> List.fold transition initial
