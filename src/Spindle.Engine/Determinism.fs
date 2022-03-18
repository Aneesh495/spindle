namespace Spindle.Engine

open System
open Spindle.Core.Domain
open Spindle.Core.Commands
open Spindle.Core.Events

/// Divergence diagnostics and determinism verification for workflow history replay.
module Determinism =

    /// Comprehensive diagnostic record explaining a replay divergence violation.
    type DivergenceReport = {
        WorkflowId: WorkflowId
        RunId: RunId
        StepIndex: int
        ExpectedEvent: string
        ActualCommand: string
        Details: string
    } with
        override this.ToString() =
            sprintf "[DIVERGENCE at Step %d] Workflow %s (Run %s): Expected history event '%s', but replaying workflow emitted command '%s'. Reason: %s"
                this.StepIndex
                this.WorkflowId.Value
                (this.RunId.ToString())
                this.ExpectedEvent
                this.ActualCommand
                this.Details

    /// Exception raised whenever history replay diverges from recorded journal commands.
    type NonDeterministicReplayException(report: DivergenceReport) =
        inherit Exception(report.ToString())
        member _.Report = report

    /// Replay validation engine comparing emitted commands against recorded journal events.
    module Validator =

        /// Validates that an emitted command corresponds causally to the recorded journal event.
        let validateStep
            (wfId: WorkflowId)
            (runId: RunId)
            (stepIndex: int)
            (command: WorkflowCommand)
            (recordedEvent: WorkflowEvent)
            : Result<unit, DivergenceReport> =

            let failWith details expected actual =
                Error {
                    WorkflowId = wfId
                    RunId = runId
                    StepIndex = stepIndex
                    ExpectedEvent = expected
                    ActualCommand = actual
                    Details = details
                }

            match command, recordedEvent with
            | WorkflowCommand.ScheduleActivity(actId, actType, _, _, _),
              WorkflowEvent.ActivityTaskScheduled(histActId, histActType, _, _, _) ->
                if actId <> histActId then
                    failWith (sprintf "ActivityId mismatch: workflow generated '%s', history recorded '%s'" actId.Value histActId.Value)
                             (sprintf "ActivityTaskScheduled(%s)" histActId.Value)
                             (sprintf "ScheduleActivity(%s)" actId.Value)
                elif actType <> histActType then
                    failWith (sprintf "ActivityType mismatch for activity '%s': workflow invoked '%s', history recorded '%s'" actId.Value actType histActType)
                             (sprintf "ActivityTaskScheduled(%s, %s)" histActId.Value histActType)
                             (sprintf "ScheduleActivity(%s, %s)" actId.Value actType)
                else
                    Ok ()

            | WorkflowCommand.StartTimer(tId, _, _),
              WorkflowEvent.TimerStarted(histTId, _, _) ->
                if tId <> histTId then
                    failWith (sprintf "TimerId mismatch: workflow started '%s', history recorded '%s'" tId.Value histTId.Value)
                             (sprintf "TimerStarted(%s)" histTId.Value)
                             (sprintf "StartTimer(%s)" tId.Value)
                else
                    Ok ()

            | WorkflowCommand.CancelTimer(tId),
              WorkflowEvent.TimerCancelled(histTId) ->
                if tId <> histTId then
                    failWith (sprintf "TimerId mismatch on cancel: workflow cancelled '%s', history recorded '%s'" tId.Value histTId.Value)
                             (sprintf "TimerCancelled(%s)" histTId.Value)
                             (sprintf "CancelTimer(%s)" tId.Value)
                else
                    Ok ()

            | WorkflowCommand.CompleteWorkflow _,
              WorkflowEvent.WorkflowExecutionCompleted _ ->
                Ok ()

            | WorkflowCommand.FailWorkflow _,
              WorkflowEvent.WorkflowExecutionFailed _ ->
                Ok ()

            | WorkflowCommand.CancelWorkflow _,
              WorkflowEvent.WorkflowExecutionCancelled _ ->
                Ok ()

            | cmd, evt ->
                failWith "Mismatched command and event variant types during deterministic replay"
                         (sprintf "%A" evt)
                         (sprintf "%A" cmd)
