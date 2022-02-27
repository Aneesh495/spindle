namespace Spindle.Core

open Spindle.Core.Domain
open Spindle.Core.Metadata

/// Commands emitted by a workflow instance during execution steps to instruct the kernel runtime.
module Commands =

    /// Discriminated union of all workflow-emitted commands.
    [<RequireQualifiedAccess>]
    type WorkflowCommand =
        | ScheduleActivity of
            activityId: ActivityId *
            activityType: string *
            input: byte[] *
            options: ActivityOptions *
            headers: HeaderMap
        | StartTimer of
            timerId: TimerId *
            duration: Duration *
            fireAt: Timestamp
        | CancelTimer of
            timerId: TimerId
        | RecordMarker of
            markerName: string *
            data: byte[] *
            details: string option
        | ScheduleChildWorkflow of
            childWorkflowId: WorkflowId *
            workflowType: string *
            input: byte[] *
            parentClosePolicy: string
        | SignalExternalWorkflow of
            targetWorkflowId: WorkflowId *
            signalName: string *
            payload: byte[]
        | CompleteWorkflow of
            result: byte[]
        | FailWorkflow of
            failure: FailureInfo
        | CancelWorkflow of
            reason: string
        | RequestCompensation of
            stepId: string *
            compensationType: string *
            payload: byte[]

    /// Helper utilities for inspecting and categorizing commands.
    module WorkflowCommand =

        let isTerminal (cmd: WorkflowCommand) : bool =
            match cmd with
            | WorkflowCommand.CompleteWorkflow _
            | WorkflowCommand.FailWorkflow _
            | WorkflowCommand.CancelWorkflow _ -> true
            | _ -> false

        let commandType (cmd: WorkflowCommand) : string =
            match cmd with
            | WorkflowCommand.ScheduleActivity _ -> "ScheduleActivity"
            | WorkflowCommand.StartTimer _ -> "StartTimer"
            | WorkflowCommand.CancelTimer _ -> "CancelTimer"
            | WorkflowCommand.RecordMarker _ -> "RecordMarker"
            | WorkflowCommand.ScheduleChildWorkflow _ -> "ScheduleChildWorkflow"
            | WorkflowCommand.SignalExternalWorkflow _ -> "SignalExternalWorkflow"
            | WorkflowCommand.CompleteWorkflow _ -> "CompleteWorkflow"
            | WorkflowCommand.FailWorkflow _ -> "FailWorkflow"
            | WorkflowCommand.CancelWorkflow _ -> "CancelWorkflow"
            | WorkflowCommand.RequestCompensation _ -> "RequestCompensation"
