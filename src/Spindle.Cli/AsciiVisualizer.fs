namespace Spindle.Cli

open System
open Spindle.Core.Domain
open Spindle.Core.Events
open Spindle.Engine.StateMachine
open Spindle.Cli.AnsiConsole

/// ASCII formatting and DAG rendering engine for terminal inspection.
module AsciiVisualizer =

    /// Renders an ASCII hierarchical workflow execution tree.
    let renderWorkflowTree (snapshot: WorkflowInstanceSnapshot) : unit =
        let statusColor =
            match snapshot.Status.ToString() with
            | "Completed" -> Green
            | "Running" | "Pending" -> Yellow
            | "Failed" | "Compensating" | "Compensated" -> Red
            | _ -> White

        printfn "%s[Workflow]%s %s (%s%s%s)" Cyan Reset snapshot.WorkflowId.Value statusColor (snapshot.Status.ToString()) Reset
        printfn "  Run ID: %s" (snapshot.RunId.ToString())
        printfn "  Current Sequence: #%d" snapshot.CurrentSequence.Value
        printfn "  Recorded Events: %d" snapshot.HistoryLength

        if not snapshot.PendingActivities.IsEmpty then
            printfn "\n  %sActive & Pending Activities:%s" Bold Reset
            for KeyValue(actId, actState) in snapshot.PendingActivities do
                printfn "    ├── [Activity] %s (Type: %s, Attempt: %d)" actId actState.ActivityType actState.Attempt

        if not snapshot.ActiveTimers.IsEmpty then
            printfn "\n  %sActive Durable Timers:%s" Bold Reset
            for KeyValue(tId, tState) in snapshot.ActiveTimers do
                printfn "    ├── [Timer] %s (Duration: %s, FiresAt: %s)" tId (tState.Duration.ToString()) (tState.FireAt.ToString())

        if not snapshot.CompensationStack.IsEmpty then
            printfn "\n  %sRegistered Saga Compensations (LIFO Barrier):%s" Bold Reset
            for (stepId, compType, _) in snapshot.CompensationStack do
                printfn "    └── [Undo Barrier] %s (Rollback: %s)" stepId compType

    /// Renders an ANSI-colored tabular representation of event journal records.
    let renderJournalTable (records: JournalRecord list) : unit =
        printfn "%s%-6s %-26s %-30s %-12s%s" Bold "SEQ" "TIMESTAMP" "EVENT TYPE" "CRC32C" Reset
        printfn "%s" (String.replicate 78 "-")
        for r in records do
            let eventName =
                match r.Event with
                | WorkflowEvent.WorkflowExecutionStarted _ -> "WorkflowExecutionStarted"
                | WorkflowEvent.WorkflowExecutionCompleted _ -> "WorkflowExecutionCompleted"
                | WorkflowEvent.WorkflowExecutionFailed _ -> "WorkflowExecutionFailed"
                | WorkflowEvent.WorkflowExecutionCancelled _ -> "WorkflowExecutionCancelled"
                | WorkflowEvent.WorkflowExecutionTimedOut _ -> "WorkflowExecutionTimedOut"
                | WorkflowEvent.WorkflowTaskScheduled _ -> "WorkflowTaskScheduled"
                | WorkflowEvent.WorkflowTaskStarted _ -> "WorkflowTaskStarted"
                | WorkflowEvent.WorkflowTaskCompleted _ -> "WorkflowTaskCompleted"
                | WorkflowEvent.ActivityTaskScheduled(actId, actType, _, _, _) -> sprintf "ActivityScheduled (%s)" actId.Value
                | WorkflowEvent.ActivityTaskStarted(actId, _, _) -> sprintf "ActivityStarted (%s)" actId.Value
                | WorkflowEvent.ActivityTaskCompleted(actId, _) -> sprintf "ActivityCompleted (%s)" actId.Value
                | WorkflowEvent.ActivityTaskFailed(actId, _, _, _) -> sprintf "ActivityFailed (%s)" actId.Value
                | WorkflowEvent.ActivityTaskTimedOut(actId, _) -> sprintf "ActivityTimedOut (%s)" actId.Value
                | WorkflowEvent.TimerStarted(tId, _, _) -> sprintf "TimerStarted (%s)" tId.Value
                | WorkflowEvent.TimerFired(tId) -> sprintf "TimerFired (%s)" tId.Value
                | WorkflowEvent.TimerCancelled(tId) -> sprintf "TimerCancelled (%s)" tId.Value
                | WorkflowEvent.WorkflowSignaled(name, _) -> sprintf "Signaled (%s)" name
                | WorkflowEvent.MarkerRecorded(name, _, _) -> sprintf "Marker (%s)" name
                | WorkflowEvent.SagaCompensationStarted(stepId, compType) -> sprintf "CompStarted (%s: %s)" stepId compType
                | WorkflowEvent.SagaCompensationCompleted(stepId, _) -> sprintf "CompCompleted (%s)" stepId
                | WorkflowEvent.SagaCompensationFailed(stepId, _) -> sprintf "CompFailed (%s)" stepId
                | WorkflowEvent.ChildWorkflowInitiated(wId, wType, _) -> sprintf "ChildInitiated (%s: %s)" wId.Value wType
                | WorkflowEvent.ChildWorkflowCompleted(wId, _) -> sprintf "ChildCompleted (%s)" wId.Value
                | WorkflowEvent.ChildWorkflowFailed(wId, _) -> sprintf "ChildFailed (%s)" wId.Value

            let evtColor =
                if eventName.Contains "Completed" then Green
                elif eventName.Contains "Failed" then Red
                elif eventName.Contains "Scheduled" || eventName.Contains "Started" then Cyan
                else White

            let tsStr = r.Timestamp.ToString()
            let seqStr = sprintf "#%d" r.SequenceNumber.Value
            let hashStr = sprintf "0x%08X" r.RecordHash
            printfn "%-6s %-26s %s%-30s%s %-12s" seqStr tsStr evtColor eventName Reset hashStr
        printfn "%s" (String.replicate 78 "-")
