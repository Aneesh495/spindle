namespace Spindle.Engine

open System
open System.Diagnostics
open Spindle.Core.Domain
open Spindle.Core.Metadata
open Spindle.Core.Commands
open Spindle.Core.Events

/// Telemetry hooks, distributed trace context propagation, and structured audit interceptors.
module Interceptors =

    /// Structured audit event emitted during workflow execution steps.
    type AuditLogEntry = {
        Timestamp: Timestamp
        WorkflowId: WorkflowId
        RunId: RunId
        Action: string
        Details: string
    }

    /// In-memory audit sink collecting telemetry records for inspection and replay verification.
    type AuditSink() =
        let entries = ResizeArray<AuditLogEntry>()
        let syncRoot = Object()

        member _.Log(wfId: WorkflowId, runId: RunId, action: string, details: string, ts: Timestamp) =
            lock syncRoot (fun () ->
                entries.Add {
                    Timestamp = ts
                    WorkflowId = wfId
                    RunId = runId
                    Action = action
                    Details = details
                }
            )

        member _.GetEntries() : AuditLogEntry list =
            lock syncRoot (fun () -> Seq.toList entries)

        member _.Clear() =
            lock syncRoot (fun () -> entries.Clear())

    /// Telemetry metrics counters and timer histograms.
    type MetricsCollector() =
        let mutable workflowsStarted = 0L
        let mutable workflowsCompleted = 0L
        let mutable workflowsFailed = 0L
        let mutable activitiesScheduled = 0L
        let mutable timersStarted = 0L
        let syncRoot = Object()

        member _.RecordWorkflowStart() =
            lock syncRoot (fun () -> workflowsStarted <- workflowsStarted + 1L)

        member _.RecordWorkflowCompletion() =
            lock syncRoot (fun () -> workflowsCompleted <- workflowsCompleted + 1L)

        member _.RecordWorkflowFailure() =
            lock syncRoot (fun () -> workflowsFailed <- workflowsFailed + 1L)

        member _.RecordActivityScheduled() =
            lock syncRoot (fun () -> activitiesScheduled <- activitiesScheduled + 1L)

        member _.RecordTimerStarted() =
            lock syncRoot (fun () -> timersStarted <- timersStarted + 1L)

        member _.Snapshot() =
            lock syncRoot (fun () ->
                {|
                    Started = workflowsStarted
                    Completed = workflowsCompleted
                    Failed = workflowsFailed
                    Activities = activitiesScheduled
                    Timers = timersStarted
                |}
            )
