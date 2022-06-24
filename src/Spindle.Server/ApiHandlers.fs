namespace Spindle.Server

open System
open System.Collections.Concurrent
open Spindle.Core.Domain
open Spindle.Core.Events
open Spindle.Core.Commands
open Spindle.Core.Metadata
open Spindle.Engine.WorkflowContext
open Spindle.Engine.WorkflowInstance
open Spindle.Engine.StateMachine
open Spindle.Storage
open Spindle.Sagas.Workflows
open Spindle.Server.ServerTypes

/// Central runtime engine coordinator handling REST and WebSocket management requests.
module ApiHandlers =

    /// In-memory runtime state and workflow execution registry.
    type WorkflowRuntimeService() =
        let storage = MemoryStorage() :> IEventStore
        let instances = ConcurrentDictionary<string, WorkflowId * RunId * (unit -> WorkflowInstanceSnapshot) * (string -> byte[] -> Async<unit>)>()
        let activeSummaries = ConcurrentDictionary<string, WorkflowSummaryResponse>()

        let now () = Timestamp.FromDateTimeOffset DateTimeOffset.UtcNow

        member _.StartWorkflow(req: StartWorkflowRequest) : Async<Result<WorkflowSummaryResponse, string>> = async {
            let wfId = WorkflowId req.WorkflowId
            let runId = RunId (Guid.NewGuid())
            let ts = now ()

            // Instantiate appropriate workflow definition based on request type
            match req.WorkflowType.ToLowerInvariant() with
            | "order_fulfillment" ->
                let orderReq = {
                    OrderId = req.WorkflowId
                    CustomerId = "cust-1001"
                    TotalAmount = 249.99M
                    Items = [ { ItemId = "item-sku-42"; Quantity = 2; UnitPrice = 124.995M } ]
                    ShippingAddress = "1 Infinite Loop, Cupertino, CA"
                }
                let wfDef () = orderFulfillmentWorkflow orderReq
                let startEvt = WorkflowEvent.WorkflowExecutionStarted ("OrderFulfillment", [||], "default-queue", None, None, HeaderMap.empty, 1)
                let initialHistory = History.Empty(wfId, runId).Append(startEvt, ts)
                let instance = WorkflowInstance(wfId, runId, wfDef, initialHistory, now)

                // Execute initial steps
                let! commands = instance.Step()

                // Register instance handlers
                let querySnap () = instance.Query() |> Async.RunSynchronously
                let deliverSignal name payload = async {
                    do! instance.Signal(name, payload)
                }
                instances.[req.WorkflowId] <- (wfId, runId, querySnap, deliverSignal)

                let summary = {
                    WorkflowId = req.WorkflowId
                    Status = "Running"
                    CurrentSequence = 1L
                    TotalEvents = 1
                    CreatedAt = ts.ToString()
                }
                activeSummaries.[req.WorkflowId] <- summary

                // Persist initial history to storage engine
                let! _ = storage.AppendRecords(wfId, runId, SequenceNumber.Zero, initialHistory.Chronological)
                return Ok summary

            | "financial_settlement" ->
                let wireReq = {
                    TransactionId = req.WorkflowId
                    SourceAccount = "ACCT-US-001"
                    DestinationAccount = "ACCT-GB-992"
                    Currency = "USD"
                    Amount = 500000.00M
                    RoutingNumber = "021000021"
                }
                let wfDef () = financialSettlementWorkflow wireReq
                let startEvt = WorkflowEvent.WorkflowExecutionStarted ("FinancialSettlement", [||], "default-queue", None, None, HeaderMap.empty, 1)
                let initialHistory = History.Empty(wfId, runId).Append(startEvt, ts)
                let instance = WorkflowInstance(wfId, runId, wfDef, initialHistory, now)

                let! _ = instance.Step()
                let querySnap () = instance.Query() |> Async.RunSynchronously
                let deliverSignal name payload = async {
                    do! instance.Signal(name, payload)
                }
                instances.[req.WorkflowId] <- (wfId, runId, querySnap, deliverSignal)

                let summary = {
                    WorkflowId = req.WorkflowId
                    Status = "Running"
                    CurrentSequence = 1L
                    TotalEvents = 1
                    CreatedAt = ts.ToString()
                }
                activeSummaries.[req.WorkflowId] <- summary
                let! _ = storage.AppendRecords(wfId, runId, SequenceNumber.Zero, initialHistory.Chronological)
                return Ok summary

            | "cloud_provisioning" ->
                let provReq = {
                    EnvironmentName = "prod-eu-west-1"
                    Region = "eu-west-1"
                    NodeCount = 12
                    DomainName = "api.mesh.internal"
                }
                let wfDef () = cloudProvisioningWorkflow provReq
                let startEvt = WorkflowEvent.WorkflowExecutionStarted ("CloudProvisioning", [||], "default-queue", None, None, HeaderMap.empty, 1)
                let initialHistory = History.Empty(wfId, runId).Append(startEvt, ts)
                let instance = WorkflowInstance(wfId, runId, wfDef, initialHistory, now)

                let! _ = instance.Step()
                let querySnap () = instance.Query() |> Async.RunSynchronously
                let deliverSignal name payload = async {
                    do! instance.Signal(name, payload)
                }
                instances.[req.WorkflowId] <- (wfId, runId, querySnap, deliverSignal)

                let summary = {
                    WorkflowId = req.WorkflowId
                    Status = "Running"
                    CurrentSequence = 1L
                    TotalEvents = 1
                    CreatedAt = ts.ToString()
                }
                activeSummaries.[req.WorkflowId] <- summary
                let! _ = storage.AppendRecords(wfId, runId, SequenceNumber.Zero, initialHistory.Chronological)
                return Ok summary

            | unknown ->
                return Error (sprintf "Unknown workflow type '%s'. Supported: order_fulfillment, financial_settlement, cloud_provisioning" unknown)
        }

        member _.GetWorkflowStatus(workflowId: string) : WorkflowSummaryResponse option =
            match instances.TryGetValue workflowId with
            | true, (wfId, runId, getSnap, _) ->
                let snap = getSnap ()
                let statusStr =
                    match snap.Status with
                    | ExecutionStatus.Pending -> "Pending"
                    | ExecutionStatus.Running -> "Running"
                    | ExecutionStatus.Completed -> "Completed"
                    | ExecutionStatus.Failed -> "Failed"
                    | ExecutionStatus.Terminated -> "Terminated"
                    | ExecutionStatus.TimedOut -> "TimedOut"
                    | ExecutionStatus.Compensating -> "Compensating"
                    | ExecutionStatus.Compensated -> "Compensated"
                Some {
                    WorkflowId = workflowId
                    Status = statusStr
                    CurrentSequence = snap.CurrentSequence.Value
                    TotalEvents = int snap.CurrentSequence.Value
                    CreatedAt = now().ToString()
                }
            | false, _ ->
                match activeSummaries.TryGetValue workflowId with
                | true, s -> Some s
                | false, _ -> None

        member _.GetWorkflowHistory(workflowId: string) : Async<JournalRecord list option> = async {
            match instances.TryGetValue workflowId with
            | true, (wfId, runId, _, _) ->
                let! records = storage.ReadRecords(wfId, runId, SequenceNumber.Initial)
                return Some records
            | false, _ ->
                return None
        }

        member _.SignalWorkflow(workflowId: string, req: SignalWorkflowRequest) : Async<Result<unit, string>> = async {
            match instances.TryGetValue workflowId with
            | true, (_, _, _, deliver) ->
                let bytes = System.Text.Encoding.UTF8.GetBytes req.PayloadJson
                do! deliver req.SignalName bytes
                return Ok ()
            | false, _ ->
                return Error (sprintf "Workflow instance '%s' not found" workflowId)
        }

        member _.GetClusterTopology() : ClusterTopologyResponse =
            {
                Nodes = [
                    { NodeId = "spindle-node-1"; Endpoint = "127.0.0.1:9001"; Status = "Alive"; Incarnation = 1L }
                    { NodeId = "spindle-node-2"; Endpoint = "127.0.0.1:9002"; Status = "Alive"; Incarnation = 1L }
                    { NodeId = "spindle-node-3"; Endpoint = "127.0.0.1:9003"; Status = "Alive"; Incarnation = 1L }
                ]
                TotalShards = 1024
            }

        member _.GetMetrics() : Map<string, obj> =
            Map [
                "active_workflows", box instances.Count
                "total_shards", box 1024
                "memory_allocated_mb", box (GC.GetTotalMemory(false) / 1048576L)
                "gc_gen0", box (GC.CollectionCount 0)
                "status", box "Healthy"
            ]
