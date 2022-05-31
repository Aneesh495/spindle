namespace Spindle.Tests.Sagas

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
open Spindle.Engine.WorkflowInstance
open Spindle.Sagas.SagaTypes
open Spindle.Sagas
open Spindle.Sagas.Workflows

module CompensationCoordinatorTests =

    [<Fact>]
    let ``Strict LIFO executes compensations in reverse registration order`` () =
        let coord = CompensationCoordinator(SagaCompensationPolicy.StrictLifo)
        let ts = Timestamp.FromUnixTimeMilliseconds 1000L

        let step1 = SagaStepRecord.Create("step-1", "Charge", "Refund", [||], [||], ts)
        let step2 = SagaStepRecord.Create("step-2", "Reserve", "Release", [||], [||], ts.AddMilliseconds 50L)
        let step3 = SagaStepRecord.Create("step-3", "Dispatch", "Cancel", [||], [||], ts.AddMilliseconds 100L)

        coord.RegisterStep(step1)
        coord.RegisterStep(step2)
        coord.RegisterStep(step3)
        Assert.Equal(3, coord.StepCount)

        let executionOrder = ResizeArray<string>()
        let invoker (stepId, compName, _) = async {
            executionOrder.Add(compName)
            return Ok ()
        }

        let outcome = coord.ExecuteRollback (FailureInfo.Create("Fatal", "Crash")) invoker |> Async.RunSynchronously
        match outcome with
        | SagaOutcome.Compensated(rolledBack, _) ->
            Assert.Equal<string>([ "Cancel"; "Release"; "Refund" ], executionOrder)
            Assert.Equal<string>([ "step-3"; "step-2"; "step-1" ], rolledBack)
        | other ->
            Assert.Fail(sprintf "Expected Compensated, got %A" other)

    [<Fact>]
    let ``Partial failure during compensation triggers CriticalFailure`` () =
        let coord = CompensationCoordinator(SagaCompensationPolicy.StrictLifo)
        let ts = Timestamp.FromUnixTimeMilliseconds 1000L

        let step1 = SagaStepRecord.Create("step-1", "Charge", "Refund", [||], [||], ts)
        let step2 = SagaStepRecord.Create("step-2", "Reserve", "Release", [||], [||], ts.AddMilliseconds 50L)

        coord.RegisterStep(step1)
        coord.RegisterStep(step2)

        let invoker (stepId, compName, _) = async {
            if compName = "Release" then
                return Error (FailureInfo.Create("WarehouseOutage", "Inventory database unreachable"))
            else
                return Ok ()
        }

        let outcome = coord.ExecuteRollback (FailureInfo.Create("PaymentFailed", "Card declined")) invoker |> Async.RunSynchronously
        match outcome with
        | SagaOutcome.CriticalFailure(uncompensated, failInfo) ->
            Assert.Contains("step-1", uncompensated)
            Assert.Equal("WarehouseOutage", failInfo.ErrorType)
        | other ->
            Assert.Fail(sprintf "Expected CriticalFailure, got %A" other)

module ProductionWorkflowTests =

    [<Fact>]
    let ``Order fulfillment workflow completes successfully when all activities pass`` () =
        let order: OrderRequest = {
            OrderId = "ord-1001"
            CustomerId = "cust-alice"
            TotalAmount = 149.99M
            Items = [ { ItemId = "item-gadget"; Quantity = 1; UnitPrice = 149.99M } ]
            ShippingAddress = "742 Evergreen Terrace"
        }

        let wfId = WorkflowId "wf-ord-happy"
        let runId = RunId (Guid.NewGuid())
        let clock () = Timestamp.FromUnixTimeMilliseconds 1000L

        let instance = WorkflowInstance(wfId, runId, (fun () -> orderFulfillmentWorkflow order), History.Empty(wfId, runId), clock)

        // Step 1: Schedule AuthorizePayment
        let cmd1 = instance.Step() |> Async.RunSynchronously
        Assert.Single(cmd1) |> ignore
        match cmd1.[0] with
        | WorkflowCommand.ScheduleActivity(actId1, "AuthorizePayment", _, _, _) ->
            let authResult = { AuthCode = "AUTH-OK-99"; AuthorizedAmount = 149.99M }
            let cmd2 = instance.ResolveActivity(actId1, Json.serializeToBytes authResult) |> Async.RunSynchronously

            // Step 2: Schedule ReserveInventory
            Assert.Single(cmd2) |> ignore
            match cmd2.[0] with
            | WorkflowCommand.ScheduleActivity(actId2, "ReserveInventory", _, _, _) ->
                let invResult = { ReservationToken = "RES-WH1-42"; WarehouseId = "WH-Primary" }
                let cmd3 = instance.ResolveActivity(actId2, Json.serializeToBytes invResult) |> Async.RunSynchronously

                // Step 3: Schedule DispatchCourier
                Assert.Single(cmd3) |> ignore
                match cmd3.[0] with
                | WorkflowCommand.ScheduleActivity(actId3, "DispatchCourier", _, _, _) ->
                    let courierResult = { TrackingNumber = "TRK-987654321"; Carrier = "FastShip" }
                    let cmd4 = instance.ResolveActivity(actId3, Json.serializeToBytes courierResult) |> Async.RunSynchronously

                    // Step 4: Workflow completes with OrderFulfillmentResult!
                    Assert.Single(cmd4) |> ignore
                    match cmd4.[0] with
                    | WorkflowCommand.CompleteWorkflow resBytes ->
                        let fulfillment = Json.deserializeFromBytes<OrderFulfillmentResult>(resBytes)
                        Assert.Equal("ord-1001", fulfillment.OrderId)
                        Assert.Equal("AUTH-OK-99", fulfillment.AuthCode)
                        Assert.Equal("RES-WH1-42", fulfillment.ReservationToken)
                        Assert.Equal("TRK-987654321", fulfillment.TrackingNumber)
                    | other -> Assert.Fail(sprintf "Expected CompleteWorkflow, got %A" other)

                | other -> Assert.Fail(sprintf "Expected DispatchCourier, got %A" other)
            | other -> Assert.Fail(sprintf "Expected ReserveInventory, got %A" other)
        | other -> Assert.Fail(sprintf "Expected AuthorizePayment, got %A" other)
