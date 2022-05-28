namespace Spindle.Sagas

open System
open Spindle.Core.Domain
open Spindle.Core.Metadata
open Spindle.Core.Commands
open Spindle.Engine.WorkflowBuilder

/// Production distributed saga workflows demonstrating real-world transaction compensation.
module Workflows =

    // ==========================================
    // SAGA 1: E-COMMERCE ORDER FULFILLMENT
    // ==========================================

    type OrderItem = { ItemId: string; Quantity: int; UnitPrice: decimal }
    type OrderRequest = {
        OrderId: string
        CustomerId: string
        TotalAmount: decimal
        Items: OrderItem list
        ShippingAddress: string
    }

    type PaymentAuthorization = { AuthCode: string; AuthorizedAmount: decimal }
    type InventoryReservation = { ReservationToken: string; WarehouseId: string }
    type CourierBooking = { TrackingNumber: string; Carrier: string }

    type OrderFulfillmentResult = {
        OrderId: string
        AuthCode: string
        ReservationToken: string
        TrackingNumber: string
        FulfilledAt: Timestamp
    }

    /// Complete E-Commerce Order Fulfillment Saga with LIFO compensation.
    let orderFulfillmentWorkflow (order: OrderRequest) : Workflow<OrderFulfillmentResult> = workflow {
        // Step 1: Authorize payment on customer credit card
        let! payment = Workflow.activity<OrderRequest, PaymentAuthorization> "AuthorizePayment" order None

        // Register compensation: if subsequent steps fail, refund authorization
        do! Workflow.compensate (fun () -> workflow {
            do! Workflow.activity<PaymentAuthorization, unit> "RefundPayment" payment None
        })

        // Step 2: Reserve inventory at primary warehouse
        let! inv = Workflow.activity<OrderRequest, InventoryReservation> "ReserveInventory" order None

        // Register compensation: release reserved inventory stock
        do! Workflow.compensate (fun () -> workflow {
            do! Workflow.activity<InventoryReservation, unit> "ReleaseInventory" inv None
        })

        // Step 3: Book courier dispatch
        let! courier = Workflow.activity<OrderRequest, CourierBooking> "DispatchCourier" order None

        // Register compensation: cancel courier pickup
        do! Workflow.compensate (fun () -> workflow {
            do! Workflow.activity<CourierBooking, unit> "CancelCourierDispatch" courier None
        })

        // Step 4: Finalize and return tracking info
        let! currentTs = Workflow.now
        return {
            OrderId = order.OrderId
            AuthCode = payment.AuthCode
            ReservationToken = inv.ReservationToken
            TrackingNumber = courier.TrackingNumber
            FulfilledAt = currentTs
        }
    }

    // ==========================================
    // SAGA 2: FINANCIAL MULTI-LEG SETTLEMENT
    // ==========================================

    type WireTransferRequest = {
        TransactionId: string
        SourceAccount: string
        DestinationAccount: string
        Currency: string
        Amount: decimal
        RoutingNumber: string
    }

    type LedgerHoldReceipt = { HoldId: string; Amount: decimal }
    type AmlCheckResult = { Passed: bool; Score: float }
    type WireExecutionResult = { FedwireReference: string; SettledTime: Timestamp }

    type FinancialSettlementResult = {
        TransactionId: string
        HoldId: string
        FedwireReference: string
        Settled: bool
    }

    /// Mission-critical interbank wire transfer saga with regulatory AML checks and recall rollback.
    let financialSettlementWorkflow (request: WireTransferRequest) : Workflow<FinancialSettlementResult> = workflow {
        // Step 1: Place debit hold on source account ledger
        let! hold = Workflow.activity<WireTransferRequest, LedgerHoldReceipt> "PlaceLedgerHold" request None

        // Register compensation: release ledger hold on rollback
        do! Workflow.compensate (fun () -> workflow {
            do! Workflow.activity<LedgerHoldReceipt, unit> "ReleaseLedgerHold" hold None
        })

        // Step 2: Regulatory AML / Sanctions screening
        let! aml = Workflow.activity<WireTransferRequest, AmlCheckResult> "RunAmlSanctionsCheck" request None
        if not aml.Passed then
            do! Workflow.fail "AmlSanctionsFailure" "Wire transfer blocked by compliance sanctions filter"

        // Step 3: Execute interbank Fedwire transfer
        let! wire = Workflow.activity<WireTransferRequest, WireExecutionResult> "ExecuteFedwireTransfer" request None

        // Register compensation: request Fedwire recall notice
        do! Workflow.compensate (fun () -> workflow {
            do! Workflow.activity<WireExecutionResult, unit> "RecallFedwireTransfer" wire None
        })

        // Step 4: Post final clearing entry
        do! Workflow.activity<string, unit> "UpdateClearingLedger" request.TransactionId None

        return {
            TransactionId = request.TransactionId
            HoldId = hold.HoldId
            FedwireReference = wire.FedwireReference
            Settled = true
        }
    }

    // ==========================================
    // SAGA 3: CLOUD INFRASTRUCTURE PROVISIONING
    // ==========================================

    type ProvisioningRequest = {
        EnvironmentName: string
        Region: string
        NodeCount: int
        DomainName: string
    }

    type VpcAllocation = { VpcId: string; SubnetCidr: string }
    type ComputeCluster = { ClusterId: string; MasterIp: string }
    type TlsCertificate = { CertId: string; Fingerprint: string }

    type ProvisioningResult = {
        EnvironmentName: string
        VpcId: string
        ClusterId: string
        DomainName: string
        Status: string
    }

    /// Cloud infrastructure orchestrator provisioning networking, compute, certificates, and DNS.
    let cloudProvisioningWorkflow (req: ProvisioningRequest) : Workflow<ProvisioningResult> = workflow {
        // Step 1: Allocate VPC network and subnets
        let! vpc = Workflow.activity<ProvisioningRequest, VpcAllocation> "AllocateVpc" req None

        do! Workflow.compensate (fun () -> workflow {
            do! Workflow.activity<VpcAllocation, unit> "DeallocateVpc" vpc None
        })

        // Step 2: Spin up compute worker nodes
        let! cluster = Workflow.activity<VpcAllocation, ComputeCluster> "CreateComputeCluster" vpc None

        do! Workflow.compensate (fun () -> workflow {
            do! Workflow.activity<ComputeCluster, unit> "DeleteComputeCluster" cluster None
        })

        // Step 3: Issue automated TLS certificate
        let! cert = Workflow.activity<string, TlsCertificate> "IssueTlsCertificate" req.DomainName None

        do! Workflow.compensate (fun () -> workflow {
            do! Workflow.activity<TlsCertificate, unit> "RevokeTlsCertificate" cert None
        })

        // Step 4: Update global DNS anycast routing
        do! Workflow.activity<ComputeCluster * string, unit> "ConfigureDnsRouting" (cluster, req.DomainName) None

        do! Workflow.compensate (fun () -> workflow {
            do! Workflow.activity<string, unit> "RevertDnsRouting" req.DomainName None
        })

        return {
            EnvironmentName = req.EnvironmentName
            VpcId = vpc.VpcId
            ClusterId = cluster.ClusterId
            DomainName = req.DomainName
            Status = "Provisioned"
        }
    }
