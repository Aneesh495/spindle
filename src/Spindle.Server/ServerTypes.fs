namespace Spindle.Server

open System
open Spindle.Core.Domain

/// Request and response data contracts for the Spindle HTTP management API.
module ServerTypes =

    [<CLIMutable>]
    type StartWorkflowRequest = {
        WorkflowType: string
        WorkflowId: string
        TenantId: string
        InputJson: string
    }

    [<CLIMutable>]
    type SignalWorkflowRequest = {
        SignalName: string
        PayloadJson: string
    }

    [<CLIMutable>]
    type WorkflowSummaryResponse = {
        WorkflowId: string
        Status: string
        CurrentSequence: int64
        TotalEvents: int
        CreatedAt: string
    }

    [<CLIMutable>]
    type ClusterNodeResponse = {
        NodeId: string
        Endpoint: string
        Status: string
        Incarnation: int64
    }

    [<CLIMutable>]
    type ClusterTopologyResponse = {
        Nodes: ClusterNodeResponse list
        TotalShards: int
    }
