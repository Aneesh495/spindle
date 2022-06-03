namespace Spindle.Cluster

open System
open Spindle.Core.Domain

/// Cluster membership primitives, node identifiers, and incarnation counters.
module NodeId =

    /// Strongly-typed cluster node identifier.
    [<Struct>]
    type NodeId =
        | NodeId of string
        member this.Value = match this with NodeId v -> v
        override this.ToString() = this.Value

    /// Lifecycle status of a node in the cluster group.
    [<RequireQualifiedAccess>]
    type NodeStatus =
        | Alive
        | Suspect
        | Dead
        | Left

    /// Full cluster node membership descriptor with incarnation number.
    [<CLIMutable>]
    type MemberInfo = {
        NodeId: NodeId
        Host: string
        Port: int
        Incarnation: int64
        Status: NodeStatus
    } with
        member this.Endpoint = sprintf "%s:%d" this.Host this.Port
        static member Create(nodeId: NodeId, host: string, port: int, incarnation: int64, status: NodeStatus) = {
            NodeId = nodeId
            Host = host
            Port = port
            Incarnation = incarnation
            Status = status
        }
