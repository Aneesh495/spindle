namespace Spindle.Sagas

open System
open Spindle.Core.Domain
open Spindle.Core.Metadata

/// Domain abstractions, compensation policies, and lifecycle outcomes for distributed sagas.
module SagaTypes =

    /// A recorded forward step that has an associated compensating action.
    type SagaStepRecord = {
        StepId: string
        StepName: string
        CompensationName: string
        ActionPayload: byte[]
        CompensationPayload: byte[]
        ExecutedAt: Timestamp
    } with
        static member Create(stepId: string, name: string, compName: string, actBytes: byte[], compBytes: byte[], ts: Timestamp) = {
            StepId = stepId
            StepName = name
            CompensationName = compName
            ActionPayload = actBytes
            CompensationPayload = compBytes
            ExecutedAt = ts
        }

    /// Rollback ordering policy for saga compensations.
    [<RequireQualifiedAccess>]
    type SagaCompensationPolicy =
        | StrictLifo
        | ParallelBestEffort

    /// Overall outcome of a saga execution.
    [<RequireQualifiedAccess>]
    type SagaOutcome<'T> =
        | Success of result: 'T
        | Compensated of rolledBackSteps: string list * originalFailure: FailureInfo
        | CriticalFailure of uncompensatedSteps: string list * rollbackFailure: FailureInfo
