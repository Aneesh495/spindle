namespace Spindle.Sagas

open System
open Spindle.Core.Domain
open Spindle.Core.Metadata
open Spindle.Sagas.SagaTypes

/// LIFO compensation coordinator executing reverse rollback chains upon distributed transaction failure.
type CompensationCoordinator(policy: SagaCompensationPolicy) =

    let steps = ResizeArray<SagaStepRecord>()
    let syncRoot = Object()

    member _.StepCount = lock syncRoot (fun () -> steps.Count)

    /// Registers a successfully executed forward step with its compensation metadata.
    member _.RegisterStep(step: SagaStepRecord) =
        lock syncRoot (fun () -> steps.Add(step))

    /// Executes the compensation sequence across all registered steps according to policy.
    member _.ExecuteRollback
        (originalFailure: FailureInfo)
        (invoker: string * string * byte[] -> Async<Result<unit, FailureInfo>>)
        : Async<SagaOutcome<unit>> = async {

        let stepList = lock syncRoot (fun () -> Seq.toList steps)
        let rolledBack = ResizeArray<string>()
        let mutable failedRollback = None

        match policy with
        | SagaCompensationPolicy.StrictLifo ->
            let lifoSteps = List.rev stepList
            for step in lifoSteps do
                if failedRollback.IsNone then
                    let! res = invoker (step.StepId, step.CompensationName, step.CompensationPayload)
                    match res with
                    | Ok () ->
                        rolledBack.Add(step.StepId)
                    | Error failInfo ->
                        failedRollback <- Some (step.StepId, failInfo)

        | SagaCompensationPolicy.ParallelBestEffort ->
            for step in stepList do
                let! res = invoker (step.StepId, step.CompensationName, step.CompensationPayload)
                match res with
                | Ok () -> rolledBack.Add(step.StepId)
                | Error failInfo ->
                    if failedRollback.IsNone then
                        failedRollback <- Some (step.StepId, failInfo)

        match failedRollback with
        | Some (failedStepId, compFailure) ->
            let uncompensated =
                stepList
                |> List.map (fun s -> s.StepId)
                |> List.filter (fun id -> not (rolledBack.Contains id))
            return SagaOutcome.CriticalFailure(uncompensated, compFailure)
        | None ->
            return SagaOutcome.Compensated(Seq.toList rolledBack, originalFailure)
    }
