namespace Spindle.Engine

open System
open Spindle.Core.Domain
open Spindle.Core.Metadata
open Spindle.Core.Commands
open Spindle.Core.Events
open Spindle.Core.Codecs
open Spindle.Engine.WorkflowContext

/// Monadic coroutine workflow computation expression and primitives.
module WorkflowBuilder =

    /// Step outcome of a workflow coroutine evaluation.
    [<RequireQualifiedAccess>]
    type WorkflowStep<'T> =
        | Done of value: 'T * context: ExecutionContext
        | Yield of command: WorkflowCommand * continuation: (ExecutionContext -> WorkflowStep<'T>)
        | Failed of failure: FailureInfo * context: ExecutionContext
        | RegisterCompensation of compensation: (unit -> ExecutionContext -> WorkflowStep<unit>) * continuation: (ExecutionContext -> WorkflowStep<'T>)

    /// Monadic workflow coroutine function type.
    type Workflow<'T> = ExecutionContext -> WorkflowStep<'T>

    /// Computation expression builder for writing durable event-sourced workflows in native F#.
    type WorkflowBuilder() =

        member _.Return(value: 'T) : Workflow<'T> =
            fun ctx -> WorkflowStep.Done(value, ctx)

        member _.ReturnFrom(w: Workflow<'T>) : Workflow<'T> =
            w

        member _.Zero() : Workflow<unit> =
            fun ctx -> WorkflowStep.Done((), ctx)

        member this.Bind(w: Workflow<'T>, f: 'T -> Workflow<'U>) : Workflow<'U> =
            fun ctx ->
                match w ctx with
                | WorkflowStep.Done(value, ctx') ->
                    f value ctx'

                | WorkflowStep.Yield(cmd, cont) ->
                    WorkflowStep.Yield(cmd, fun ctx'' -> this.Bind(cont, f) ctx'')

                | WorkflowStep.Failed(failure, ctx') ->
                    WorkflowStep.Failed(failure, ctx')

                | WorkflowStep.RegisterCompensation(comp, cont) ->
                    WorkflowStep.RegisterCompensation(comp, fun ctx'' -> this.Bind(cont, f) ctx'')

        member _.Delay(generator: unit -> Workflow<'T>) : Workflow<'T> =
            fun ctx -> generator () ctx

        member _.Run(w: Workflow<'T>) : Workflow<'T> =
            w

        member this.Combine(w1: Workflow<unit>, w2: Workflow<'T>) : Workflow<'T> =
            this.Bind(w1, fun () -> w2)

        member this.TryWith(w: Workflow<'T>, handler: exn -> Workflow<'T>) : Workflow<'T> =
            fun ctx ->
                try
                    match w ctx with
                    | WorkflowStep.Done(v, c) -> WorkflowStep.Done(v, c)
                    | WorkflowStep.Yield(cmd, cont) ->
                        WorkflowStep.Yield(cmd, fun c' -> this.TryWith(cont, handler) c')
                    | WorkflowStep.Failed(fail, c) ->
                        WorkflowStep.Failed(fail, c)
                    | WorkflowStep.RegisterCompensation(comp, cont) ->
                        WorkflowStep.RegisterCompensation(comp, fun c' -> this.TryWith(cont, handler) c')
                with ex ->
                    handler ex ctx

        member this.TryFinally(w: Workflow<'T>, finalizer: unit -> unit) : Workflow<'T> =
            fun ctx ->
                try
                    match w ctx with
                    | WorkflowStep.Done(v, c) ->
                        finalizer ()
                        WorkflowStep.Done(v, c)
                    | WorkflowStep.Yield(cmd, cont) ->
                        WorkflowStep.Yield(cmd, fun c' -> this.TryFinally(cont, finalizer) c')
                    | WorkflowStep.Failed(fail, c) ->
                        finalizer ()
                        WorkflowStep.Failed(fail, c)
                    | WorkflowStep.RegisterCompensation(comp, cont) ->
                        WorkflowStep.RegisterCompensation(comp, fun c' -> this.TryFinally(cont, finalizer) c')
                with ex ->
                    finalizer ()
                    reraise ()

        member this.While(guard: unit -> bool, body: Workflow<unit>) : Workflow<unit> =
            if not (guard ()) then
                this.Zero()
            else
                this.Bind(body, fun () -> this.While(guard, body))

        member this.For(sequence: seq<'T>, body: 'T -> Workflow<unit>) : Workflow<unit> =
            use enumerator = sequence.GetEnumerator()
            let rec loop () =
                if enumerator.MoveNext() then
                    this.Bind(body enumerator.Current, loop)
                else
                    this.Zero()
            loop ()

    /// Computation expression instance keyword.
    let workflow = WorkflowBuilder()

    /// Core library of workflow operations and coroutine yield primitives.
    module Workflow =

        /// Schedule an external activity task with typed input and output.
        /// If replaying and the activity is already completed in history, returns immediately.
        let activity<'TInput, 'TOutput>
            (activityType: string)
            (input: 'TInput)
            (options: ActivityOptions option)
            : Workflow<'TOutput> =
            fun ctx ->
                let actIdStr, ctxWithId = ctx.NextDeterministicId("act")
                let actId = ActivityId actIdStr
                let resolvedOptions = defaultArg options (ActivityOptions.Create("default", Duration.FromSeconds 30L))

                match ctxWithId.ResolvedActivities |> Map.tryFind actId.Value with
                | Some (ResolutionState.Completed payload) ->
                    let output = Json.deserializeFromBytes<'TOutput>(payload)
                    WorkflowStep.Done(output, ctxWithId)

                | Some (ResolutionState.Failed failure) ->
                    WorkflowStep.Failed(failure, ctxWithId)

                | Some (ResolutionState.TimedOut timeoutType) ->
                    let failure = FailureInfo.Create("Spindle.ActivityTimeout", sprintf "Activity timed out (%s)" timeoutType, nonRetryable = true)
                    WorkflowStep.Failed(failure, ctxWithId)

                | Some ResolutionState.Pending
                | None ->
                    let inputBytes = Json.serializeToBytes input
                    let cmd = WorkflowCommand.ScheduleActivity(actId, activityType, inputBytes, resolvedOptions, HeaderMap.empty)
                    let ctxWithCmd = ctxWithId.EmitCommand(cmd)

                    WorkflowStep.Yield(cmd, fun resumedCtx ->
                        match resumedCtx.ResolvedActivities |> Map.tryFind actId.Value with
                        | Some (ResolutionState.Completed payload) ->
                            let output = Json.deserializeFromBytes<'TOutput>(payload)
                            WorkflowStep.Done(output, resumedCtx)
                        | Some (ResolutionState.Failed failure) ->
                            WorkflowStep.Failed(failure, resumedCtx)
                        | Some (ResolutionState.TimedOut timeoutType) ->
                            let failure = FailureInfo.Create("Spindle.ActivityTimeout", sprintf "Activity timed out (%s)" timeoutType, nonRetryable = true)
                            WorkflowStep.Failed(failure, resumedCtx)
                        | _ ->
                            WorkflowStep.Failed(FailureInfo.Create("Spindle.UnresolvedActivity", "Activity was yielded but not resolved in context"), resumedCtx)
                    )

        /// Suspends the workflow execution until a durable timer fires.
        let sleep (duration: Duration) : Workflow<unit> =
            fun ctx ->
                let timerIdStr, ctxWithId = ctx.NextDeterministicId("timer")
                let timerId = TimerId timerIdStr

                if ctxWithId.FiredTimers.Contains(timerId.Value) then
                    WorkflowStep.Done((), ctxWithId)
                else
                    let fireAt = ctxWithId.CurrentTimestamp.AddMilliseconds(int64 duration.TotalMilliseconds)
                    let cmd = WorkflowCommand.StartTimer(timerId, duration, fireAt)
                    let ctxWithCmd = ctxWithId.EmitCommand(cmd)

                    WorkflowStep.Yield(cmd, fun resumedCtx ->
                        if resumedCtx.FiredTimers.Contains(timerId.Value) then
                            WorkflowStep.Done((), resumedCtx)
                        else
                            WorkflowStep.Failed(FailureInfo.Create("Spindle.TimerNotFired", "Timer yielded but did not fire in resumed context"), resumedCtx)
                    )

        /// Awaits an external asynchronous signal sent to the workflow instance.
        let awaitSignal<'T> (signalName: string) : Workflow<'T> =
            fun ctx ->
                match ctx.ConsumeSignal(signalName) with
                | Some payload, updatedCtx ->
                    let signalData = Json.deserializeFromBytes<'T>(payload)
                    WorkflowStep.Done(signalData, updatedCtx)

                | None, ctxWaiting ->
                    let rec waitForSignal (c: ExecutionContext) =
                        match c.ConsumeSignal(signalName) with
                        | Some payload, finalCtx ->
                            let signalData = Json.deserializeFromBytes<'T>(payload)
                            WorkflowStep.Done(signalData, finalCtx)
                        | None, pendingCtx ->
                            let timerIdStr, ctxWithId = pendingCtx.NextDeterministicId("sigwait")
                            let cmd = WorkflowCommand.StartTimer(TimerId timerIdStr, Duration.FromMilliseconds 100L, pendingCtx.CurrentTimestamp.AddMilliseconds 100L)
                            WorkflowStep.Yield(cmd, waitForSignal)

                    let timerIdStr, ctxWithId = ctxWaiting.NextDeterministicId("sigwait")
                    let cmd = WorkflowCommand.StartTimer(TimerId timerIdStr, Duration.FromMilliseconds 100L, ctxWaiting.CurrentTimestamp.AddMilliseconds 100L)
                    let ctxWithCmd = ctxWithId.EmitCommand(cmd)
                    WorkflowStep.Yield(cmd, waitForSignal)

        /// Registers a compensation rollback closure into the LIFO saga rollback stack.
        let compensate (compensationAction: unit -> Workflow<unit>) : Workflow<unit> =
            fun ctx ->
                let compWrapper () : ExecutionContext -> WorkflowStep<unit> =
                    fun (compCtx: ExecutionContext) ->
                        let compWf = compensationAction ()
                        compWf compCtx

                WorkflowStep.RegisterCompensation(compWrapper, fun resumedCtx ->
                    WorkflowStep.Done((), resumedCtx)
                )

        /// Returns the current deterministic virtual timestamp from the execution context.
        let now : Workflow<Timestamp> =
            fun ctx -> WorkflowStep.Done(ctx.CurrentTimestamp, ctx)

        /// Generates a deterministic GUID seeded by the workflow execution ID and step sequence.
        let newGuid : Workflow<Guid> =
            fun ctx ->
                let guid, updatedCtx = ctx.NextDeterministicGuid()
                WorkflowStep.Done(guid, updatedCtx)

        /// Halts workflow execution with an unhandled domain failure.
        let fail (errorType: string) (message: string) : Workflow<'T> =
            fun ctx ->
                let failInfo = FailureInfo.Create(errorType, message)
                WorkflowStep.Failed(failInfo, ctx)

        /// Deterministically executes a list of workflows in sequential join order.
        let forkJoin (workflows: Workflow<'T> list) : Workflow<'T list> =
            let rec stepAll (remaining: Workflow<'T> list) (acc: 'T list) (ctx: ExecutionContext) : WorkflowStep<'T list> =
                match remaining with
                | [] -> WorkflowStep.Done(acc, ctx)
                | w :: rest ->
                    match w ctx with
                    | WorkflowStep.Done(v, c) -> stepAll rest (acc @ [ v ]) c
                    | WorkflowStep.Yield(cmd, cont) ->
                        WorkflowStep.Yield(cmd, fun nextCtx ->
                            match cont nextCtx with
                            | WorkflowStep.Done(v, c') -> stepAll rest (acc @ [ v ]) c'
                            | WorkflowStep.Yield(nCmd, nCont) ->
                                WorkflowStep.Yield(nCmd, fun c'' ->
                                    match nCont c'' with
                                    | WorkflowStep.Done(v', c''') -> stepAll rest (acc @ [ v' ]) c'''
                                    | WorkflowStep.Failed(f, c''') -> WorkflowStep.Failed(f, c''')
                                    | _ -> WorkflowStep.Failed(FailureInfo.Create("JoinFailure", "Unresolved parallel join"), c'')
                                )
                            | WorkflowStep.Failed(f, c') -> WorkflowStep.Failed(f, c')
                            | WorkflowStep.RegisterCompensation(comp, nCont) ->
                                WorkflowStep.RegisterCompensation(comp, fun c' -> stepAll rest acc c')
                        )
                    | WorkflowStep.Failed(f, c) -> WorkflowStep.Failed(f, c)
                    | WorkflowStep.RegisterCompensation(comp, cont) ->
                        WorkflowStep.RegisterCompensation(comp, fun c ->
                            match cont c with
                            | WorkflowStep.Done(_, c') -> stepAll rest acc c'
                            | _ -> stepAll rest acc c
                        )

            fun ctx -> stepAll workflows [] ctx
