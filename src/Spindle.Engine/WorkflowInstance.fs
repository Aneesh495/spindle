namespace Spindle.Engine

open System
open System.Threading.Tasks
open Spindle.Core.Domain
open Spindle.Core.Metadata
open Spindle.Core.Commands
open Spindle.Core.Events
open Spindle.Engine.WorkflowContext
open Spindle.Engine.WorkflowBuilder
open Spindle.Engine.StateMachine
open Spindle.Engine.Determinism

/// Stateful actor container managing workflow execution, history, and message dispatch.
module WorkflowInstance =

    /// Messages accepted by the workflow instance actor mailbox.
    [<RequireQualifiedAccess>]
    type InstanceMessage =
        | StepExecution of reply: AsyncReplyChannel<WorkflowCommand list>
        | ResolveActivity of activityId: ActivityId * result: byte[] * reply: AsyncReplyChannel<WorkflowCommand list>
        | FailActivity of activityId: ActivityId * failure: FailureInfo * reply: AsyncReplyChannel<WorkflowCommand list>
        | FireTimer of timerId: TimerId * reply: AsyncReplyChannel<WorkflowCommand list>
        | DeliverSignal of signalName: string * payload: byte[] * reply: AsyncReplyChannel<unit>
        | QuerySnapshot of reply: AsyncReplyChannel<WorkflowInstanceSnapshot>

    /// Live actor-hosted workflow execution instance.
    type WorkflowInstance<'T>(
        workflowId: WorkflowId,
        runId: RunId,
        workflowDef: unit -> Workflow<'T>,
        initialHistory: History,
        clock: unit -> Timestamp
    ) =
        let mutable currentHistory = initialHistory
        let mutable currentState = StateMachine.rebuildFromHistory workflowId runId initialHistory.Chronological
        let mutable activeCompensations: (unit -> ExecutionContext -> WorkflowStep<unit>) list = []

        let executeWorkflow () : WorkflowCommand list =
            let ctx = ExecutionContext.Create(workflowId, runId, currentHistory)
            let wf = workflowDef ()

            let rec stepToCommand (step: WorkflowStep<'T>) (currentCtx: ExecutionContext) : WorkflowCommand list =
                match step with
                | WorkflowStep.Done(res, resCtx) ->
                    let resBytes = Spindle.Core.Codecs.Json.serializeToBytes res
                    let completeCmd = WorkflowCommand.CompleteWorkflow resBytes
                    let ts = clock ()
                    let rec1 = JournalRecord.Create(
                        (match currentHistory.Events with [] -> SequenceNumber.Initial | h :: _ -> h.SequenceNumber.Next()),
                        ts,
                        WorkflowEvent.WorkflowExecutionCompleted resBytes,
                        (match currentHistory.Events with [] -> 0u | h :: _ -> h.RecordHash)
                    )
                    currentHistory <- { currentHistory with Events = rec1 :: currentHistory.Events }
                    currentState <- StateMachine.transition currentState rec1
                    [ completeCmd ]

                | WorkflowStep.Yield(cmd, _) ->
                    [ cmd ]

                | WorkflowStep.Failed(failure, _) ->
                    let failCmd = WorkflowCommand.FailWorkflow failure
                    let ts = clock ()
                    let rec1 = JournalRecord.Create(
                        (match currentHistory.Events with [] -> SequenceNumber.Initial | h :: _ -> h.SequenceNumber.Next()),
                        ts,
                        WorkflowEvent.WorkflowExecutionFailed failure,
                        (match currentHistory.Events with [] -> 0u | h :: _ -> h.RecordHash)
                    )
                    currentHistory <- { currentHistory with Events = rec1 :: currentHistory.Events }
                    currentState <- StateMachine.transition currentState rec1
                    [ failCmd ]

                | WorkflowStep.RegisterCompensation(comp, cont, ctx') ->
                    activeCompensations <- comp :: activeCompensations
                    stepToCommand (cont ctx') ctx'

            stepToCommand (wf ctx) ctx

        let mailbox = MailboxProcessor<InstanceMessage>.Start(fun inbox ->
            let rec loop () = async {
                let! msg = inbox.Receive()
                try
                    match msg with
                    | InstanceMessage.StepExecution reply ->
                        let commands = executeWorkflow ()
                        reply.Reply commands
                        return! loop ()

                    | InstanceMessage.ResolveActivity(actId, resultPayload, reply) ->
                        let ts = clock ()
                        let evt = WorkflowEvent.ActivityTaskCompleted(actId, resultPayload)
                        currentHistory <- currentHistory.Append(evt, ts)
                        currentState <- StateMachine.transition currentState (List.head currentHistory.Events)
                        let commands = executeWorkflow ()
                        reply.Reply commands
                        return! loop ()

                    | InstanceMessage.FailActivity(actId, failure, reply) ->
                        let ts = clock ()
                        let evt = WorkflowEvent.ActivityTaskFailed(actId, failure, 1, false)
                        currentHistory <- currentHistory.Append(evt, ts)
                        currentState <- StateMachine.transition currentState (List.head currentHistory.Events)
                        let commands = executeWorkflow ()
                        reply.Reply commands
                        return! loop ()

                    | InstanceMessage.FireTimer(tId, reply) ->
                        let ts = clock ()
                        let evt = WorkflowEvent.TimerFired(tId)
                        currentHistory <- currentHistory.Append(evt, ts)
                        currentState <- StateMachine.transition currentState (List.head currentHistory.Events)
                        let commands = executeWorkflow ()
                        reply.Reply commands
                        return! loop ()

                    | InstanceMessage.DeliverSignal(sigName, payload, reply) ->
                        let ts = clock ()
                        let evt = WorkflowEvent.WorkflowSignaled(sigName, payload)
                        currentHistory <- currentHistory.Append(evt, ts)
                        currentState <- StateMachine.transition currentState (List.head currentHistory.Events)
                        reply.Reply ()
                        return! loop ()

                    | InstanceMessage.QuerySnapshot reply ->
                        reply.Reply currentState
                        return! loop ()
                with ex ->
                    printfn "WorkflowInstance Mailbox Exception: %s\n%s" ex.Message ex.StackTrace
                    return! loop ()
            }
            loop ()
        )

        member _.WorkflowId = workflowId
        member _.RunId = runId
        member _.History = currentHistory
        member _.State = currentState

        member _.Step() : Async<WorkflowCommand list> =
            mailbox.PostAndAsyncReply InstanceMessage.StepExecution

        member _.ResolveActivity(actId: ActivityId, resultBytes: byte[]) : Async<WorkflowCommand list> =
            mailbox.PostAndAsyncReply (fun r -> InstanceMessage.ResolveActivity(actId, resultBytes, r))

        member _.FailActivity(actId: ActivityId, failure: FailureInfo) : Async<WorkflowCommand list> =
            mailbox.PostAndAsyncReply (fun r -> InstanceMessage.FailActivity(actId, failure, r))

        member _.FireTimer(timerId: TimerId) : Async<WorkflowCommand list> =
            mailbox.PostAndAsyncReply (fun r -> InstanceMessage.FireTimer(timerId, r))

        member _.Signal(signalName: string, payload: byte[]) : Async<unit> =
            mailbox.PostAndAsyncReply (fun r -> InstanceMessage.DeliverSignal(signalName, payload, r))

        member _.Query() : Async<WorkflowInstanceSnapshot> =
            mailbox.PostAndAsyncReply InstanceMessage.QuerySnapshot
