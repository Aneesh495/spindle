namespace Spindle.Queue

open System
open System.Collections.Concurrent
open Spindle.Core.Domain
open Spindle.Core.Metadata
open Spindle.Core.Commands

/// A queued activity task awaiting worker execution.
[<CLIMutable>]
type QueuedActivityTask = {
    ActivityId: ActivityId
    WorkflowId: WorkflowId
    RunId: RunId
    ActivityType: string
    InputPayload: byte[]
    Options: ActivityOptions
    Headers: HeaderMap
    Attempt: int
    ScheduledAt: Timestamp
}

/// Messages for the sharded queue dispatcher actor.
[<RequireQualifiedAccess>]
type DispatcherMessage =
    | EnqueueTask of task: QueuedActivityTask * priority: int * reply: AsyncReplyChannel<unit>
    | PollTask of queueName: string * workerId: string * timeout: Duration * now: Timestamp * reply: AsyncReplyChannel<QueuedActivityTask option>
    | GetQueueDepth of queueName: string * reply: AsyncReplyChannel<int>

/// Multi-tenant sharded task queue dispatcher managing partitioned priority queues.
type ShardedDispatcher(leaseManager: LeaseManager) =

    let queues = ConcurrentDictionary<string, PriorityQueue<QueuedActivityTask>>()

    let getOrCreateQueue (queueName: string) =
        queues.GetOrAdd(queueName, fun _ -> PriorityQueue<QueuedActivityTask>())

    let mailbox = MailboxProcessor<DispatcherMessage>.Start(fun inbox ->
        let rec loop () = async {
            let! msg = inbox.Receive()
            match msg with
            | DispatcherMessage.EnqueueTask(task, priority, reply) ->
                let q = getOrCreateQueue task.Options.TaskQueue
                q.Enqueue(task, priority)
                reply.Reply ()
                return! loop ()

            | DispatcherMessage.PollTask(qName, workerId, timeout, now, reply) ->
                let q = getOrCreateQueue qName
                let rec tryAcquire () =
                    match q.TryDequeue() with
                    | Some task ->
                        match leaseManager.AcquireLease(task.ActivityId, workerId, timeout, now) with
                        | Ok _ -> Some task
                        | Error _ ->
                            // Task already leased or conflicting, try next
                            tryAcquire ()
                    | None -> None

                let selected = tryAcquire ()
                reply.Reply selected
                return! loop ()

            | DispatcherMessage.GetQueueDepth(qName, reply) ->
                let depth =
                    match queues.TryGetValue qName with
                    | true, q -> q.Count
                    | false, _ -> 0
                reply.Reply depth
                return! loop ()
        }
        loop ()
    )

    /// Enqueues an activity task with an explicit integer priority.
    member _.Enqueue(task: QueuedActivityTask, ?priority: int) : Async<unit> =
        let prio = defaultArg priority 0
        mailbox.PostAndAsyncReply (fun r -> DispatcherMessage.EnqueueTask(task, prio, r))

    /// Polls for the highest-priority available task, acquiring a lease on it atomically.
    member _.Poll(queueName: string, workerId: string, timeout: Duration, now: Timestamp) : Async<QueuedActivityTask option> =
        mailbox.PostAndAsyncReply (fun r -> DispatcherMessage.PollTask(queueName, workerId, timeout, now, r))

    /// Returns the number of pending tasks in a named queue.
    member _.GetDepth(queueName: string) : Async<int> =
        mailbox.PostAndAsyncReply (fun r -> DispatcherMessage.GetQueueDepth(queueName, r))
