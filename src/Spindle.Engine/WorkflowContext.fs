namespace Spindle.Engine

open System
open System.Collections.Generic
open Spindle.Core.Domain
open Spindle.Core.Metadata
open Spindle.Core.Commands
open Spindle.Core.Events

/// Runtime environment tracking execution progress, virtual clocks, and memoized history.
module WorkflowContext =

    /// Status of a memoized activity or child execution.
    [<RequireQualifiedAccess>]
    type ResolutionState =
        | Pending
        | Completed of payload: byte[]
        | Failed of failure: FailureInfo
        | TimedOut of timeoutType: string

    /// Context state threaded through workflow execution steps.
    type ExecutionContext = {
        WorkflowId: WorkflowId
        RunId: RunId
        CurrentTimestamp: Timestamp
        ReplayIndex: int
        IsReplaying: bool
        Events: JournalRecord list
        ResolvedActivities: Map<string, ResolutionState>
        FiredTimers: Set<string>
        BufferedSignals: Map<string, byte[] list>
        EmittedCommands: WorkflowCommand list
        DeterministicCounter: int64
    } with
        static member Create(wfId: WorkflowId, runId: RunId, history: History) =
            let chronological = history.Chronological

            // Pre-index resolved activities, fired timers, and signals from recorded history
            let mutable acts = Map.empty<string, ResolutionState>
            let mutable timers = Set.empty<string>
            let mutable signals = Map.empty<string, byte[] list>
            let mutable lastTs = Timestamp.Zero

            for record in chronological do
                lastTs <- record.Timestamp
                match record.Event with
                | WorkflowEvent.ActivityTaskScheduled(actId, _, _, _, _) ->
                    if not (acts.ContainsKey actId.Value) then
                        acts <- acts.Add(actId.Value, ResolutionState.Pending)

                | WorkflowEvent.ActivityTaskCompleted(actId, res) ->
                    acts <- acts.Add(actId.Value, ResolutionState.Completed res)

                | WorkflowEvent.ActivityTaskFailed(actId, fail, _, _) ->
                    acts <- acts.Add(actId.Value, ResolutionState.Failed fail)

                | WorkflowEvent.ActivityTaskTimedOut(actId, tType) ->
                    acts <- acts.Add(actId.Value, ResolutionState.TimedOut tType)

                | WorkflowEvent.TimerFired(tId) ->
                    timers <- timers.Add(tId.Value)

                | WorkflowEvent.WorkflowSignaled(sigName, payload) ->
                    let existing = signals |> Map.tryFind sigName |> Option.defaultValue []
                    signals <- signals.Add(sigName, existing @ [ payload ])

                | _ -> ()

            {
                WorkflowId = wfId
                RunId = runId
                CurrentTimestamp = lastTs
                ReplayIndex = 0
                IsReplaying = not (List.isEmpty chronological)
                Events = chronological
                ResolvedActivities = acts
                FiredTimers = timers
                BufferedSignals = signals
                EmittedCommands = []
                DeterministicCounter = 0L
            }

        member this.NextDeterministicId(prefix: string) : string * ExecutionContext =
            let nextCounter = this.DeterministicCounter + 1L
            let id = sprintf "%s-%d" prefix nextCounter
            id, { this with DeterministicCounter = nextCounter }

        member this.NextDeterministicGuid() : Guid * ExecutionContext =
            let nextCounter = this.DeterministicCounter + 1L
            let seed = Murmur3.hashString (uint32 nextCounter) (this.RunId.ToString())
            let bytes = Array.zeroCreate<byte> 16
            let seedBytes = BitConverter.GetBytes(seed)
            Array.Copy(seedBytes, 0, bytes, 0, 4)
            let counterBytes = BitConverter.GetBytes(nextCounter)
            Array.Copy(counterBytes, 0, bytes, 4, 8)
            let guid = Guid(bytes)
            guid, { this with DeterministicCounter = nextCounter }

        member this.EmitCommand(cmd: WorkflowCommand) : ExecutionContext =
            { this with EmittedCommands = this.EmittedCommands @ [ cmd ] }

        member this.ConsumeSignal(signalName: string) : byte[] option * ExecutionContext =
            match this.BufferedSignals |> Map.tryFind signalName with
            | Some (first :: rest) ->
                let updated = this.BufferedSignals.Add(signalName, rest)
                Some first, { this with BufferedSignals = updated }
            | _ -> None, this
