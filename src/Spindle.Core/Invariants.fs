namespace Spindle.Core

open Spindle.Core.Domain
open Spindle.Core.Events

/// Formal validation rules and invariant verification for workflow journals and execution trees.
module Invariants =

    /// Detailed invariant failure diagnostic.
    type InvariantViolation = {
        Rule: string
        Expected: string
        Actual: string
        SequenceNumber: SequenceNumber
    }

    /// Verifies that a history stream satisfies all ordering, hashing, and causal rules.
    module Verifier =

        /// Validates sequence numbers strictly increase by 1 starting from 1.
        let verifySequenceMonotonicity (records: JournalRecord list) : Result<unit, InvariantViolation> =
            let rec loop (expectedSeq: SequenceNumber) (items: JournalRecord list) : Result<unit, InvariantViolation> =
                match items with
                | [] -> Ok ()
                | r :: rest ->
                    if r.SequenceNumber <> expectedSeq then
                        Error {
                            Rule = "SequenceMonotonicity"
                            Expected = string expectedSeq.Value
                            Actual = string r.SequenceNumber.Value
                            SequenceNumber = r.SequenceNumber
                        }
                    else
                        loop (expectedSeq.Next()) rest

            loop SequenceNumber.Initial records

        /// Validates that the hash chain is unbroken across all records in the journal.
        let verifyHashChain (records: JournalRecord list) : Result<unit, InvariantViolation> =
            let rec loop (prevHash: uint32) (items: JournalRecord list) : Result<unit, InvariantViolation> =
                match items with
                | [] -> Ok ()
                | r :: rest ->
                    if r.PrevHash <> prevHash then
                        Error {
                            Rule = "HashChainContinuity"
                            Expected = sprintf "0x%08X" prevHash
                            Actual = sprintf "0x%08X" r.PrevHash
                            SequenceNumber = r.SequenceNumber
                        }
                    else
                        loop r.RecordHash rest

            loop 0u records

        /// Validates causal dependencies (e.g. ActivityCompleted must match a prior ActivityScheduled).
        let verifyCausalDependencies (records: JournalRecord list) : Result<unit, InvariantViolation> =
            let mutable scheduledActivities = Set.empty<string>
            let mutable activeTimers = Set.empty<string>
            let mutable isTerminal = false
            let mutable result = Ok ()

            for r in records do
                if result |> Result.isOk then
                    if isTerminal then
                        result <- Error {
                            Rule = "NoEventsAfterTerminal"
                            Expected = "Journal to end after terminal event"
                            Actual = "Recorded additional event"
                            SequenceNumber = r.SequenceNumber
                        }
                    else
                        match r.Event with
                        | WorkflowEvent.WorkflowExecutionCompleted _
                        | WorkflowEvent.WorkflowExecutionFailed _
                        | WorkflowEvent.WorkflowExecutionCancelled _
                        | WorkflowEvent.WorkflowExecutionTimedOut _ ->
                            isTerminal <- true

                        | WorkflowEvent.ActivityTaskScheduled(actId, _, _, _, _) ->
                            scheduledActivities <- scheduledActivities.Add(actId.Value)

                        | WorkflowEvent.ActivityTaskCompleted(actId, _)
                        | WorkflowEvent.ActivityTaskFailed(actId, _, _, _)
                        | WorkflowEvent.ActivityTaskTimedOut(actId, _) ->
                            if not (scheduledActivities.Contains(actId.Value)) then
                                result <- Error {
                                    Rule = "ActivityCausality"
                                    Expected = sprintf "Prior ActivityTaskScheduled for %s" actId.Value
                                    Actual = "Activity resolved without prior schedule"
                                    SequenceNumber = r.SequenceNumber
                                }

                        | WorkflowEvent.TimerStarted(tId, _, _) ->
                            activeTimers <- activeTimers.Add(tId.Value)

                        | WorkflowEvent.TimerFired(tId)
                        | WorkflowEvent.TimerCancelled(tId) ->
                            if not (activeTimers.Contains(tId.Value)) then
                                result <- Error {
                                    Rule = "TimerCausality"
                                    Expected = sprintf "Prior TimerStarted for %s" tId.Value
                                    Actual = "Timer fired or cancelled without prior start"
                                    SequenceNumber = r.SequenceNumber
                                }

                        | _ -> ()

            result

        /// Performs a comprehensive invariant audit across the entire journal.
        let verifyAll (records: JournalRecord list) : Result<unit, InvariantViolation list> =
            let seqRes = verifySequenceMonotonicity records
            let hashRes = verifyHashChain records
            let causalRes = verifyCausalDependencies records

            let violations =
                [
                    match seqRes with Error v -> yield v | Ok () -> ()
                    match hashRes with Error v -> yield v | Ok () -> ()
                    match causalRes with Error v -> yield v | Ok () -> ()
                ]

            if List.isEmpty violations then Ok ()
            else Error violations
