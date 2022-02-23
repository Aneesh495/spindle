namespace Spindle.Core

open System
open System.Collections.Generic
open Spindle.Core.Domain

/// Metadata configurations, execution states, retry policies, and telemetry headers.
module Metadata =

    /// Current lifecycle state of a workflow execution.
    [<RequireQualifiedAccess>]
    type ExecutionStatus =
        | Pending
        | Running
        | Completed
        | Failed
        | Terminated
        | TimedOut
        | Compensating
        | Compensated
        member this.IsTerminal =
            match this with
            | Completed | Failed | Terminated | TimedOut | Compensated -> true
            | Pending | Running | Compensating -> false

    /// Standard failure envelope capturing error classification and diagnostic details.
    type FailureInfo = {
        ErrorType: string
        Message: string
        StackTrace: string option
        NonRetryable: bool
    } with
        static member Create(errType: string, msg: string, ?stack: string, ?nonRetryable: bool) = {
            ErrorType = errType
            Message = msg
            StackTrace = stack
            NonRetryable = defaultArg nonRetryable false
        }
        static member FromException(ex: exn, ?nonRetryable: bool) = {
            ErrorType = ex.GetType().FullName
            Message = ex.Message
            StackTrace = Option.ofObj ex.StackTrace
            NonRetryable = defaultArg nonRetryable false
        }

    /// Retry backoff algorithm configuration.
    [<RequireQualifiedAccess>]
    type BackoffStrategy =
        | Fixed of interval: Duration
        | Exponential of initial: Duration * coefficient: float * max: Duration
        | FullJitter of initial: Duration * max: Duration
        | DecorrelatedJitter of baseInterval: Duration * max: Duration

    /// Comprehensive retry policy specification for activity task execution.
    type RetryPolicy = {
        Strategy: BackoffStrategy
        MaximumAttempts: int option
        NonRetryableErrors: Set<string>
    } with
        static member Default = {
            Strategy = BackoffStrategy.Exponential(Duration.FromMilliseconds(500L), 2.0, Duration.FromSeconds(60L))
            MaximumAttempts = Some 5
            NonRetryableErrors = Set.empty
        }

        static member None = {
            Strategy = BackoffStrategy.Fixed(Duration.Zero)
            MaximumAttempts = Some 1
            NonRetryableErrors = Set.empty
        }

        static member WithMaxAttempts(attempts: int, policy: RetryPolicy) =
            { policy with MaximumAttempts = Some attempts }

        member this.IsRetryable(errorType: string, attempt: int) : bool =
            if this.NonRetryableErrors.Contains(errorType) then false
            else
                match this.MaximumAttempts with
                | Some maxAttempts when attempt >= maxAttempts -> false
                | _ -> true

    /// Execution options for scheduling an activity.
    type ActivityOptions = {
        TaskQueue: string
        ScheduleToStartTimeout: Duration option
        StartToCloseTimeout: Duration
        HeartbeatTimeout: Duration option
        RetryPolicy: RetryPolicy
    } with
        static member Create(taskQueue: string, timeout: Duration) = {
            TaskQueue = taskQueue
            ScheduleToStartTimeout = None
            StartToCloseTimeout = timeout
            HeartbeatTimeout = None
            RetryPolicy = RetryPolicy.Default
        }

    /// W3C distributed trace propagation context for distributed observability.
    type TraceContext = {
        TraceId: string
        SpanId: string
        TraceFlags: byte
        TraceState: string option
    } with
        static member CreateNew() = {
            TraceId = Guid.NewGuid().ToString("N")
            SpanId = Guid.NewGuid().ToString("N").Substring(0, 16)
            TraceFlags = 1uy
            TraceState = None
        }

        static member Empty = {
            TraceId = ""
            SpanId = ""
            TraceFlags = 0uy
            TraceState = None
        }

    /// Key-value header map for carrying out-of-band context and credentials across tasks.
    type HeaderMap = Map<string, string>
    module HeaderMap =
        let empty: HeaderMap = Map.empty
        let ofSeq (pairs: (string * string) seq) : HeaderMap = Map.ofSeq pairs
