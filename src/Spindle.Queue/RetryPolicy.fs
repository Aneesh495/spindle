namespace Spindle.Queue

open System
open Spindle.Core.Domain
open Spindle.Core.Metadata

/// Backoff calculation algorithms including Full Jitter and Decorrelated Jitter.
module RetryEvaluator =

    /// Calculates the sleep backoff duration for a retry attempt.
    let calculateBackoff
        (policy: RetryPolicy)
        (attempt: int)
        (prevInterval: Duration)
        (rng: float -> float) // Injected deterministic RNG function mapping [0, max] -> sample
        : Duration =

        match policy.Strategy with
        | BackoffStrategy.Fixed interval ->
            interval

        | BackoffStrategy.Exponential(initial, coeff, maxInterval) ->
            let multiplier = Math.Pow(coeff, float (max 0 (attempt - 1)))
            let rawMs = float initial.TotalMilliseconds * multiplier
            let cappedMs = min (float maxInterval.TotalMilliseconds) rawMs
            Duration.FromMilliseconds(int64 cappedMs)

        | BackoffStrategy.FullJitter(initial, maxInterval) ->
            let multiplier = Math.Pow(2.0, float (max 0 (attempt - 1)))
            let ceilingMs = min (float maxInterval.TotalMilliseconds) (float initial.TotalMilliseconds * multiplier)
            let jitteredMs = rng ceilingMs
            Duration.FromMilliseconds(int64 jitteredMs)

        | BackoffStrategy.DecorrelatedJitter(baseInterval, maxInterval) ->
            let low = float baseInterval.TotalMilliseconds
            let high = float prevInterval.TotalMilliseconds * 3.0
            let range = max low high
            let sample = rng range
            let cappedMs = min (float maxInterval.TotalMilliseconds) (max low sample)
            Duration.FromMilliseconds(int64 cappedMs)

    /// Determines whether a task failure should be routed to a dead-letter queue (DLQ).
    let shouldRouteToDlq (policy: RetryPolicy) (errorType: string) (attempt: int) : bool =
        not (policy.IsRetryable(errorType, attempt))
