# Spindle Engineering Bug Ledger

This document catalogs non-trivial architectural and implementation bugs encountered during the development of Spindle. In accordance with zero-defect engineering practices, every bug is documented with its failure symptoms, root cause analysis, reproduction test case, and verified resolution.

---

## Bug Ledger Index

1. [BUG-001: System.Text.Json Serialization Failure on F# Discriminated Unions](#bug-001)
2. [BUG-002: Hierarchical Wheel Level Cascade Order Inversion](#bug-002)
3. [BUG-003: Compensation Execution Context Counter Reset & Activity ID Collision](#bug-003)
4. [BUG-004: Discrete Virtual Clock Step Ordering & Packet Delivery Delay](#bug-004)

---

### BUG-001: System.Text.Json Serialization Failure on F# Discriminated Unions {#bug-001}

* **Component**: `Spindle.Core.Codecs`
* **Severity**: High (Engine crashed during payload round-trip)
* **Symptom**: When serializing F# discriminated unions (e.g., `WorkflowEvent`, `WorkflowCommand`), `System.Text.Json` threw a `NotSupportedException` indicating that F# union types require custom converter registration.
* **Root Cause**: The default reflection-based serializer in .NET `System.Text.Json` does not natively parse F# discriminated union tag fields or record copy semantics without explicit metadata converters.
* **Reproduction**:
  ```fsharp
  let evt = WorkflowEvent.ActivityTaskCompleted(ActivityId "act-1", [| 1uy; 2uy; 3uy |])
  let json = System.Text.Json.JsonSerializer.Serialize(evt) // Throws NotSupportedException
  ```
* **Resolution**:
  Added the `FSharp.SystemTextJson` package to `Spindle.Core.fsproj` and configured `JsonSerializerOptions` in `Spindle.Core.Codecs.Json` with `JsonFSharpConverter()` supporting union tags and record structures.
* **Regression Test**: Verified via `Spindle.Tests.Unit.DomainTests` ("Workflow event round-trips through JSON codec cleanly").

---

### BUG-002: Hierarchical Wheel Level Cascade Order Inversion {#bug-002}

* **Component**: `Spindle.Timers.HierarchicalWheel`
* **Severity**: Critical (Delayed timer execution by 256 ticks)
* **Symptom**: Timers scheduled at `currentTick + 256L` (the boundary between Level 0 and Level 1) were not fired when virtual time reached 256 ticks. They remained dormant until tick 512.
* **Root Cause**: In the `Tick()` method, Level 0 slot collection was executed *before* higher-tier cascading:
  ```fsharp
  // Faulty execution order:
  let expired = level0.[slot] // Collected slot 0 for tick 256 (empty)
  cascadeHigherLevels()       // Cascaded Level 1 into level0 AFTER slot 0 was collected!
  ```
  Timers that belonged to tick 256 cascaded into `level0.[0]` *after* slot 0 was emptied, forcing them to wait an entire wheel cycle (256 ticks).
* **Reproduction**:
  ```fsharp
  let wheel = HierarchicalWheel(0L)
  let timer = TimerEntry.Create(TimerId "t1", ..., expTicks = 256L, ...)
  wheel.Schedule(timer)
  for _ in 1 .. 256 do wheel.Tick() |> ignore // Expected timer to fire on tick 256, but received 0 timers
  ```
* **Resolution**:
  Reordered operations in `HierarchicalWheel.Tick()` so that higher-tier cascading executes *before* Level 0 slot collection. When tick 256 rolls over, Level 1 immediately promotes timers into Level 0 slots before `level0.[slot]` is drained.
* **Regression Test**: Verified via `Spindle.Tests.Timers.TimerTests` ("HierarchicalWheel cascades multi-tier timers correctly").

---

### BUG-003: Compensation Execution Context Counter Reset & Activity ID Collision {#bug-003}

* **Component**: `Spindle.Engine.WorkflowInstance` & `Spindle.Engine.WorkflowBuilder`
* **Severity**: Critical (Activity ID collision and state corruption)
* **Symptom**: During saga compensation after an activity failure, activity identifiers generated inside compensation blocks collided with prior forward activity IDs (both generated `act-1`), corrupting event history and deserializing wrong return payloads.
* **Root Cause**: `WorkflowStep.RegisterCompensation` originally captured only a callback closure `(unit -> WorkflowStep<unit>)` without threading the monotonically advancing `ExecutionContext`. When compensation blocks were evaluated, they began with a fresh context counter `DeterministicCounter = 0`, generating duplicate activity IDs.
* **Reproduction**:
  ```fsharp
  // Forward activity: generates "act-1"
  let! a = Workflow.activity "Step1" input None
  do! Workflow.compensate (fun () -> workflow {
      // Rollback activity: collided on "act-1" instead of "act-2"
      do! Workflow.activity "UndoStep1" () None
  })
  ```
* **Resolution**:
  Modified `WorkflowStep.RegisterCompensation` to be a 3-tuple `(comp, cont, ctx')` carrying `context: ExecutionContext`. In `stepToCommand`, the advancing context `ctx'` is threaded through the compensation registration, ensuring `DeterministicCounter` advances monotonically across both forward and backward steps.
* **Regression Test**: Verified via `Spindle.Tests.Sagas.SagaTests` ("Compensation coordinator executes registered compensations in LIFO order").

---

### BUG-004: Discrete Virtual Clock Step Ordering & Packet Delivery Delay {#bug-004}

* **Component**: `Spindle.Simulation.SimCluster`
* **Severity**: Medium (Packets scheduled for delivery at tick N were evaluated against tick N-1)
* **Symptom**: Messages routed through `SimCluster.RoutePacket(..., delayTicks = 1L)` were not delivered on the subsequent `Step()` invocation.
* **Root Cause**: `currentTick` was incremented at the *end* of `Step()`. When a packet was scheduled with `DeliverAtTick = currentTick + 1L = 1L`, the readiness check `p.DeliverAtTick <= currentTick` checked `1L <= 0L`, which evaluated to false. The packet was only delivered on the second step.
* **Reproduction**:
  ```fsharp
  let cluster = SimCluster(config)
  cluster.RoutePacket(n1, n2, msg, delayTicks = 1L)
  cluster.Step()
  Assert.Equal(1, cluster.MessagesRouted) // Failed: Actual was 0
  ```
* **Resolution**:
  Moved `currentTick <- currentTick + 1L` to the beginning of `Step()`. Virtual time ticks from 0 to 1 at the beginning of the step, so packets scheduled for tick 1 are delivered and processed immediately in that step.
* **Regression Test**: Verified via `Spindle.Tests.Simulation.SimulationTests` ("SimCluster routes messages across nodes under virtual clock").
