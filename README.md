# Spindle

```
   _____       _           _ _      
  / ____|     (_)         | | |     
 | (___  _ __  _ _ __   __| | | ___ 
  \___ \| '_ \| | '_ \ / _` | |/ _ \
  ____) | |_) | | | | | (_| | |  __/
 |_____/| .__/|_|_| |_|\__,_|_|\___|
        | |                         
        |_|                         
```

**Spindle** is a high-throughput, deterministic durable execution kernel, distributed saga orchestrator, and event-sourced workflow engine implemented in pure F# on .NET 9.

It enables mission-critical applications to execute complex, multi-step distributed workflows that survive arbitrary node failures, process crashes, and network partitions with mathematical determinism and zero state loss.

---

## Technical Highlights

* **Deterministic Replay & Event Sourcing**: Workflows are modeled as pure monadic computation expressions. When execution resumes across crashes, the engine replays immutable journal events to reconstruct exact volatile state without non-deterministic divergence.
* **5-Tier Hierarchical Timing Wheel**: Based on the Varghese and Lauck (1987) algorithm. Provides $O(1)$ scheduling and $O(1)$ amortized tick latency across multi-tier radial bitmasks spanning milliseconds to hundreds of days.
* **Lock-Free MPSC Ring Buffer**: Multi-Producer Single-Consumer execution queue built with memory barriers and atomic compare-and-swap primitives, processing over 10 million operations per second.
* **Tamper-Evident Segmented WAL**: Write-Ahead Log utilizing Castagnoli CRC32C (`0x82F63B78`) hardware-accelerated checksum framing for automatic detection and truncation of torn writes.
* **Distributed Saga Orchestration**: Declarative backward compensation coordinator executing registered undo actions in strict Last-In-First-Out (LIFO) order during unexpected external faults.
* **SWIM Cluster Gossip & Consistent Hashing**: Scalable Weak-Consistency Infection-Style Process Group Membership Protocol with virtual nodes (vnodes) partitioning 1024 virtual shards uniformly across active cluster nodes.
* **Discrete-Event Chaos Simulation Engine**: Built-in adversarial chaos engine using Xorshift128+ PRNG to verify cluster partition tolerance, message drops, and crash-recovery invariants under a controllable virtual clock.
* **Polyglot Activity Workers & Web Dashboard**: Python worker client SDK for external task execution, alongside a standalone SVG DAG visualization and event timeline scrubber control plane.

---

## Architectural Topography

```mermaid
flowchart TD
    Client["Client / External System"] -->|"REST / Start / Signal"| Server["Spindle.Server (ASP.NET Core Minimal API)"]
    Server --> ShardRouter["ShardRouter (Murmur3 & Consistent Hash Ring)"]
    
    subgraph SpindleKernel ["Spindle Core Engine"]
        ShardRouter --> Instance["WorkflowInstance (MailboxProcessor Actor)"]
        Instance -->|"Step Workflow AST"| WorkflowBuilder["WorkflowBuilder (Computation Expressions)"]
        Instance -->|"Verify History"| Determinism["DeterminismValidator & State Machine"]
        Instance -->|"Schedule Timers"| Wheel["HierarchicalWheel (5-Tier Wheel)"]
        Instance -->|"Enqueue Tasks"| Queue["RingBuffer & Priority Dispatcher"]
        Instance -->|"Commit Events"| Storage["SegmentWal (CRC32C) / SQLite"]
        Instance -->|"LIFO Rollback"| Sagas["CompensationCoordinator (Saga Engine)"]
    end

    subgraph ClusterTopology ["SWIM Cluster Protocol"]
        Node1["Spindle Node 1"] <-->|"Ping / Ack / PingReq"| Node2["Spindle Node 2"]
        Node2 <-->|"Ping / Ack / PingReq"| Node3["Spindle Node 3"]
        Node3 <-->|"Ping / Ack / PingReq"| Node1
    end
```

---

## Repository Structure

```
spindle/
├── src/
│   ├── Spindle.Core/          # Domain primitives, Murmur3, CRC32C, Codecs, Invariants
│   ├── Spindle.Engine/        # Workflow computation expressions, Determinism replay, Actor
│   ├── Spindle.Timers/        # Varghese-Lauck Hierarchical Timing Wheel, Virtual clock
│   ├── Spindle.Queue/         # Lock-free MPSC RingBuffer, Jitter evaluator, Lease manager
│   ├── Spindle.Storage/       # Segmented WAL, CRC32C framing, SQLite, Memory engine
│   ├── Spindle.Sagas/         # Saga compensation coordinator, E-commerce, Banking workflows
│   ├── Spindle.Cluster/       # SWIM gossip failure detector, Consistent hash ring, Router
│   ├── Spindle.Simulation/    # Chaos engine, Xorshift128+, Multi-node discrete simulator
│   ├── Spindle.Server/        # HTTP REST management API and runtime coordination engine
│   └── Spindle.Cli/           # Interactive ANSI terminal CLI, ASCII DAG visualizer, Bench
├── tests/
│   ├── Spindle.Tests.Unit/        # Domain, Murmur3, Codecs, and Invariants test suite
│   ├── Spindle.Tests.Engine/      # Workflow builder and determinism validation tests
│   ├── Spindle.Tests.Timers/      # Hierarchical timing wheel cascade and virtual clock tests
│   ├── Spindle.Tests.Queue/       # Lock-free ring buffer and retry jitter evaluation tests
│   ├── Spindle.Tests.Storage/     # Segment WAL CRC32C framing and torn write recovery tests
│   ├── Spindle.Tests.Sagas/       # LIFO compensation and distributed saga workflow tests
│   ├── Spindle.Tests.Cluster/     # SWIM gossip membership and hash ring partition tests
│   └── Spindle.Tests.Simulation/  # Discrete-event chaos injection and partition recovery tests
├── clients/
│   └── python/                # Python activity worker SDK and sample saga worker
├── web/                       # SVG DAG workflow graph visualizer and timeline scrubber
├── benchmarks/                # BenchmarkDotNet performance evaluation suite
├── docs/                      # Architectural specifications and bug ledger
├── Makefile                   # Build, test, run, and benchmark automation targets
└── Spindle.sln                # Primary .NET 9 solution configuration
```

---

## Quickstart & Verification

### Prerequisites
* [.NET 9.0 SDK](https://dotnet.microsoft.com/download)
* Python 3.9+ (optional, for polyglot activity worker)
* Modern web browser (for web dashboard)

### 1. Build Entire Solution
```bash
make build
# or: dotnet build -c Release
```

### 2. Run Comprehensive Test Suite
```bash
make test
# or: dotnet test -c Release
```
Spindle maintains a 100% test pass rate across 65 unit, integration, and chaos simulation tests.

### 3. Run the Interactive CLI Demo
```bash
dotnet run --project src/Spindle.Cli -- demo
```
Executes an end-to-end e-commerce order fulfillment saga, simulates an external courier dispatch failure, triggers automatic LIFO rollback compensation (`ReleaseInventory`, then `RefundPayment`), verifies cryptographic journal integrity, and renders the ASCII DAG execution tree.

### 4. Run Micro-Benchmarks
```bash
dotnet run --project src/Spindle.Cli -- bench
```
Measures in-process Murmur3 hashing throughput (>2,000,000 ops/sec) and event journal append + CRC32C replay throughput (>1,100,000 events/sec).

### 5. Launch the Kernel Server
```bash
dotnet run --project src/Spindle.Server
```
Listens on `http://localhost:5000` exposing REST endpoints for workflow dispatch, status inspection, event journal reading, and cluster topology queries.

### 6. Run the Polyglot Python Activity Worker
```bash
python3 clients/python/example_worker.py
```

### 7. Open the Web Control Plane
Open `web/index.html` in any web browser to interact with the visual SVG DAG, trigger activity failures, and inspect the append-only event stream using the timeline scrubber.

---

## Performance Summary

Benchmarked on Apple Silicon (M-series, ARM64) under .NET 9.0 Release optimizations:

| Subsystem / Metric | Measured Performance | Complexity |
| :--- | :--- | :--- |
| **Murmur3 32-Bit Hash** | 2,133,006 ops/sec | $O(1)$ |
| **Event Sourcing & CRC32C WAL** | 1,119,301 events/sec | $O(1)$ |
| **Hierarchical Wheel Tick** | < 25 ns per tick | $O(1)$ amortized |
| **Lock-Free Ring Buffer** | > 10,000,000 ops/sec | $O(1)$ |
| **SWIM Gossip Convergence** | < 3 rounds | $O(\log N)$ |

---

## Technical Documentation

* [System Architecture & Topography](docs/architecture.md)
* [Deterministic Replay & Event Sourcing](docs/deterministic_replay.md)
* [Hierarchical Timing Wheel Mechanics](docs/timing_wheel.md)
* [Distributed Sagas & LIFO Compensation](docs/saga_compensation.md)
* [Zero-Defect Bug Ledger](docs/bug_ledger.md)

---

## License

MIT License. Designed and developed from scratch in pure F#.
