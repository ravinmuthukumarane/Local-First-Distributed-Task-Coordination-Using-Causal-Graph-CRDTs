# Local-First Distributed Task Coordination

A Rust implementation of a distributed task system where multiple nodes can create, claim, and complete tasks — without a central server. Nodes work independently, sync their state by exchanging messages, and always end up consistent, even if they were offline or missed messages.

## How it works

Every action a node takes (creating a task, claiming it, completing it) is recorded as an **event**. Events are stored in a graph where each event knows which earlier events it followed — this is the causal graph. When two nodes sync, they merge their graphs. Because the merge is designed to always produce the same result regardless of order, nodes always converge to the same state.

The three core concepts:

- **Events** — immutable records of something that happened (Create / Claim / Complete a task)
- **Causal graph** — a DAG linking each event to the events that preceded it; the source of truth for every node
- **Merge** — the sync operation; merging A into B gives the same result as merging B into A, and doing it twice changes nothing

When two nodes both claim the same task without seeing each other's claim first, the system detects the conflict and records both — it does not silently drop either side.

## Project layout

```
local_first_task_coordination/
├── crdt_core/      # Core data types: IDs, Event, Operation, CausalGraph, Message
├── task_model/     # Domain logic: extract per-task state from a graph
└── node_sim/       # Simulation: nodes, message delivery, scenarios, metrics
```

### crdt_core

The foundation. Defines `TaskId`, `NodeId`, `EventId` (all `u128` newtypes), the `Operation` enum, the `Event` struct, and the `CausalGraph` with `insert` and `merge`. Also defines `Message`, the envelope used to send events between nodes.

### task_model

Sits on top of `crdt_core`. Given a graph and a task ID, it can extract all events relevant to that task and derive a `TaskState` — whether the task exists, who has claimed it, who has completed it, and whether there is a conflict.

### node_sim

The simulation harness. Each `Node` has a local graph, an outbox (events to send), and an inbox (events received). A `Simulation` wires multiple nodes together, routes messages, and supports:

- Normal broadcast (reliable delivery)
- Broadcast with failures (drop specific nodes, simulate network partitions)
- Partition and heal
- Per-round metrics collection and structured logging

## Requirements

- [Rust](https://rustup.rs/) 1.70 or later
- No external dependencies — pure `std`

## Build

```sh
cargo build
```

## Run

```sh
cargo run
```

This runs four scenarios at seed 1 and prints a round-by-round summary to the terminal. It also writes JSON files to the current directory.

**Scenarios:**

| Scenario | What it simulates |
|---|---|
| `no_partition` | Clean network — all nodes receive every message |
| `short_partition` | One node cut off for 2 rounds, then reconnected |
| `long_partition` | One node cut off for 5 rounds, then reconnected |
| `high_contention` | All nodes isolated, each claims the same task, then reconnect |

The output also runs each scenario across seeds 1–10 and writes aggregate files.

**Output files:**

```
no_partition_seed1.json
short_partition_seed1.json
long_partition_seed1.json
high_contention_seed1.json

no_partition_multi_seed.json
short_partition_multi_seed.json
long_partition_multi_seed.json
high_contention_multi_seed.json
```

## Test

```sh
cargo test
```

84 tests covering:

- Basic correctness (insert, merge, deduplication, conflict detection)
- Algebraic properties of merge (idempotent, commutative, associative)
- Scenario-level integration tests (partition, heal, convergence)
- Semantic correctness (a task with two concurrent claims is always detected as a conflict)

## JSON output

Each single-seed file records the simulation state after every delivery round:

```json
{
  "scenario": "no_partition",
  "seed": 1,
  "rounds": [
    {
      "round": 1,
      "converged": true,
      "message_count": 3,
      "event_counts": [1, 1, 1],
      "conflict_counts": [0, 0, 0],
      "edge_counts": [0, 0, 0],
      "convergence_round": 1
    }
  ],
  "log": [
    "BROADCAST sender=0 event_count=1",
    "DELIVER node=0 events_in_graph=1",
    "SNAPSHOT label=round1 node=0 events=1 exists=true claims=0 completions=0 conflict=false"
  ]
}
```

**Field reference:**

| Field | Meaning |
|---|---|
| `event_counts` | Number of events in each node's graph after this round |
| `conflict_counts` | 1 if the node sees a conflict on the tracked task, otherwise 0 |
| `edge_counts` | Number of causal edges (parent→child links) in each node's graph |
| `convergence_round` | The first round where all nodes had identical event sets (`null` if never) |
| `log` | Ordered trace of every broadcast, delivery, and state snapshot |

Multi-seed files add a `summary` block with `min`, `max`, and `avg` convergence round across all seeds, plus `always_converged`.
