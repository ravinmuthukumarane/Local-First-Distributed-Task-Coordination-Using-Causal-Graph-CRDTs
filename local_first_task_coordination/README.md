# Local-First Distributed Task Coordination — Cargo Workspace

This is the Rust workspace for the project. For the full submission
documentation (purpose, requirements, installation, scenarios, result schema,
known limitations), see the **[top-level README](../README.md)**.

A distributed task system where multiple nodes create, claim, and complete
tasks without a central server. Nodes work independently, sync by exchanging
messages, and converge to the same state via a causal-graph CRDT — even after
being offline or partitioned. Concurrent conflicting claims are detected and
recorded, never silently dropped.

## Crates

```
crdt_core/   # Core data types: IDs, Event, Operation, CausalGraph (insert + merge), Message
task_model/  # Domain logic: extract per-task subgraph, derive TaskState + conflicts
node_sim/    # Simulation: Node, message routing, partition/heal, scenarios, metrics (the binary)
```

- **`crdt_core`** — `TaskId` / `NodeId` / `EventId` (`u128` newtypes), the
  `Operation` enum, the immutable `Event`, the append-only `CausalGraph`, and
  the `Message` envelope. `merge` is idempotent, commutative, and associative.
- **`task_model`** — given a graph and a task ID, extracts the task's events and
  derives whether it exists, who claimed/completed it, and whether it conflicts.
- **`node_sim`** — each `Node` has a local graph, an outbox, and an inbox; a
  `Simulation` routes messages, supports reliable broadcast, broadcast with
  failures, partition/heal, and per-round metrics + structured logging.

## Requirements

- **Rust 1.85 or later** (crates use edition 2024). Install via
  [rustup](https://rustup.rs/).
- No external dependencies — pure `std`.

## Build, test, run

```sh
cargo build
cargo test   # 84 tests, all passing
cargo run    # runs 4 scenarios (seed 1) + a 1–10 seed sweep; writes result JSON to ./results/
```

`cargo run` prints a round-by-round summary and writes per-seed and multi-seed
JSON files into a `results/` directory (created automatically). Committed sample
copies live in [results/](results/). See the [top-level README](../README.md)
for the scenario list and the JSON field reference.
