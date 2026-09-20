# Local-First Distributed Task Coordination Using Causal-Graph CRDTs

---

## 1. Project title and purpose

**Title:** Local-First Distributed Task Coordination Using Causal-Graph CRDTs

**Purpose.** This project implements and evaluates a *local-first*, serverless
mechanism for coordinating tasks across many independent nodes. Each node can
**create**, **claim**, and **complete** tasks entirely offline, then reconcile
its state with other nodes purely by exchanging messages — with no central
server, no coordinator, and no locking.

The coordination substrate is a **Conflict-free Replicated Data Type (CRDT)**
built on a **causal graph**: a directed acyclic graph in which every recorded
action (an *event*) points back to the events it causally depended on. The
merge operation over these graphs is **idempotent, commutative, and
associative**, so every node that has seen the same set of events converges to
the same state regardless of the order or timing in which messages arrive —
including after network partitions and offline periods (Strong Eventual
Consistency).

The deliverable is:

1. A reusable CRDT + task-model **library** (the coordination logic), and
2. A deterministic **simulation harness** that drives the library through a set
   of network scenarios (clean network, short/long partitions, high
   contention, and reordered delivery with auto-wired causal parents),
   collects convergence and conflict metrics, and writes them to JSON for
   analysis.

Concurrent, conflicting actions (e.g. two nodes claiming the same task without
having seen each other) are **never silently dropped** — the system records
both sides and flags the conflict, which is the property the evaluation is
designed to demonstrate.

---

## 2. Research context

This is a systems / distributed-computing dissertation artefact. The research
question concerns whether a causal-graph CRDT can provide correct, convergent
task coordination for local-first applications under adverse network
conditions. There is **no machine-learning component** — see
[Section 13, Not applicable](#13-not-applicable-to-this-project) for what
that means for the project's scope.

---

## 3. Programming languages, libraries and frameworks used

| Concern | Choice |
|---|---|
| Language | **Rust** (edition 2024) |
| Build system / package manager | **Cargo** (ships with Rust) |
| Runtime libraries | **None** — the entire system uses only the Rust standard library (`std`). There are zero third-party crate dependencies. |
| Test framework | Rust's built-in `#[test]` harness (`cargo test`) |
| Data format for results | JSON (hand-serialised via `std`; no serde dependency) |

The absence of external dependencies is deliberate: it keeps the CRDT
correctness argument self-contained and makes the build fully reproducible
offline. Where a dependency would ordinarily be reached for — JSON output, a
PRNG for the reordered-delivery scenario — the project uses a small
self-contained implementation instead (`node_sim/src/json_util.rs`,
`node_sim/src/rng.rs`) rather than pulling in `serde_json` or `rand`.

---

## 4. Software and hardware requirements

**Software**

- **Rust toolchain 1.85 or newer** (required because the crates use *edition
  2024*). Verified on `rustc` / `cargo` 1.94. Install via
  [rustup](https://rustup.rs/).
- A supported operating system: **macOS, Linux, or Windows** (developed and
  tested on macOS / Darwin).
- No database engine, container runtime, or network access is required.

**Hardware**

- Any machine capable of running the Rust toolchain.
- The simulation is single-threaded, in-memory, and finishes in well under a
  second. Memory footprint is a few megabytes; roughly **150–300 MB of disk**
  is needed for the Rust toolchain and the compiled `target/` directory.

---

## 5. Repository structure

```
Local-First-Distributed-Task-Coordination-Using-Causal-Graph-CRDTs/
├── README.md                        # ← project overview and usage guide
└── local_first_task_coordination/   # The Cargo workspace (all source code lives here)
    ├── Cargo.toml                   # Workspace manifest
    ├── Cargo.lock                   # Pinned build (reproducibility)
    ├── results/                     # All generated result JSON (the single output location)
    │   ├── *_seed1.json             #   Per-round trace for each scenario at seed 1
    │   └── *_multi_seed.json        #   Aggregate across seeds 1–10 for each scenario
    ├── crdt_core/                   # Core CRDT data types
    │   ├── src/lib.rs               #   IDs, Operation, Event
    │   ├── src/causal_graph.rs      #   CausalGraph: insert + merge (the CRDT join)
    │   ├── src/message.rs           #   Message envelope for node-to-node sync
    │   └── tests/                   #   merge_properties.rs, merge_fuzz.rs, causal_graph_integration.rs
    ├── task_model/                  # Domain logic on top of the CRDT
    │   ├── src/lib.rs               #   extract_task_subgraph, derive_task_state, resolve (conflict resolution)
    │   └── tests/semantic_validation.rs
    └── node_sim/                    # Simulation harness (the runnable binary)
        ├── src/node.rs              #   Node: local graph + inbox/outbox + causal frontier tracking
        ├── src/simulation.rs        #   Simulation: routing, partitions, metrics, logging
        ├── src/scenarios.rs         #   The five experiment scenarios + multi-seed runner
        ├── src/json_util.rs         #   Hand-rolled JSON string/array escaping helpers
        ├── src/rng.rs               #   Deterministic xorshift PRNG (reordered-delivery scenario)
        └── src/main.rs              #   Entry point: runs all scenarios, writes JSON
```

All generated output lives in **one place**: `local_first_task_coordination/results/`.
The ten files there are committed as sample output / test evidence; re-running
the program overwrites them with byte-identical content (the run is
deterministic).

**The three crates (a layered design):**

- **`crdt_core`** — the foundation. Defines `TaskId`, `NodeId`, `EventId` (all
  `u128` newtypes, `Copy`), the `Operation` enum (`Create` / `Claim` /
  `Complete`), the immutable `Event` struct, the append-only `CausalGraph`
  with `insert` and `merge`, and the `Message` envelope used to ship events
  between nodes. `CausalGraph`'s `events`/`children` maps are private —
  callers read them through accessors (`events()`, `children_of()`, …) so the
  edge map can't be pushed out of sync with the events it indexes. A
  `by_task` index, maintained incrementally on `insert`, lets task-scoped
  lookups (used by `task_model::extract_task_subgraph`) skip scanning every
  event in the graph.
- **`task_model`** — sits on `crdt_core`. Given a graph and a task ID it
  extracts the task's sub-graph and derives a `TaskState` (does the task
  exist, who claimed it, who completed it, is there a conflict). It also
  **resolves** conflicts, not just detects them: `TaskState.resolved_claimant`
  / `resolved_completer` give the single `NodeId` every node converges on as
  the winner — a claim that causally supersedes all others wins outright; a
  genuine (concurrent) conflict is broken deterministically by lowest
  `NodeId` — so every node picks the same winner regardless of delivery order.
- **`node_sim`** — the experiment harness. A `Node` owns a local graph plus an
  outbox/inbox; a `Simulation` wires several nodes together, routes messages,
  can partition and heal the network, and collects per-round `Metrics`. A
  `Node` can also track its own **causal frontier** per task
  (`frontier_for`) and auto-wire `causal_parents` from it
  (`create_task_auto` / `claim_task_auto` / `complete_task_auto`), so a
  scenario no longer has to compute causal parents by hand. `Simulation`
  additionally supports **randomized delivery order**
  (`deliver_all_shuffled`, backed by a small seeded PRNG in `rng.rs`) to
  demonstrate convergence under out-of-order arrival empirically rather than
  only asserting it from the merge-law tests.

---

## 6. Installation procedure

### Step 1 — Install the Rust toolchain

If Rust is not already installed:

```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
# then follow the prompts, and restart your shell (or `source ~/.cargo/env`)
```

Verify the version is 1.85 or newer:

```sh
rustc --version
cargo --version
```

### Step 2 — Obtain the code

Clone the repository:

```sh
git clone <repository-url>
cd Local-First-Distributed-Task-Coordination-Using-Causal-Graph-CRDTs
```

### Step 3 — Enter the workspace and build

**All Cargo commands must be run from inside the `local_first_task_coordination`
directory** (that is where the workspace `Cargo.toml` lives):

```sh
cd local_first_task_coordination
cargo build
```

### Dependency installation

**None required.** The project has no third-party dependencies. `cargo build`
compiles only the three local crates against the Rust standard library. `Cargo.lock`
is committed so the build is fully reproducible with no network access.

---

## 7. Configuration requirements

There are **no configuration files, environment variables, or secrets**. The
experiment parameters are set directly in code and are intentionally simple:

| Parameter | Where | Default |
|---|---|---|
| Single-run seed | `node_sim/src/main.rs` (`let seed = 1u128;`) | `1` |
| Multi-seed range | `node_sim/src/main.rs` (`let seeds = (1..=10)`) | seeds `1..=10` |
| Scenario set | `node_sim/src/scenarios.rs` | five scenarios (see below) |

The **seed** drives a deterministic `EventCounter`, so re-running with the same
seed reproduces byte-identical results — this is what makes the evaluation
repeatable.

---

## 8. Instructions for running the system

From inside `local_first_task_coordination/`:

```sh
cargo run
```

This:

1. Runs the five scenarios at **seed 1**, printing a round-by-round summary to
   the terminal.
2. Re-runs each scenario across **seeds 1–10** and prints an aggregate table.
3. Writes all JSON result files into a **`results/` directory** (created
   automatically), relative to where you run the command. Running from inside
   `local_first_task_coordination/` — as instructed — puts them in
   `local_first_task_coordination/results/`.

### Scenarios

| Scenario | What it simulates |
|---|---|
| `no_partition` | Clean network — every node receives every message; linear Create → Claim → Complete |
| `short_partition` | One node cut off for 2 rounds, then reconnected and caught up |
| `long_partition` | One node cut off for 5 rounds, then reconnected and caught up |
| `high_contention` | All nodes isolated, each claims the same task, then reconnect → conflict is detected |
| `auto_frontier_reordered` | Same Create → Claim → Complete chain as `no_partition`, but `causal_parents` are auto-wired from each node's own causal frontier instead of by hand, and every round is delivered in a seeded-random order instead of broadcast order — demonstrating that convergence holds regardless of both |

### Output files

Per-seed traces (in `results/`):

```
no_partition_seed1.json              long_partition_seed1.json
short_partition_seed1.json           high_contention_seed1.json
auto_frontier_reordered_seed1.json
```

Multi-seed aggregates (in `results/`):

```
no_partition_multi_seed.json              long_partition_multi_seed.json
short_partition_multi_seed.json           high_contention_multi_seed.json
auto_frontier_reordered_multi_seed.json
```

Pre-generated copies of all ten files are committed in
`local_first_task_coordination/results/` as sample output/test evidence, so
results can be inspected without rebuilding.

---

## 9. Testing and test evidence

Run the full suite from inside `local_first_task_coordination/`:

```sh
cargo test
```

**Expected result: 125 tests, all passing** (verified with the toolchain in
Section 4), distributed as:

| Location | Tests | Focus |
|---|---:|---|
| `crdt_core` unit tests | 18 | IDs, `Event`, `insert`, `merge`, dedup, `Message`, the `by_task` index |
| `crdt_core/tests/merge_properties.rs` | 3 | merge is **idempotent, commutative, associative** (the CRDT laws), on hand-picked graphs |
| `crdt_core/tests/merge_fuzz.rs` | 4 | the same CRDT laws, re-checked over 150 random graphs per test (600 total checks) |
| `crdt_core/tests/causal_graph_integration.rs` | 6 | multi-event graph behaviour |
| `task_model` unit tests | 20 | sub-graph extraction, state derivation, causal-ancestry conflict detection, deterministic conflict *resolution* |
| `task_model/tests/semantic_validation.rs` | 6 | end-to-end semantics (e.g. two concurrent claims → conflict) |
| `node_sim` unit tests | 68 | node buffers, broadcast/deliver, partition/heal, metrics, logging, JSON escaping, causal frontier tracking, the seeded PRNG, the reordered-delivery scenario |
| **Total** | **125** | |

The pre-generated JSON files in `local_first_task_coordination/results/` are the
recorded output of a successful run and serve as reproducible test evidence.

---

## 10. Understanding the output (result-file schema)

Each **single-seed** file records the simulation state after every delivery
round, plus an ordered event log:

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

| Field | Meaning |
|---|---|
| `event_counts` | Number of events in each node's graph after this round |
| `conflict_counts` | `1` if the node sees a conflict on the tracked task, else `0` (per node) |
| `edge_counts` | Number of causal edges (parent→child links) in each node's graph |
| `converged` | `true` when every node holds an identical set of event IDs |
| `convergence_round` | First round in which all nodes converged (`null` if never) |
| `log` | Ordered trace of every broadcast, delivery, and state snapshot |

**Multi-seed** files replace `rounds` with one entry per seed and add a
`summary` block reporting `min` / `max` / `avg` convergence round across all
seeds plus `always_converged`.

---

## 11. How it works (core concepts)

- **Event** — an immutable record of one action (`Create` / `Claim` /
  `Complete`) plus the list of event IDs it causally depended on.
- **Causal graph** — an append-only DAG of events; each node's local source of
  truth. Edges for a not-yet-received parent are still recorded, so
  out-of-order delivery is handled gracefully.
- **Merge** — the CRDT join. Merging graph *A* into *B* gives the same result as
  merging *B* into *A*, and merging twice changes nothing. This is what
  guarantees convergence.
- **Conflict detection** — a task with more than one *concurrent* claim (or
  completion) is derived as a conflict; both sides are retained, never
  overwritten. Concurrency is decided from the causal graph itself: two
  claims conflict only if neither is a causal ancestor of the other, so a
  legitimate sequential re-claim (e.g. staged after the original claimant's
  `Complete`) is not mistaken for a conflict.
- **Conflict resolution** — beyond detecting a conflict, `task_model::resolve`
  picks a single deterministic winner: among the claims nothing else
  causally supersedes, the lowest `NodeId` wins. Every node that has
  observed the same events resolves to the same winner regardless of the
  order the events arrived in — the resolution is exposed on `TaskState` as
  `resolved_claimant` / `resolved_completer`.
- **Causal frontier tracking** — a `Node` can compute its own frontier for a
  task (the events it knows of that nothing else it knows of causally
  follows) and use it to auto-wire new events' `causal_parents`, instead of
  a scenario computing them by hand.
- **Delivery-order independence** — because merge is commutative and
  idempotent, convergence does not depend on the order messages are
  delivered in. The `auto_frontier_reordered` scenario delivers messages in
  a seeded-random order each round to demonstrate this empirically rather
  than only asserting it from the merge-law tests.

---

## Instructions for training or evaluating models

**Not applicable** — this project contains no machine-learning models. The
equivalent "evaluation" step is running the scenario simulations and inspecting
the convergence/conflict metrics; see Sections 8–10.

---

## Default user credentials or test accounts

**Not applicable** — the system is a standalone, offline simulation with no
users, authentication, sessions, or accounts.

---

## 12. Known limitations

- **Simulation, not a live network.** Nodes, message delivery, and partitions
  are modelled in a single deterministic in-memory process. There is no real
  transport (TCP/UDP), no serialization-over-the-wire, and no wall-clock
  timing.
- **Manual causal parents in most scenarios.** Event IDs always come from a
  seeded counter (`Node::create_task_next` / `claim_task_next` /
  `complete_task_next`, or the `_auto` variants below, draw directly from
  it). `Node::frontier_for` and the `create_task_auto` / `claim_task_auto` /
  `complete_task_auto` methods can auto-wire `causal_parents` from a node's
  own local view instead — used by the `auto_frontier_reordered` scenario —
  but the four original scenarios still wire `causal_parents` by hand, since
  their specific hand-crafted DAG shapes (e.g. the diamond in
  `high_contention`) are part of what they're demonstrating.
- **Conflict resolution is a fixed policy, not pluggable.** `task_model::resolve`
  (surfaced as `TaskState.resolved_claimant` / `resolved_completer`) picks a
  winner deterministically — lowest `NodeId` breaks a genuine tie — but that
  policy is not configurable per application. A consumer wanting a different
  tie-break (e.g. by timestamp, or a priority order) has to implement it
  against the maximal-claims logic itself.
- **Unbounded, ever-growing graph.** Events are append-only with no compaction,
  garbage collection, or snapshotting, so memory grows with history. This is
  acceptable for the bounded experiments here but not for long-lived
  deployment.
- **Hand-written JSON serialisation.** Result files are produced by manual
  string formatting (no serde). Output is valid JSON for these fixed shapes but
  is not a general-purpose serialiser.
- **Fixed topology.** Scenarios use three nodes and a single tracked task;
  scaling parameters are changed by editing the source, not via configuration.

---

## 13. Not applicable to this project

This is a systems research artefact rather than a data-science /
web-application project, so several items commonly expected of a software
project don't apply here, flagged for completeness:

| Checklist item | Status |
|---|---|
| Model training / evaluation scripts | **N/A** — no ML models |
| Data preprocessing / feature engineering | **N/A** — input data is generated deterministically from seeds, not ingested |
| Front-end / back-end / database components | **N/A** — no UI, server, or database; the system is a library + CLI simulation |
| Database scripts and schemas | **N/A** — no persistent datastore |
| Trained model files | **N/A** |
| Sample input data / dataset preparation | The "dataset" is the generated simulation input; the ten committed `results/*.json` files are sample **output**. No external dataset download is required. |
| Configuration files | **N/A** — parameters are in-code (Section 7) |
| API integration instructions | **N/A** — no external or internal API |
| Deployment configuration | **N/A** — runs locally via `cargo run` |
| External services or API keys | **None required** — the project makes no network calls |

---

## 14. License

Source-available under the [PolyForm Noncommercial License 1.0.0](LICENSE):
free to use, modify, and distribute for personal, academic, and research
purposes. Use in a commercial product or service requires a separate
commercial license — contact ravinmuthukumarane@gmail.com. See
[LICENSE](LICENSE) for the full terms.

---

## 15. Quick start (summary)

```sh
# 1. Ensure Rust ≥ 1.85 is installed (https://rustup.rs/)
rustc --version

# 2. Enter the workspace
cd local_first_task_coordination

# 3. Build, test, run
cargo build
cargo test      # 125 tests, all passing
cargo run       # runs all scenarios and writes result JSON to the current directory
```
