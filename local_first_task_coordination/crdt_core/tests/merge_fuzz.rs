//! Randomized (but deterministic, seeded) tests of the CRDT merge laws.
//!
//! `merge_properties.rs` checks idempotence/commutativity/associativity on a
//! handful of hand-picked graphs. This file runs the same three checks over
//! hundreds of randomly generated graphs and random overlapping splits of
//! them, to catch edge cases a few examples wouldn't stumble into (e.g. an
//! event with many parents, a task referenced by only one event, deeply
//! nested causal chains).
//!
//! The project is intentionally zero-dependency (see the top-level README),
//! so this uses a small self-contained xorshift PRNG rather than a crate
//! like `rand` — it only needs to be deterministic and reasonably
//! well-distributed, not cryptographically sound.

use crdt_core::causal_graph::CausalGraph;
use crdt_core::{Event, EventId, NodeId, Operation, TaskId};

/// Minimal deterministic PRNG (xorshift64). Reproducible from a seed; not
/// suitable for anything security-sensitive, which this isn't.
struct Xorshift64 {
    state: u64,
}

impl Xorshift64 {
    fn new(seed: u64) -> Self {
        // xorshift is undefined at state 0, so nudge it away from zero.
        Xorshift64 { state: seed.max(1) }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    /// A value in `0..n`. Panics if `n == 0`.
    fn next_range(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

/// Builds a graph of `n` events with random operations over a small pool of
/// task/node ids, each with 0-2 causal parents chosen from earlier events.
/// Ids are just `0..n`, so results are trivially comparable across graphs
/// built from the same `n` and seed.
fn random_graph(rng: &mut Xorshift64, n: u128) -> CausalGraph {
    let mut graph = CausalGraph::new();
    let mut existing_ids: Vec<u128> = Vec::new();

    for id in 0..n {
        let task_id = TaskId(rng.next_range(4) as u128);
        let node_id = NodeId(rng.next_range(4) as u128);
        let operation = match rng.next_range(3) {
            0 => Operation::Create(task_id),
            1 => Operation::Claim(task_id, node_id),
            _ => Operation::Complete(task_id, node_id),
        };

        let num_parents = if existing_ids.is_empty() {
            0
        } else {
            rng.next_range(3).min(existing_ids.len() as u64)
        };
        let mut causal_parents = Vec::new();
        for _ in 0..num_parents {
            let idx = rng.next_range(existing_ids.len() as u64) as usize;
            causal_parents.push(EventId(existing_ids[idx]));
        }

        graph.insert(Event {
            event_id: EventId(id),
            operation,
            causal_parents,
        });
        existing_ids.push(id);
    }

    graph
}

/// Splits every event of `graph` into three new graphs, each event landing
/// in a random non-empty subset of `{a, b, c}` (so overlap between the three
/// is common, mirroring how real replicas' local views overlap).
fn random_three_way_split(
    rng: &mut Xorshift64,
    graph: &CausalGraph,
) -> (CausalGraph, CausalGraph, CausalGraph) {
    let mut a = CausalGraph::new();
    let mut b = CausalGraph::new();
    let mut c = CausalGraph::new();

    for event in graph.events().values() {
        let bits = loop {
            let candidate = rng.next_range(8);
            if candidate != 0 {
                break candidate;
            }
        };
        if bits & 1 != 0 {
            a.insert(event.clone());
        }
        if bits & 2 != 0 {
            b.insert(event.clone());
        }
        if bits & 4 != 0 {
            c.insert(event.clone());
        }
    }

    (a, b, c)
}

const ITERATIONS: u64 = 150;
const EVENTS_PER_GRAPH: u128 = 20;

#[test]
fn fuzz_merge_is_idempotent() {
    for seed in 0..ITERATIONS {
        let mut rng = Xorshift64::new(seed * 2_654_435_761 + 1);
        let graph = random_graph(&mut rng, EVENTS_PER_GRAPH);

        let mut doubled = graph.clone();
        doubled.merge(&graph.clone());

        assert_eq!(
            doubled.events(),
            graph.events(),
            "seed {seed}: merge(A, A) changed events"
        );
        assert_eq!(
            doubled.children(),
            graph.children(),
            "seed {seed}: merge(A, A) changed children"
        );
    }
}

#[test]
fn fuzz_merge_is_commutative() {
    for seed in 0..ITERATIONS {
        let mut rng = Xorshift64::new(seed * 2_654_435_761 + 2);
        let graph = random_graph(&mut rng, EVENTS_PER_GRAPH);
        let (a, b, _c) = random_three_way_split(&mut rng, &graph);

        let mut left = a.clone();
        left.merge(&b);

        let mut right = b.clone();
        right.merge(&a);

        assert_eq!(
            left.events(),
            right.events(),
            "seed {seed}: merge(A, B) != merge(B, A) events"
        );
    }
}

#[test]
fn fuzz_merge_is_associative() {
    for seed in 0..ITERATIONS {
        let mut rng = Xorshift64::new(seed * 2_654_435_761 + 3);
        let graph = random_graph(&mut rng, EVENTS_PER_GRAPH);
        let (a, b, c) = random_three_way_split(&mut rng, &graph);

        // left: (A merge B) merge C
        let mut left = a.clone();
        left.merge(&b);
        left.merge(&c);

        // right: A merge (B merge C)
        let mut bc = b.clone();
        bc.merge(&c);
        let mut right = a.clone();
        right.merge(&bc);

        assert_eq!(
            left.events(),
            right.events(),
            "seed {seed}: (A merge B) merge C != A merge (B merge C) events"
        );
        assert_eq!(
            left.children(),
            right.children(),
            "seed {seed}: (A merge B) merge C != A merge (B merge C) children"
        );
    }
}

#[test]
fn fuzz_merge_converges_to_the_original_full_graph() {
    // Merging every fragment of a random 3-way split back together should
    // reconstruct exactly the original graph's events, regardless of how
    // the pieces overlapped.
    for seed in 0..ITERATIONS {
        let mut rng = Xorshift64::new(seed * 2_654_435_761 + 4);
        let graph = random_graph(&mut rng, EVENTS_PER_GRAPH);
        let (a, b, c) = random_three_way_split(&mut rng, &graph);

        let mut reconstructed = a;
        reconstructed.merge(&b);
        reconstructed.merge(&c);

        assert_eq!(
            reconstructed.events(),
            graph.events(),
            "seed {seed}: merging all split fragments didn't reconstruct the original"
        );
    }
}
