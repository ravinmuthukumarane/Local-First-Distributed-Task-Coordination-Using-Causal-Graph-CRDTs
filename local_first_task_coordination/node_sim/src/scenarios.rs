use crdt_core::{NodeId, TaskId};

use crate::json_util::{json_string, opt_usize, usize_array};
use crate::rng::Xorshift64;
use crate::simulation::{EventCounter, Metrics, Simulation};

/// Summary of one scenario run at a specific seed, used for multi-seed analysis.
pub struct SeedSummary {
    /// The seed used for event ID generation.
    pub seed: u128,
    /// Round in which all nodes first converged, or `None` if they never did.
    pub convergence_round: Option<usize>,
    /// Event counts per node in the final round.
    pub final_event_counts: Vec<usize>,
    /// Conflict indicators per node in the final round.
    pub final_conflict_counts: Vec<usize>,
    /// Total messages delivered across all rounds.
    pub total_messages: usize,
    /// Causal edge counts per node in the final round.
    pub final_edge_counts: Vec<usize>,
}

/// Delivers all in-flight messages, logs a snapshot under `label`, and records
/// a `Metrics` entry. Shared by every scenario to avoid repeating this
/// three-call sequence after each broadcast.
fn record_round(sim: &mut Simulation, task_id: &TaskId, label: &str, snapshots: &mut Vec<Metrics>) {
    sim.deliver_all();
    sim.log_state_snapshot(task_id, label);
    snapshots.push(sim.collect_metrics(task_id));
}

/// Finds the convergence round across `snapshots` and stamps it back onto
/// every entry, so callers can read `convergence_round` off any snapshot.
fn stamp_convergence_round(snapshots: &mut [Metrics]) {
    let conv = Simulation::find_convergence_round(snapshots);
    for m in snapshots.iter_mut() {
        m.convergence_round = conv;
    }
}

/// Like [`record_round`], but delivers via
/// [`Simulation::deliver_all_shuffled`] instead of [`Simulation::deliver_all`].
fn record_round_shuffled(
    sim: &mut Simulation,
    rng: &mut Xorshift64,
    task_id: &TaskId,
    label: &str,
    snapshots: &mut Vec<Metrics>,
) {
    sim.deliver_all_shuffled(rng);
    sim.log_state_snapshot(task_id, label);
    snapshots.push(sim.collect_metrics(task_id));
}

/// Runs the no-partition baseline: 3 nodes, linear Create → Claim → Complete chain.
pub fn scenario_no_partition(seed: u128) -> (Simulation, Vec<Metrics>) {
    let mut counter = EventCounter::new(seed);
    let mut sim = Simulation::new(vec![NodeId(0), NodeId(1), NodeId(2)]);
    let mut snapshots = Vec::new();
    let task_id = TaskId(1);

    let e1 = sim.nodes[0].create_task_next(task_id, &mut counter, vec![]);
    sim.broadcast(0);
    record_round(&mut sim, &task_id, "round1", &mut snapshots);

    let e2 = sim.nodes[1].claim_task_next(task_id, &mut counter, vec![e1]);
    sim.broadcast(1);
    record_round(&mut sim, &task_id, "round2", &mut snapshots);

    sim.nodes[2].complete_task_next(task_id, &mut counter, vec![e2]);
    sim.broadcast(2);
    record_round(&mut sim, &task_id, "round3", &mut snapshots);

    stamp_convergence_round(&mut snapshots);

    (sim, snapshots)
}

/// Runs the short-partition scenario: node 2 partitioned for 2 rounds, then healed.
pub fn scenario_short_partition(seed: u128) -> (Simulation, Vec<Metrics>) {
    let mut counter = EventCounter::new(seed);
    let mut sim = Simulation::new(vec![NodeId(0), NodeId(1), NodeId(2)]);
    let mut snapshots = Vec::new();
    let task_id = TaskId(1);

    sim.partition(2);

    let e1 = sim.nodes[0].create_task_next(task_id, &mut counter, vec![]);
    sim.broadcast_with_failures(0, &[]);
    record_round(&mut sim, &task_id, "round1", &mut snapshots);

    sim.nodes[1].claim_task_next(task_id, &mut counter, vec![e1]);
    sim.broadcast_with_failures(1, &[]);
    record_round(&mut sim, &task_id, "round2", &mut snapshots);

    // Heal node 2 and deliver its backlog directly into its inbox before
    // the next deliver_all — this represents the anti-entropy catch-up
    // that occurs when a previously partitioned node reconnects.
    sim.heal(2);
    let backlog: Vec<_> = sim.nodes[0].graph.events().values().cloned().collect();
    for event in backlog {
        sim.nodes[2].inbox.push(event);
    }
    sim.nodes[0].create_task_next(TaskId(2), &mut counter, vec![]);
    sim.broadcast(0);
    record_round(&mut sim, &task_id, "round3", &mut snapshots);

    stamp_convergence_round(&mut snapshots);

    (sim, snapshots)
}

/// Runs the long-partition scenario: node 2 partitioned for 5 rounds, then healed.
pub fn scenario_long_partition(seed: u128) -> (Simulation, Vec<Metrics>) {
    let mut counter = EventCounter::new(seed);
    let mut sim = Simulation::new(vec![NodeId(0), NodeId(1), NodeId(2)]);
    let mut snapshots = Vec::new();
    let task_id = TaskId(1);

    sim.partition(2);

    let e1 = sim.nodes[0].create_task_next(task_id, &mut counter, vec![]);
    sim.broadcast_with_failures(0, &[]);
    record_round(&mut sim, &task_id, "round1", &mut snapshots);

    sim.nodes[1].claim_task_next(task_id, &mut counter, vec![e1]);
    sim.broadcast_with_failures(1, &[]);
    record_round(&mut sim, &task_id, "round2", &mut snapshots);

    for round in 3usize..=5 {
        sim.step(1);
        sim.log_state_snapshot(&task_id, &format!("round{}", round));
        snapshots.push(sim.collect_metrics(&task_id));
    }

    sim.heal(2);
    let backlog: Vec<_> = sim.nodes[0].graph.events().values().cloned().collect();
    for event in backlog {
        sim.nodes[2].inbox.push(event);
    }
    sim.nodes[0].create_task_next(TaskId(2), &mut counter, vec![]);
    sim.broadcast(0);
    record_round(&mut sim, &task_id, "round6", &mut snapshots);

    stamp_convergence_round(&mut snapshots);

    (sim, snapshots)
}

/// Runs the high-contention scenario: all 3 nodes claim concurrently, producing a conflict.
pub fn scenario_high_contention(seed: u128) -> (Simulation, Vec<Metrics>) {
    let mut counter = EventCounter::new(seed);
    let mut sim = Simulation::new(vec![NodeId(0), NodeId(1), NodeId(2)]);
    let mut snapshots = Vec::new();
    let task_id = TaskId(1);

    sim.partition(0);
    sim.partition(1);
    sim.partition(2);

    sim.nodes[0].create_task_next(task_id, &mut counter, vec![]);
    sim.nodes[1].claim_task_next(task_id, &mut counter, vec![]);
    sim.nodes[2].claim_task_next(task_id, &mut counter, vec![]);

    sim.heal(0);
    sim.heal(1);
    sim.heal(2);

    sim.broadcast(0);
    record_round(&mut sim, &task_id, "round1", &mut snapshots);

    sim.broadcast(1);
    record_round(&mut sim, &task_id, "round2", &mut snapshots);

    sim.broadcast(2);
    record_round(&mut sim, &task_id, "round3", &mut snapshots);

    stamp_convergence_round(&mut snapshots);

    (sim, snapshots)
}

/// Runs a Create → Claim → Complete chain like [`scenario_no_partition`], but
/// exercises two additional capabilities together:
///
/// - **Automatic causal-parent wiring** — every event is staged via
///   `Node::create_task_auto` / `claim_task_auto` / `complete_task_auto`
///   rather than the scenario computing and passing `causal_parents` by
///   hand, so the causal chain comes entirely from each node's own local
///   view (see [`crate::node::Node::frontier_for`]).
/// - **Randomized delivery order** — each round is delivered via
///   [`Simulation::deliver_all_shuffled`] with a seeded RNG instead of
///   [`Simulation::deliver_all`], so messages are inserted into each node's
///   graph in a randomized order rather than broadcast order.
///
/// Convergence and the absence of a conflict are still expected: neither
/// property depends on parents being wired by hand or on delivery order,
/// which is exactly the robustness the CRDT design is meant to provide.
pub fn scenario_auto_frontier_reordered(seed: u128) -> (Simulation, Vec<Metrics>) {
    let mut counter = EventCounter::new(seed);
    let mut rng = Xorshift64::new(seed as u64);
    let mut sim = Simulation::new(vec![NodeId(0), NodeId(1), NodeId(2)]);
    let mut snapshots = Vec::new();
    let task_id = TaskId(1);

    sim.nodes[0].create_task_auto(task_id, &mut counter);
    sim.broadcast(0);
    record_round_shuffled(&mut sim, &mut rng, &task_id, "round1", &mut snapshots);

    sim.nodes[1].claim_task_auto(task_id, &mut counter);
    sim.broadcast(1);
    record_round_shuffled(&mut sim, &mut rng, &task_id, "round2", &mut snapshots);

    sim.nodes[2].complete_task_auto(task_id, &mut counter);
    sim.broadcast(2);
    record_round_shuffled(&mut sim, &mut rng, &task_id, "round3", &mut snapshots);

    stamp_convergence_round(&mut snapshots);

    (sim, snapshots)
}

/// Runs a named scenario for each seed and returns a `SeedSummary` per run.
///
/// Valid `scenario_name` values: `"no_partition"`, `"short_partition"`,
/// `"long_partition"`, `"high_contention"`, `"auto_frontier_reordered"`.
pub fn run_multi_seed(scenario_name: &str, seeds: &[u128]) -> Vec<SeedSummary> {
    seeds
        .iter()
        .map(|&seed| {
            let (_, metrics) = match scenario_name {
                "no_partition" => scenario_no_partition(seed),
                "short_partition" => scenario_short_partition(seed),
                "long_partition" => scenario_long_partition(seed),
                "high_contention" => scenario_high_contention(seed),
                "auto_frontier_reordered" => scenario_auto_frontier_reordered(seed),
                other => panic!("unknown scenario: {}", other),
            };
            let last = metrics.last().expect("scenario produced no metrics");
            SeedSummary {
                seed,
                convergence_round: last.convergence_round,
                final_event_counts: last.event_counts.clone(),
                final_conflict_counts: last.conflict_counts.clone(),
                total_messages: last.message_count,
                final_edge_counts: last.edge_counts.clone(),
            }
        })
        .collect()
}

pub fn print_multi_seed_summary(summaries: &[SeedSummary]) {
    crate::print_table_header(&[
        ("seed", 6),
        ("convergence_round", 17),
        ("total_messages", 15),
        ("conflicts", 9),
    ]);
    for s in summaries {
        let conv = match s.convergence_round {
            Some(r) => r.to_string(),
            None => "none".to_string(),
        };
        let any_conflict: usize = s.final_conflict_counts.iter().sum();
        println!(
            "  {:>6} | {:>17} | {:>15} | {:?}",
            s.seed,
            conv,
            s.total_messages,
            any_conflict > 0
        );
    }
}

pub fn write_multi_seed_json(
    scenario_name: &str,
    summaries: &[SeedSummary],
) -> std::io::Result<()> {
    std::fs::create_dir_all("results")?;
    let filename = format!("results/{}_multi_seed.json", scenario_name);

    let runs_json = summaries
        .iter()
        .map(|s| {
            format!(
                "    {{\n      \"seed\": {},\n      \"convergence_round\": {},\n      \"total_messages\": {},\n      \"final_event_counts\": {},\n      \"final_conflict_counts\": {},\n      \"final_edge_counts\": {}\n    }}",
                s.seed,
                opt_usize(s.convergence_round),
                s.total_messages,
                usize_array(&s.final_event_counts),
                usize_array(&s.final_conflict_counts),
                usize_array(&s.final_edge_counts),
            )
        })
        .collect::<Vec<_>>()
        .join(",\n");

    let converged_rounds: Vec<usize> = summaries
        .iter()
        .filter_map(|s| s.convergence_round)
        .collect();
    let always_converged = !summaries.is_empty() && converged_rounds.len() == summaries.len();
    let (min_conv, max_conv, avg_conv) = if converged_rounds.is_empty() {
        ("null".to_string(), "null".to_string(), "null".to_string())
    } else {
        let min = converged_rounds.iter().min().unwrap();
        let max = converged_rounds.iter().max().unwrap();
        let avg = converged_rounds.iter().sum::<usize>() as f64 / converged_rounds.len() as f64;
        (min.to_string(), max.to_string(), format!("{:.2}", avg))
    };

    let summary_json = format!(
        "  \"summary\": {{\n    \"min_convergence_round\": {},\n    \"max_convergence_round\": {},\n    \"avg_convergence_round\": {},\n    \"always_converged\": {}\n  }}",
        min_conv, max_conv, avg_conv, always_converged,
    );

    let json = format!(
        "{{\n  \"scenario\": {},\n  \"runs\": [\n{}\n  ],\n{}\n}}",
        json_string(scenario_name),
        runs_json,
        summary_json,
    );

    std::fs::write(&filename, json)?;
    println!("Written: {}", filename);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scenario_no_partition_converges() {
        let (_, snapshots) = scenario_no_partition(1);
        assert!(snapshots.last().unwrap().converged);
    }

    #[test]
    fn scenario_short_partition_converges() {
        let (_, snapshots) = scenario_short_partition(1);
        assert!(snapshots.last().unwrap().converged);
    }

    #[test]
    fn scenario_long_partition_converges() {
        let (_, snapshots) = scenario_long_partition(1);
        assert!(snapshots.last().unwrap().converged);
    }

    #[test]
    fn scenario_high_contention_converges_and_shows_conflict() {
        let (_, snapshots) = scenario_high_contention(1);
        let final_snap = snapshots.last().unwrap();
        assert!(final_snap.converged);
        assert!(final_snap.conflict_counts.iter().any(|&c| c > 0));
    }

    #[test]
    fn scenario_auto_frontier_reordered_converges_with_no_conflict() {
        let (sim, snapshots) = scenario_auto_frontier_reordered(1);
        let final_snap = snapshots.last().unwrap();

        assert!(final_snap.converged);
        assert!(final_snap.conflict_counts.iter().all(|&c| c == 0));

        for i in 0..sim.nodes.len() {
            let state = sim.derive_state(i, &TaskId(1));
            assert!(state.exists);
            assert_eq!(state.claims.len(), 1);
            assert_eq!(state.completions.len(), 1);
            assert_eq!(state.resolved_claimant, state.claims.first().copied());
        }
    }

    #[test]
    fn scenario_auto_frontier_reordered_is_reproducible_across_runs() {
        // Same seed must give byte-identical results, including the shuffle
        // order derived from it, so the evaluation stays reproducible.
        let (_, snapshots_a) = scenario_auto_frontier_reordered(7);
        let (_, snapshots_b) = scenario_auto_frontier_reordered(7);

        let event_counts_a: Vec<_> = snapshots_a.iter().map(|m| m.event_counts.clone()).collect();
        let event_counts_b: Vec<_> = snapshots_b.iter().map(|m| m.event_counts.clone()).collect();
        assert_eq!(event_counts_a, event_counts_b);
    }

    #[test]
    fn scenario_auto_frontier_reordered_different_seeds_still_converge() {
        // Different seeds shuffle delivery differently; convergence must
        // hold regardless, since CRDT merge is commutative and idempotent.
        for seed in 1..=10u128 {
            let (_, snapshots) = scenario_auto_frontier_reordered(seed);
            assert!(
                snapshots.last().unwrap().converged,
                "seed {seed} failed to converge"
            );
        }
    }
}
