use crdt_core::{NodeId, TaskId};

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

/// Runs the no-partition baseline: 3 nodes, linear Create → Claim → Complete chain.
pub fn scenario_no_partition(seed: u128) -> (Simulation, Vec<Metrics>) {
    let mut counter = EventCounter::new(seed);
    let mut sim = Simulation::new(vec![NodeId(0), NodeId(1), NodeId(2)]);
    let mut snapshots = Vec::new();

    let e1 = counter.next_id();
    sim.nodes[0].create_task(TaskId(1), e1.clone(), vec![]);
    sim.broadcast(0);
    sim.deliver_all();
    sim.log_state_snapshot(&TaskId(1), "round1");
    snapshots.push(sim.collect_metrics(&TaskId(1)));

    let e2 = counter.next_id();
    sim.nodes[1].claim_task(TaskId(1), e2.clone(), vec![e1]);
    sim.broadcast(1);
    sim.deliver_all();
    sim.log_state_snapshot(&TaskId(1), "round2");
    snapshots.push(sim.collect_metrics(&TaskId(1)));

    let e3 = counter.next_id();
    sim.nodes[2].complete_task(TaskId(1), e3, vec![e2]);
    sim.broadcast(2);
    sim.deliver_all();
    sim.log_state_snapshot(&TaskId(1), "round3");
    snapshots.push(sim.collect_metrics(&TaskId(1)));

    let conv = Simulation::find_convergence_round(&snapshots);
    for m in snapshots.iter_mut() {
        m.convergence_round = conv;
    }

    (sim, snapshots)
}

/// Runs the short-partition scenario: node 2 partitioned for 2 rounds, then healed.
pub fn scenario_short_partition(seed: u128) -> (Simulation, Vec<Metrics>) {
    let mut counter = EventCounter::new(seed);
    let mut sim = Simulation::new(vec![NodeId(0), NodeId(1), NodeId(2)]);
    let mut snapshots = Vec::new();

    sim.partition(2);

    let e1 = counter.next_id();
    sim.nodes[0].create_task(TaskId(1), e1.clone(), vec![]);
    sim.broadcast_with_failures(0, &[]);
    sim.deliver_all();
    sim.log_state_snapshot(&TaskId(1), "round1");
    snapshots.push(sim.collect_metrics(&TaskId(1)));

    let e2 = counter.next_id();
    sim.nodes[1].claim_task(TaskId(1), e2, vec![e1]);
    sim.broadcast_with_failures(1, &[]);
    sim.deliver_all();
    sim.log_state_snapshot(&TaskId(1), "round2");
    snapshots.push(sim.collect_metrics(&TaskId(1)));

    // Heal node 2 and deliver its backlog directly into its inbox before
    // the next deliver_all — this represents the anti-entropy catch-up
    // that occurs when a previously partitioned node reconnects.
    sim.heal(2);
    let backlog: Vec<_> = sim.nodes[0].graph.events.values().cloned().collect();
    for event in backlog {
        sim.nodes[2].inbox.push(event);
    }
    let e3 = counter.next_id();
    sim.nodes[0].create_task(TaskId(2), e3, vec![]);
    sim.broadcast(0);
    sim.deliver_all();
    sim.log_state_snapshot(&TaskId(1), "round3");
    snapshots.push(sim.collect_metrics(&TaskId(1)));

    let conv = Simulation::find_convergence_round(&snapshots);
    for m in snapshots.iter_mut() {
        m.convergence_round = conv;
    }

    (sim, snapshots)
}

/// Runs the long-partition scenario: node 2 partitioned for 5 rounds, then healed.
pub fn scenario_long_partition(seed: u128) -> (Simulation, Vec<Metrics>) {
    let mut counter = EventCounter::new(seed);
    let mut sim = Simulation::new(vec![NodeId(0), NodeId(1), NodeId(2)]);
    let mut snapshots = Vec::new();

    sim.partition(2);

    let e1 = counter.next_id();
    sim.nodes[0].create_task(TaskId(1), e1.clone(), vec![]);
    sim.broadcast_with_failures(0, &[]);
    sim.deliver_all();
    sim.log_state_snapshot(&TaskId(1), "round1");
    snapshots.push(sim.collect_metrics(&TaskId(1)));

    let e2 = counter.next_id();
    sim.nodes[1].claim_task(TaskId(1), e2, vec![e1]);
    sim.broadcast_with_failures(1, &[]);
    sim.deliver_all();
    sim.log_state_snapshot(&TaskId(1), "round2");
    snapshots.push(sim.collect_metrics(&TaskId(1)));

    for round in 3usize..=5 {
        sim.step(1);
        sim.log_state_snapshot(&TaskId(1), &format!("round{}", round));
        snapshots.push(sim.collect_metrics(&TaskId(1)));
    }

    sim.heal(2);
    let backlog: Vec<_> = sim.nodes[0].graph.events.values().cloned().collect();
    for event in backlog {
        sim.nodes[2].inbox.push(event);
    }
    let e3 = counter.next_id();
    sim.nodes[0].create_task(TaskId(2), e3, vec![]);
    sim.broadcast(0);
    sim.deliver_all();
    sim.log_state_snapshot(&TaskId(1), "round6");
    snapshots.push(sim.collect_metrics(&TaskId(1)));

    let conv = Simulation::find_convergence_round(&snapshots);
    for m in snapshots.iter_mut() {
        m.convergence_round = conv;
    }

    (sim, snapshots)
}

/// Runs the high-contention scenario: all 3 nodes claim concurrently, producing a conflict.
pub fn scenario_high_contention(seed: u128) -> (Simulation, Vec<Metrics>) {
    let mut counter = EventCounter::new(seed);
    let mut sim = Simulation::new(vec![NodeId(0), NodeId(1), NodeId(2)]);
    let mut snapshots = Vec::new();

    sim.partition(0);
    sim.partition(1);
    sim.partition(2);

    let e1 = counter.next_id();
    sim.nodes[0].create_task(TaskId(1), e1, vec![]);
    let e2 = counter.next_id();
    sim.nodes[1].claim_task(TaskId(1), e2, vec![]);
    let e3 = counter.next_id();
    sim.nodes[2].claim_task(TaskId(1), e3, vec![]);

    sim.heal(0);
    sim.heal(1);
    sim.heal(2);

    sim.broadcast(0);
    sim.deliver_all();
    sim.log_state_snapshot(&TaskId(1), "round1");
    snapshots.push(sim.collect_metrics(&TaskId(1)));

    sim.broadcast(1);
    sim.deliver_all();
    sim.log_state_snapshot(&TaskId(1), "round2");
    snapshots.push(sim.collect_metrics(&TaskId(1)));

    sim.broadcast(2);
    sim.deliver_all();
    sim.log_state_snapshot(&TaskId(1), "round3");
    snapshots.push(sim.collect_metrics(&TaskId(1)));

    let conv = Simulation::find_convergence_round(&snapshots);
    for m in snapshots.iter_mut() {
        m.convergence_round = conv;
    }

    (sim, snapshots)
}

/// Runs a named scenario for each seed and returns a `SeedSummary` per run.
///
/// Valid `scenario_name` values: `"no_partition"`, `"short_partition"`,
/// `"long_partition"`, `"high_contention"`.
pub fn run_multi_seed(scenario_name: &str, seeds: &[u128]) -> Vec<SeedSummary> {
    seeds
        .iter()
        .map(|&seed| {
            let (_, metrics) = match scenario_name {
                "no_partition" => scenario_no_partition(seed),
                "short_partition" => scenario_short_partition(seed),
                "long_partition" => scenario_long_partition(seed),
                "high_contention" => scenario_high_contention(seed),
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
    println!(
        "  {:>6} | {:>17} | {:>15} | conflicts",
        "seed", "convergence_round", "total_messages"
    );
    println!(
        "  {}+{}+{}+{}",
        "-".repeat(7),
        "-".repeat(19),
        "-".repeat(17),
        "-".repeat(10)
    );
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

pub fn write_multi_seed_json(scenario_name: &str, summaries: &[SeedSummary]) {
    std::fs::create_dir_all("results").expect("failed to create results directory");
    let filename = format!("results/{}_multi_seed.json", scenario_name);

    let runs_json = summaries
        .iter()
        .map(|s| {
            let event_counts = s
                .final_event_counts
                .iter()
                .map(|n| n.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            let conflict_counts = s
                .final_conflict_counts
                .iter()
                .map(|n| n.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            let edge_counts = s
                .final_edge_counts
                .iter()
                .map(|n| n.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            let conv = match s.convergence_round {
                Some(r) => r.to_string(),
                None => "null".to_string(),
            };
            format!(
                "    {{\n      \"seed\": {},\n      \"convergence_round\": {},\n      \"total_messages\": {},\n      \"final_event_counts\": [{}],\n      \"final_conflict_counts\": [{}],\n      \"final_edge_counts\": [{}]\n    }}",
                s.seed, conv, s.total_messages, event_counts, conflict_counts, edge_counts,
            )
        })
        .collect::<Vec<_>>()
        .join(",\n");

    let converged_rounds: Vec<usize> = summaries
        .iter()
        .filter_map(|s| s.convergence_round)
        .collect();
    let always_converged = converged_rounds.len() == summaries.len();
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
        "{{\n  \"scenario\": \"{}\",\n  \"runs\": [\n{}\n  ],\n{}\n}}",
        scenario_name, runs_json, summary_json,
    );

    std::fs::write(&filename, json).expect("failed to write multi-seed JSON");
    println!("Written: {}", filename);
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
}
