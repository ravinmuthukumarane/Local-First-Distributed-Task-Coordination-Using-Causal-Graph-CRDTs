mod node;
mod scenarios;
mod simulation;

use scenarios::{
    print_multi_seed_summary, run_multi_seed, scenario_high_contention, scenario_long_partition,
    scenario_no_partition, scenario_short_partition, write_multi_seed_json,
};
use simulation::{Metrics, Simulation};

fn main() {
    let seed = 1u128;

    println!("=== SCENARIO: No Partition (seed={}) ===", seed);
    let (sim, metrics) = scenario_no_partition(seed);
    print_summary(&sim, &metrics, "no_partition");
    write_scenario_json("no_partition", seed, &metrics, &sim.log);

    println!("\n=== SCENARIO: Short Partition (seed={}) ===", seed);
    let (sim, metrics) = scenario_short_partition(seed);
    print_summary(&sim, &metrics, "short_partition");
    write_scenario_json("short_partition", seed, &metrics, &sim.log);

    println!("\n=== SCENARIO: Long Partition (seed={}) ===", seed);
    let (sim, metrics) = scenario_long_partition(seed);
    print_summary(&sim, &metrics, "long_partition");
    write_scenario_json("long_partition", seed, &metrics, &sim.log);

    println!("\n=== SCENARIO: High Contention (seed={}) ===", seed);
    let (sim, metrics) = scenario_high_contention(seed);
    print_summary(&sim, &metrics, "high_contention");
    write_scenario_json("high_contention", seed, &metrics, &sim.log);

    let scenario_names = [
        "no_partition",
        "short_partition",
        "long_partition",
        "high_contention",
    ];
    let seeds: Vec<u128> = (1..=10).collect();

    for scenario in &scenario_names {
        println!("\n=== MULTI-SEED: {} ===", scenario);
        let summaries = run_multi_seed(scenario, &seeds);
        print_multi_seed_summary(&summaries);
        write_multi_seed_json(scenario, &summaries);
    }
}

fn print_summary(sim: &Simulation, metrics: &[Metrics], label: &str) {
    println!(
        "[{}] rounds={} convergence_round={:?}",
        label,
        metrics.len(),
        metrics.first().and_then(|m| m.convergence_round)
    );
    for (i, m) in metrics.iter().enumerate() {
        println!(
            "  round={} converged={} message_count={} event_counts={:?} edge_counts={:?} conflicts={:?}",
            i + 1,
            m.converged,
            m.message_count,
            m.event_counts,
            m.edge_counts,
            m.conflict_counts,
        );
    }
    println!("  log_entries={}", sim.log.len());
}

fn write_scenario_json(label: &str, seed: u128, metrics: &[Metrics], log: &[String]) {
    let filename = format!("{}_seed{}.json", label, seed);

    let rounds_json = metrics
        .iter()
        .enumerate()
        .map(|(i, m)| {
            let event_counts = m
                .event_counts
                .iter()
                .map(|n| n.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            let conflict_counts = m
                .conflict_counts
                .iter()
                .map(|n| n.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            let edge_counts = m
                .edge_counts
                .iter()
                .map(|n| n.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            let convergence_round = match m.convergence_round {
                Some(r) => r.to_string(),
                None => "null".to_string(),
            };
            format!(
                "    {{\n      \"round\": {},\n      \"converged\": {},\n      \"message_count\": {},\n      \"event_counts\": [{}],\n      \"conflict_counts\": [{}],\n      \"edge_counts\": [{}],\n      \"convergence_round\": {}\n    }}",
                i + 1,
                m.converged,
                m.message_count,
                event_counts,
                conflict_counts,
                edge_counts,
                convergence_round,
            )
        })
        .collect::<Vec<_>>()
        .join(",\n");

    let log_json = log
        .iter()
        .map(|entry| {
            format!(
                "    \"{}\"",
                entry.replace('\\', "\\\\").replace('"', "\\\"")
            )
        })
        .collect::<Vec<_>>()
        .join(",\n");

    let json = format!(
        "{{\n  \"scenario\": \"{}\",\n  \"seed\": {},\n  \"rounds\": [\n{}\n  ],\n  \"log\": [\n{}\n  ]\n}}",
        label, seed, rounds_json, log_json,
    );

    std::fs::write(&filename, json).expect("failed to write JSON");
    println!("Written: {}", filename);
}
