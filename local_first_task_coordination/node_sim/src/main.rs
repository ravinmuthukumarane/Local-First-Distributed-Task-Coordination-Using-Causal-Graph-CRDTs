mod json_util;
mod node;
mod rng;
mod scenarios;
mod simulation;

use crdt_core::TaskId;
use json_util::{json_string, opt_usize, usize_array};
use scenarios::{
    print_multi_seed_summary, run_multi_seed, scenario_auto_frontier_reordered,
    scenario_high_contention, scenario_long_partition, scenario_no_partition,
    scenario_short_partition, write_multi_seed_json,
};
use simulation::{Metrics, Simulation};

fn main() -> std::io::Result<()> {
    let seed = 1u128;

    println!("=== SCENARIO: No Partition (seed={}) ===", seed);
    let (sim, metrics) = scenario_no_partition(seed);
    print_summary(&sim, &metrics, "no_partition");
    write_scenario_json("no_partition", seed, &metrics, &sim.log)?;

    println!("\n=== SCENARIO: Short Partition (seed={}) ===", seed);
    let (sim, metrics) = scenario_short_partition(seed);
    print_summary(&sim, &metrics, "short_partition");
    write_scenario_json("short_partition", seed, &metrics, &sim.log)?;

    println!("\n=== SCENARIO: Long Partition (seed={}) ===", seed);
    let (sim, metrics) = scenario_long_partition(seed);
    print_summary(&sim, &metrics, "long_partition");
    write_scenario_json("long_partition", seed, &metrics, &sim.log)?;

    println!("\n=== SCENARIO: High Contention (seed={}) ===", seed);
    let (sim, metrics) = scenario_high_contention(seed);
    print_summary(&sim, &metrics, "high_contention");
    write_scenario_json("high_contention", seed, &metrics, &sim.log)?;

    println!(
        "\n=== SCENARIO: Auto Frontier + Reordered Delivery (seed={}) ===",
        seed
    );
    let (sim, metrics) = scenario_auto_frontier_reordered(seed);
    print_summary(&sim, &metrics, "auto_frontier_reordered");
    write_scenario_json("auto_frontier_reordered", seed, &metrics, &sim.log)?;

    let scenario_names = [
        "no_partition",
        "short_partition",
        "long_partition",
        "high_contention",
        "auto_frontier_reordered",
    ];
    let seeds: Vec<u128> = (1..=10).collect();

    for scenario in &scenario_names {
        println!("\n=== MULTI-SEED: {} ===", scenario);
        let summaries = run_multi_seed(scenario, &seeds);
        print_multi_seed_summary(&summaries);
        write_multi_seed_json(scenario, &summaries)?;
    }

    Ok(())
}

/// Prints a header row plus a matching separator line for a simple
/// fixed-width text table. `columns` pairs each header label with the
/// display width its data rows use for that column (matching a `{:>width}`
/// format), so the header and the `-+-` divider are derived from one list
/// instead of being hand-tuned separately (and potentially drifting apart).
pub(crate) fn print_table_header(columns: &[(&str, usize)]) {
    let header: Vec<String> = columns
        .iter()
        .map(|(label, width)| format!("{:>width$}", label, width = width))
        .collect();
    println!("  {}", header.join(" | "));

    let separator: Vec<String> = columns
        .iter()
        .map(|(_, width)| "-".repeat(width + 2))
        .collect();
    println!("  {}", separator.join("+"));
}

fn print_summary(sim: &Simulation, metrics: &[Metrics], label: &str) {
    let convergence_round = match metrics.first().and_then(|m| m.convergence_round) {
        Some(r) => r.to_string(),
        None => "none".to_string(),
    };
    println!(
        "[{}] rounds={} convergence_round={}",
        label,
        metrics.len(),
        convergence_round
    );

    print_table_header(&[
        ("round", 6),
        ("converged", 9),
        ("message_count", 14),
        ("event_counts", 12),
        ("edge_counts", 12),
        ("conflicts", 9),
    ]);
    for (i, m) in metrics.iter().enumerate() {
        println!(
            "  {:>6} | {:>9} | {:>14} | {:>12} | {:>12} | {}",
            i + 1,
            m.converged,
            m.message_count,
            join_counts(&m.event_counts),
            join_counts(&m.edge_counts),
            join_counts(&m.conflict_counts),
        );
    }
    println!("  log_entries={}", sim.log.len());

    // TaskId(1) is the task tracked across every scenario; report its final
    // per-node state so the console output isn't limited to raw counts.
    for i in 0..sim.nodes.len() {
        let state = sim.derive_state(i, &TaskId(1));
        let resolved = match state.resolved_claimant {
            Some(node_id) => format!("node{}", node_id.0),
            None => "none".to_string(),
        };
        println!(
            "  final[node={}] exists={} claims={} completions={} conflict={} resolved_claimant={}",
            i,
            state.exists,
            state.claims.len(),
            state.completions.len(),
            state.has_conflict,
            resolved,
        );
    }
}

/// Renders a per-node count vector as a compact `a/b/c` string for table cells.
fn join_counts(counts: &[usize]) -> String {
    counts
        .iter()
        .map(|n| n.to_string())
        .collect::<Vec<_>>()
        .join("/")
}

fn write_scenario_json(
    label: &str,
    seed: u128,
    metrics: &[Metrics],
    log: &[String],
) -> std::io::Result<()> {
    std::fs::create_dir_all("results")?;
    let filename = format!("results/{}_seed{}.json", label, seed);

    let rounds_json = metrics
        .iter()
        .enumerate()
        .map(|(i, m)| {
            format!(
                "    {{\n      \"round\": {},\n      \"converged\": {},\n      \"message_count\": {},\n      \"event_counts\": {},\n      \"conflict_counts\": {},\n      \"edge_counts\": {},\n      \"convergence_round\": {}\n    }}",
                i + 1,
                m.converged,
                m.message_count,
                usize_array(&m.event_counts),
                usize_array(&m.conflict_counts),
                usize_array(&m.edge_counts),
                opt_usize(m.convergence_round),
            )
        })
        .collect::<Vec<_>>()
        .join(",\n");

    let log_json = log
        .iter()
        .map(|entry| format!("    {}", json_string(entry)))
        .collect::<Vec<_>>()
        .join(",\n");

    let json = format!(
        "{{\n  \"scenario\": {},\n  \"seed\": {},\n  \"rounds\": [\n{}\n  ],\n  \"log\": [\n{}\n  ]\n}}",
        json_string(label),
        seed,
        rounds_json,
        log_json,
    );

    std::fs::write(&filename, json)?;
    println!("Written: {}", filename);
    Ok(())
}
