use std::collections::HashSet;

use crdt_core::message::Message;
use crdt_core::{EventId, NodeId, TaskId};
use task_model::TaskState;

use crate::node::Node;

/// A snapshot of simulation state collected at the end of one delivery round.
pub struct Metrics {
    /// Number of events in each node's graph, indexed by node position.
    pub event_counts: Vec<usize>,
    /// Per-node conflict indicator (1 = conflict, 0 = none) for the tracked task.
    pub conflict_counts: Vec<usize>,
    /// Cumulative number of `receive_message` calls since simulation start.
    pub message_count: usize,
    /// `true` if all nodes hold an identical set of `EventId`s.
    pub converged: bool,
    /// Number of entries in each node's `children` map (causal edge count).
    pub edge_counts: Vec<usize>,
    /// 1-indexed round in which convergence was first achieved, or `None`.
    pub convergence_round: Option<usize>,
}

/// A seeded counter that produces unique, reproducible `EventId`s.
///
/// Using the same seed across multiple runs guarantees identical event IDs,
/// making experiment results deterministic.
pub struct EventCounter {
    next: u128,
}

impl EventCounter {
    /// Creates a new counter starting at `seed`.
    pub fn new(seed: u128) -> EventCounter {
        EventCounter { next: seed }
    }

    /// Returns the next `EventId` and advances the counter.
    pub fn next_id(&mut self) -> EventId {
        let id = EventId(self.next);
        self.next += 1;
        id
    }
}

/// A deterministic multi-node simulation of the distributed task coordination system.
pub struct Simulation {
    /// The simulated nodes, in index order.
    pub nodes: Vec<Node>,
    /// Per-node inbound message queues, indexed by node position.
    pub message_queues: Vec<Vec<Message>>,
    /// Indices of nodes that are currently partitioned from the network.
    pub partitioned: HashSet<usize>,
    /// Cumulative count of `receive_message` calls, used as the message metric.
    pub messages_delivered: usize,
    /// Ordered log of simulation events in `key=value` format.
    pub log: Vec<String>,
}

impl Simulation {
    fn record(&mut self, entry: String) {
        self.log.push(entry);
    }

    /// Broadcasts the sender's outbox to all nodes (including itself).
    pub fn broadcast(&mut self, sender_index: usize) {
        let message = self.nodes[sender_index].collect_outbox_as_message();
        let event_count = message.events.len();
        for (i, queue) in self.message_queues.iter_mut().enumerate() {
            if i != sender_index {
                queue.push(message.clone());
            }
        }
        self.message_queues[sender_index].push(message);
        self.record(format!(
            "BROADCAST sender={} event_count={}",
            sender_index, event_count
        ));
    }

    /// Drains all message queues and processes every node's inbox.
    pub fn deliver_all(&mut self) {
        let mut delivered = 0usize;
        let mut log_entries: Vec<String> = Vec::new();
        for (i, (node, queue)) in self
            .nodes
            .iter_mut()
            .zip(self.message_queues.iter_mut())
            .enumerate()
        {
            for message in queue.drain(..) {
                node.receive_message(message);
                delivered += 1;
            }
            node.process_inbox();
            log_entries.push(format!(
                "DELIVER node={} events_in_graph={}",
                i,
                node.graph.events.len()
            ));
        }
        self.messages_delivered += delivered;
        self.log.extend(log_entries);
    }

    /// Creates a simulation with one node per supplied `NodeId`.
    pub fn new(node_ids: Vec<NodeId>) -> Simulation {
        let message_queues = vec![Vec::new(); node_ids.len()];
        let nodes = node_ids.into_iter().map(Node::new).collect();
        Simulation {
            nodes,
            message_queues,
            partitioned: HashSet::new(),
            messages_delivered: 0,
            log: Vec::new(),
        }
    }

    /// Collects a `Metrics` snapshot for the current simulation state.
    ///
    /// `convergence_round` is always `None` here; call [`Self::find_convergence_round`]
    /// after all rounds are complete and stamp the value back with `iter_mut`.
    pub fn collect_metrics(&self, task_id: &TaskId) -> Metrics {
        let event_counts = self.nodes.iter().map(|n| n.graph.events.len()).collect();

        let conflict_counts = self
            .nodes
            .iter()
            .map(|n| task_model::derive_task_state(&n.graph, task_id).has_conflict as usize)
            .collect();

        let edge_counts = self.nodes.iter().map(|n| n.graph.children.len()).collect();

        let converged = {
            let sets: Vec<HashSet<&EventId>> = self
                .nodes
                .iter()
                .map(|n| n.graph.events.keys().collect())
                .collect();
            sets.windows(2).all(|w| w[0] == w[1])
        };

        Metrics {
            event_counts,
            conflict_counts,
            message_count: self.messages_delivered,
            converged,
            edge_counts,
            convergence_round: None,
        }
    }

    /// Returns the 1-indexed round number of the first converged snapshot, or `None`.
    pub fn find_convergence_round(metrics: &[Metrics]) -> Option<usize> {
        metrics
            .iter()
            .enumerate()
            .find(|(_, m)| m.converged)
            .map(|(i, _)| i + 1)
    }

    /// Marks a node as partitioned; it will be excluded from future deliveries.
    pub fn partition(&mut self, node_index: usize) {
        self.partitioned.insert(node_index);
    }

    /// Removes the partition flag from a node, re-enabling delivery to it.
    pub fn heal(&mut self, node_index: usize) {
        self.partitioned.remove(&node_index);
    }

    /// Broadcasts the sender's outbox, skipping partitioned nodes and any in `drop_indices`.
    ///
    /// If the sender itself is partitioned its outbox is silently drained and no
    /// messages are enqueued for anyone, including itself.
    pub fn broadcast_with_failures(&mut self, sender_index: usize, drop_indices: &[usize]) {
        let entry = format!(
            "BROADCAST_WITH_FAILURES sender={} event_count={} dropped={} partitioned={}",
            sender_index,
            self.nodes[sender_index].outbox.len(),
            drop_indices.len(),
            self.partitioned.len(),
        );
        self.record(entry);

        if self.partitioned.contains(&sender_index) {
            // drain the outbox silently — sender is cut off from everyone including itself
            self.nodes[sender_index].collect_outbox_as_message();
            return;
        }

        let message = self.nodes[sender_index].collect_outbox_as_message();
        for (i, queue) in self.message_queues.iter_mut().enumerate() {
            if self.partitioned.contains(&i) || drop_indices.contains(&i) {
                continue;
            }
            if i != sender_index {
                queue.push(message.clone());
            }
        }
        self.message_queues[sender_index].push(message);
    }

    /// Calls [`Self::deliver_all`] `steps` times.
    pub fn step(&mut self, steps: usize) {
        for _ in 0..steps {
            self.deliver_all();
        }
    }

    /// Derives the task state for one node without mutating the simulation.
    #[allow(dead_code)]
    pub fn derive_state(&self, node_index: usize, task_id: &TaskId) -> TaskState {
        task_model::derive_task_state(&self.nodes[node_index].graph, task_id)
    }

    /// Appends one `SNAPSHOT` log entry per node for the given task and label.
    pub fn log_state_snapshot(&mut self, task_id: &TaskId, label: &str) {
        let entries: Vec<String> = self
            .nodes
            .iter()
            .enumerate()
            .map(|(i, n)| {
                let state = task_model::derive_task_state(&n.graph, task_id);
                format!(
                    "SNAPSHOT label={} node={} events={} exists={} claims={} completions={} conflict={}",
                    label,
                    i,
                    n.graph.events.len(),
                    state.exists,
                    state.claims.len(),
                    state.completions.len(),
                    state.has_conflict,
                )
            })
            .collect();
        self.log.extend(entries);
    }
}

#[cfg(test)]
mod tests {
    use crdt_core::{EventId, NodeId, TaskId};

    use super::*;

    #[test]
    fn simulation_creates_correct_node_and_queue_counts() {
        let sim = Simulation::new(vec![NodeId(1), NodeId(2)]);
        assert_eq!(sim.nodes.len(), 2);
        assert_eq!(sim.message_queues.len(), 2);
    }

    #[test]
    fn broadcast_enqueues_message_for_all_nodes() {
        let mut sim = Simulation::new(vec![NodeId(1), NodeId(2)]);
        sim.nodes[0].create_task(TaskId(1), EventId(1), vec![]);
        sim.broadcast(0);

        assert_eq!(sim.message_queues[0].len(), 1);
        assert_eq!(sim.message_queues[1].len(), 1);
    }

    #[test]
    fn deliver_all_clears_queues_and_populates_graphs() {
        let mut sim = Simulation::new(vec![NodeId(1), NodeId(2)]);
        sim.nodes[0].create_task(TaskId(1), EventId(1), vec![]);
        sim.broadcast(0);
        sim.deliver_all();

        assert!(sim.message_queues[0].is_empty());
        assert!(sim.message_queues[1].is_empty());
        assert_eq!(sim.nodes[0].graph.events.len(), 1);
        assert_eq!(sim.nodes[1].graph.events.len(), 1);
    }

    #[test]
    fn convergence_after_broadcast_two_nodes() {
        let mut sim = Simulation::new(vec![NodeId(1), NodeId(2)]);
        sim.nodes[0].create_task(TaskId(1), EventId(1), vec![]);
        sim.broadcast(0);
        sim.deliver_all();

        assert!(sim.derive_state(0, &TaskId(1)).exists);
        assert!(sim.derive_state(1, &TaskId(1)).exists);
    }

    #[test]
    fn three_node_convergence() {
        let mut sim = Simulation::new(vec![NodeId(1), NodeId(2), NodeId(3)]);
        sim.nodes[1].create_task(TaskId(42), EventId(1), vec![]);
        sim.broadcast(1);
        sim.deliver_all();

        assert!(sim.derive_state(0, &TaskId(42)).exists);
        assert!(sim.derive_state(1, &TaskId(42)).exists);
        assert!(sim.derive_state(2, &TaskId(42)).exists);
    }

    #[test]
    fn partition_blocks_delivery() {
        let mut sim = Simulation::new(vec![NodeId(1), NodeId(2), NodeId(3)]);
        sim.partition(2);
        sim.nodes[0].create_task(TaskId(1), EventId(1), vec![]);
        sim.broadcast_with_failures(0, &[]);
        sim.deliver_all();

        assert!(sim.derive_state(0, &TaskId(1)).exists);
        assert!(sim.derive_state(1, &TaskId(1)).exists);
        assert!(!sim.derive_state(2, &TaskId(1)).exists);
    }

    #[test]
    fn heal_restores_delivery() {
        let mut sim = Simulation::new(vec![NodeId(1), NodeId(2), NodeId(3)]);
        sim.partition(2);
        sim.nodes[0].create_task(TaskId(1), EventId(1), vec![]);
        sim.broadcast_with_failures(0, &[]);
        sim.deliver_all();

        sim.heal(2);
        sim.nodes[0].create_task(TaskId(1), EventId(2), vec![EventId(1)]);
        sim.broadcast_with_failures(0, &[]);
        sim.deliver_all();

        assert!(sim.derive_state(2, &TaskId(1)).exists);
    }

    #[test]
    fn drop_without_partition() {
        let mut sim = Simulation::new(vec![NodeId(1), NodeId(2), NodeId(3)]);
        sim.nodes[0].create_task(TaskId(1), EventId(1), vec![]);
        sim.broadcast_with_failures(0, &[1]);
        sim.deliver_all();

        assert!(!sim.derive_state(1, &TaskId(1)).exists);
        assert!(sim.derive_state(2, &TaskId(1)).exists);
    }

    #[test]
    fn partitioned_sender_delivers_to_no_one() {
        let mut sim = Simulation::new(vec![NodeId(1), NodeId(2), NodeId(3)]);
        sim.partition(0);
        sim.nodes[0].create_task(TaskId(1), EventId(1), vec![]);
        sim.broadcast_with_failures(0, &[]);
        sim.deliver_all();

        assert!(!sim.derive_state(0, &TaskId(1)).exists);
        assert!(!sim.derive_state(1, &TaskId(1)).exists);
        assert!(!sim.derive_state(2, &TaskId(1)).exists);
    }

    #[test]
    fn step_runs_deliver_all_n_times() {
        let mut sim = Simulation::new(vec![NodeId(1), NodeId(2)]);
        sim.nodes[0].create_task(TaskId(1), EventId(1), vec![]);
        sim.broadcast(0);

        assert!(!sim.message_queues[0].is_empty());
        sim.step(1);

        assert!(sim.message_queues[0].is_empty());
        assert!(sim.message_queues[1].is_empty());
        assert!(sim.derive_state(0, &TaskId(1)).exists);
        assert!(sim.derive_state(1, &TaskId(1)).exists);
    }

    #[test]
    fn node_logic_unchanged_during_partition() {
        let mut sim = Simulation::new(vec![NodeId(1), NodeId(2)]);
        sim.partition(0);

        sim.nodes[0].create_task(TaskId(1), EventId(1), vec![]);
        sim.nodes[0].claim_task(TaskId(1), EventId(2), vec![EventId(1)]);

        assert_eq!(sim.nodes[0].outbox.len(), 2);
    }

    #[test]
    fn fresh_simulation_metrics_are_zero_and_converged() {
        let sim = Simulation::new(vec![NodeId(1), NodeId(2), NodeId(3)]);
        let m = sim.collect_metrics(&TaskId(1));

        assert_eq!(m.event_counts, vec![0, 0, 0]);
        assert_eq!(m.message_count, 0);
        assert!(m.converged);
    }

    #[test]
    fn event_counts_equal_after_delivery() {
        let mut sim = Simulation::new(vec![NodeId(1), NodeId(2), NodeId(3)]);
        sim.nodes[0].create_task(TaskId(1), EventId(1), vec![]);
        sim.broadcast(0);
        sim.deliver_all();

        let m = sim.collect_metrics(&TaskId(1));
        assert_eq!(m.event_counts, vec![1, 1, 1]);
    }

    #[test]
    fn conflict_detected_after_concurrent_claims() {
        let mut sim = Simulation::new(vec![NodeId(1), NodeId(2)]);

        sim.nodes[0].create_task(TaskId(1), EventId(1), vec![]);
        sim.broadcast(0);
        sim.deliver_all();

        sim.nodes[0].claim_task(TaskId(1), EventId(2), vec![EventId(1)]);
        sim.nodes[1].claim_task(TaskId(1), EventId(3), vec![EventId(1)]);
        sim.broadcast(0);
        sim.broadcast(1);
        sim.deliver_all();

        let m = sim.collect_metrics(&TaskId(1));
        assert_eq!(m.conflict_counts, vec![1, 1]);
    }

    #[test]
    fn message_count_accumulates_across_rounds() {
        let mut sim = Simulation::new(vec![NodeId(1), NodeId(2)]);

        sim.nodes[0].create_task(TaskId(1), EventId(1), vec![]);
        sim.broadcast(0);
        sim.deliver_all();

        sim.nodes[1].claim_task(TaskId(1), EventId(2), vec![EventId(1)]);
        sim.broadcast(1);
        sim.deliver_all();

        let m = sim.collect_metrics(&TaskId(1));
        assert_eq!(m.message_count, 4);
    }

    #[test]
    fn converged_true_after_full_sync() {
        let mut sim = Simulation::new(vec![NodeId(1), NodeId(2), NodeId(3)]);
        sim.nodes[0].create_task(TaskId(1), EventId(1), vec![]);
        sim.broadcast(0);
        sim.deliver_all();

        assert!(sim.collect_metrics(&TaskId(1)).converged);
    }

    #[test]
    fn converged_false_during_partition() {
        let mut sim = Simulation::new(vec![NodeId(1), NodeId(2), NodeId(3)]);
        sim.partition(2);
        sim.nodes[0].create_task(TaskId(1), EventId(1), vec![]);
        sim.broadcast_with_failures(0, &[]);
        sim.deliver_all();

        assert!(!sim.collect_metrics(&TaskId(1)).converged);
    }

    #[test]
    fn log_is_empty_on_fresh_simulation() {
        let sim = Simulation::new(vec![NodeId(1), NodeId(2)]);
        assert!(sim.log.is_empty());
    }

    #[test]
    fn broadcast_appends_one_broadcast_log_entry() {
        let mut sim = Simulation::new(vec![NodeId(1), NodeId(2)]);
        sim.nodes[0].create_task(TaskId(1), EventId(1), vec![]);
        sim.broadcast(0);

        let broadcast_entries: Vec<_> =
            sim.log.iter().filter(|e| e.contains("BROADCAST")).collect();
        assert_eq!(broadcast_entries.len(), 1);
    }

    #[test]
    fn deliver_all_appends_one_deliver_entry_per_node() {
        let mut sim = Simulation::new(vec![NodeId(1), NodeId(2), NodeId(3)]);
        sim.nodes[0].create_task(TaskId(1), EventId(1), vec![]);
        sim.broadcast(0);
        let log_len_before = sim.log.len();
        sim.deliver_all();

        let deliver_entries: Vec<_> = sim.log[log_len_before..]
            .iter()
            .filter(|e| e.contains("DELIVER"))
            .collect();
        assert_eq!(deliver_entries.len(), 3);
    }

    #[test]
    fn log_state_snapshot_appends_one_snapshot_entry_per_node() {
        let mut sim = Simulation::new(vec![NodeId(1), NodeId(2), NodeId(3)]);
        sim.log_state_snapshot(&TaskId(1), "check");

        let snapshot_entries: Vec<_> =
            sim.log.iter().filter(|e| e.contains("SNAPSHOT")).collect();
        assert_eq!(snapshot_entries.len(), 3);
    }

    #[test]
    fn full_scenario_log_order_is_broadcast_then_deliver_then_snapshot() {
        let mut sim = Simulation::new(vec![NodeId(1), NodeId(2)]);
        sim.nodes[0].create_task(TaskId(1), EventId(1), vec![]);
        sim.broadcast(0);
        sim.deliver_all();
        sim.log_state_snapshot(&TaskId(1), "end");

        let broadcast_pos = sim
            .log
            .iter()
            .position(|e| e.contains("BROADCAST"))
            .unwrap();
        let first_deliver_pos = sim.log.iter().position(|e| e.contains("DELIVER")).unwrap();
        let first_snapshot_pos = sim
            .log
            .iter()
            .position(|e| e.contains("SNAPSHOT"))
            .unwrap();

        assert!(broadcast_pos < first_deliver_pos);
        assert!(first_deliver_pos < first_snapshot_pos);
    }

}
