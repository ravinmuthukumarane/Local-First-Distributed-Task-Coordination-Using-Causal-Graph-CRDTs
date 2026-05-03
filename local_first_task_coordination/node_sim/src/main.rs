use std::collections::HashSet;

use crdt_core::causal_graph::CausalGraph;
use crdt_core::message::Message;
use crdt_core::{Event, EventId, NodeId, TaskId};
use task_model::{Op as Operation, TaskState};

pub struct Node {
    pub node_id: NodeId,
    pub graph: CausalGraph,
    pub outbox: Vec<Event>,
    pub inbox: Vec<Event>,
}

impl Node {
    pub fn new(node_id: NodeId) -> Node {
        Node {
            node_id,
            graph: CausalGraph::new(),
            outbox: Vec::new(),
            inbox: Vec::new(),
        }
    }

    pub fn create_task(&mut self, task_id: TaskId, event_id: EventId, parents: Vec<EventId>) {
        self.outbox.push(Event {
            event_id,
            operation: Operation::Create(task_id),
            causal_parents: parents,
        });
    }

    pub fn claim_task(&mut self, task_id: TaskId, event_id: EventId, parents: Vec<EventId>) {
        self.outbox.push(Event {
            event_id,
            operation: Operation::Claim(task_id, self.node_id.clone()),
            causal_parents: parents,
        });
    }

    pub fn complete_task(&mut self, task_id: TaskId, event_id: EventId, parents: Vec<EventId>) {
        self.outbox.push(Event {
            event_id,
            operation: Operation::Complete(task_id, self.node_id.clone()),
            causal_parents: parents,
        });
    }

    pub fn receive(&mut self, event: Event) {
        self.inbox.push(event);
    }

    pub fn process_inbox(&mut self) {
        for event in self.inbox.drain(..) {
            self.graph.insert(event);
        }
    }

    pub fn collect_outbox_as_message(&mut self) -> Message {
        Message::from_events(self.outbox.drain(..).collect())
    }

    pub fn receive_message(&mut self, message: Message) {
        for event in message.events {
            self.receive(event);
        }
    }
}

pub struct Simulation {
    pub nodes: Vec<Node>,
    pub message_queues: Vec<Vec<Message>>,
    pub partitioned: HashSet<usize>,
}

impl Simulation {
    pub fn new(node_ids: Vec<NodeId>) -> Simulation {
        let message_queues = vec![Vec::new(); node_ids.len()];
        let nodes = node_ids.into_iter().map(Node::new).collect();
        Simulation { nodes, message_queues, partitioned: HashSet::new() }
    }

    pub fn partition(&mut self, node_index: usize) {
        self.partitioned.insert(node_index);
    }

    pub fn heal(&mut self, node_index: usize) {
        self.partitioned.remove(&node_index);
    }

    pub fn broadcast(&mut self, sender_index: usize) {
        let message = self.nodes[sender_index].collect_outbox_as_message();
        for (i, queue) in self.message_queues.iter_mut().enumerate() {
            if i != sender_index {
                queue.push(message.clone());
            }
        }
        self.message_queues[sender_index].push(message);
    }

    pub fn broadcast_with_failures(&mut self, sender_index: usize, drop_indices: &[usize]) {
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

    pub fn deliver_all(&mut self) {
        for (node, queue) in self.nodes.iter_mut().zip(self.message_queues.iter_mut()) {
            for message in queue.drain(..) {
                node.receive_message(message);
            }
            node.process_inbox();
        }
    }

    pub fn step(&mut self, steps: usize) {
        for _ in 0..steps {
            self.deliver_all();
        }
    }

    pub fn derive_state(&self, node_index: usize, task_id: &TaskId) -> TaskState {
        task_model::derive_task_state(&self.nodes[node_index].graph, task_id)
    }
}

fn main() {}

#[cfg(test)]
mod tests {
    use crdt_core::{EventId, NodeId, TaskId};

    use super::*;

    fn node() -> Node {
        Node::new(NodeId(1))
    }

    #[test]
    fn two_nodes_start_empty_with_distinct_ids() {
        let a = Node::new(NodeId(1));
        let b = Node::new(NodeId(2));

        assert!(a.graph.events.is_empty());
        assert!(a.outbox.is_empty());
        assert!(a.inbox.is_empty());

        assert!(b.graph.events.is_empty());
        assert!(b.outbox.is_empty());
        assert!(b.inbox.is_empty());

        assert_ne!(a.node_id, b.node_id);
    }

    #[test]
    fn create_task_pushes_to_outbox() {
        let mut n = node();
        n.create_task(TaskId(1), EventId(1), vec![]);

        assert_eq!(n.outbox.len(), 1);
        assert_eq!(n.outbox[0].operation, Operation::Create(TaskId(1)));
    }

    #[test]
    fn claim_task_pushes_to_outbox_with_node_id() {
        let mut n = node();
        n.claim_task(TaskId(1), EventId(1), vec![]);

        assert_eq!(n.outbox.len(), 1);
        assert_eq!(
            n.outbox[0].operation,
            Operation::Claim(TaskId(1), NodeId(1))
        );
    }

    #[test]
    fn complete_task_pushes_to_outbox_with_node_id() {
        let mut n = node();
        n.complete_task(TaskId(1), EventId(1), vec![]);

        assert_eq!(n.outbox.len(), 1);
        assert_eq!(
            n.outbox[0].operation,
            Operation::Complete(TaskId(1), NodeId(1))
        );
    }

    #[test]
    fn multiple_actions_accumulate_in_outbox() {
        let mut n = node();
        n.create_task(TaskId(1), EventId(1), vec![]);
        n.claim_task(TaskId(1), EventId(2), vec![EventId(1)]);
        n.complete_task(TaskId(1), EventId(3), vec![EventId(2)]);

        assert_eq!(n.outbox.len(), 3);
    }

    #[test]
    fn graph_is_untouched_after_local_actions() {
        let mut n = node();
        n.create_task(TaskId(1), EventId(1), vec![]);
        n.claim_task(TaskId(1), EventId(2), vec![EventId(1)]);

        assert!(n.graph.events.is_empty());
    }

    #[test]
    fn empty_inbox_does_nothing() {
        let mut n = node();
        n.process_inbox();

        assert!(n.graph.events.is_empty());
        assert!(n.inbox.is_empty());
    }

    #[test]
    fn single_event_received_and_merged() {
        let mut n = node();
        n.receive(Event {
            event_id: EventId(1),
            operation: Operation::Create(TaskId(1)),
            causal_parents: vec![],
        });
        n.process_inbox();

        assert_eq!(n.graph.events.len(), 1);
        assert!(n.inbox.is_empty());
    }

    #[test]
    fn multiple_events_merged() {
        let mut n = node();
        for id in 1..=3 {
            n.receive(Event {
                event_id: EventId(id),
                operation: Operation::Create(TaskId(id)),
                causal_parents: vec![],
            });
        }
        n.process_inbox();

        assert_eq!(n.graph.events.len(), 3);
        assert!(n.inbox.is_empty());
    }

    #[test]
    fn outbox_unaffected_by_process_inbox() {
        let mut n = node();
        n.create_task(TaskId(1), EventId(1), vec![]);
        n.receive(Event {
            event_id: EventId(2),
            operation: Operation::Create(TaskId(2)),
            causal_parents: vec![],
        });
        n.process_inbox();

        assert_eq!(n.outbox.len(), 1);
        assert_eq!(n.outbox[0].operation, Operation::Create(TaskId(1)));
    }

    #[test]
    fn derive_state_after_merge() {
        let mut node_a = Node::new(NodeId(1));
        node_a.create_task(TaskId(42), EventId(1), vec![]);

        let mut node_b = Node::new(NodeId(2));
        node_b.receive(node_a.outbox[0].clone());
        node_b.process_inbox();

        let state = task_model::derive_task_state(&node_b.graph, &TaskId(42));
        assert!(state.exists);
    }

    #[test]
    fn collect_empty_outbox_returns_empty_message() {
        let mut n = node();
        let msg = n.collect_outbox_as_message();

        assert!(msg.events.is_empty());
        assert!(n.outbox.is_empty());
    }

    #[test]
    fn collect_drains_outbox() {
        let mut n = node();
        n.create_task(TaskId(1), EventId(1), vec![]);
        let msg = n.collect_outbox_as_message();

        assert_eq!(msg.events.len(), 1);
        assert!(n.outbox.is_empty());
    }

    #[test]
    fn receive_message_fills_inbox() {
        let mut n = node();
        let msg = Message::from_events(vec![
            Event { event_id: EventId(1), operation: Operation::Create(TaskId(1)), causal_parents: vec![] },
            Event { event_id: EventId(2), operation: Operation::Create(TaskId(2)), causal_parents: vec![] },
        ]);
        n.receive_message(msg);

        assert_eq!(n.inbox.len(), 2);
    }

    #[test]
    fn full_one_way_pipeline() {
        let mut node_a = Node::new(NodeId(1));
        node_a.create_task(TaskId(1), EventId(1), vec![]);
        let msg = node_a.collect_outbox_as_message();

        let mut node_b = Node::new(NodeId(2));
        node_b.receive_message(msg);
        node_b.process_inbox();

        let state = task_model::derive_task_state(&node_b.graph, &TaskId(1));
        assert!(state.exists);
    }

    #[test]
    fn two_way_sync() {
        let mut node_a = Node::new(NodeId(1));
        let mut node_b = Node::new(NodeId(2));

        // A creates task; clone before draining so A can self-deliver
        node_a.create_task(TaskId(1), EventId(1), vec![]);
        let create_event = node_a.outbox[0].clone();
        let msg_a = node_a.collect_outbox_as_message();
        node_a.receive(create_event);
        node_b.receive_message(msg_a);
        node_a.process_inbox();
        node_b.process_inbox();

        // B claims task; clone before draining so B can self-deliver
        node_b.claim_task(TaskId(1), EventId(2), vec![EventId(1)]);
        let claim_event = node_b.outbox[0].clone();
        let msg_b = node_b.collect_outbox_as_message();
        node_b.receive(claim_event);
        node_a.receive_message(msg_b);
        node_b.process_inbox();
        node_a.process_inbox();

        let state_a = task_model::derive_task_state(&node_a.graph, &TaskId(1));
        let state_b = task_model::derive_task_state(&node_b.graph, &TaskId(1));

        assert!(state_a.exists);
        assert_eq!(state_a.claims.len(), 1);
        assert!(state_b.exists);
        assert_eq!(state_b.claims.len(), 1);
    }

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
}
