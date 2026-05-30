use crdt_core::causal_graph::CausalGraph;
use crdt_core::message::Message;
use crdt_core::{Event, EventId, NodeId, Operation, TaskId};

/// A simulated distributed node with a local causal graph and message buffers.
pub struct Node {
    /// This node's unique identifier.
    pub node_id: NodeId,
    /// The node's local view of the causal event graph.
    pub graph: CausalGraph,
    /// Events staged for broadcast to other nodes.
    pub outbox: Vec<Event>,
    /// Events received from other nodes, pending processing.
    pub inbox: Vec<Event>,
}

impl Node {
    /// Creates a new node with the given identity and empty buffers.
    pub fn new(node_id: NodeId) -> Node {
        Node {
            node_id,
            graph: CausalGraph::new(),
            outbox: Vec::new(),
            inbox: Vec::new(),
        }
    }

    /// Stages a `Create` event for the given task in the outbox.
    pub fn create_task(&mut self, task_id: TaskId, event_id: EventId, parents: Vec<EventId>) {
        self.outbox.push(Event {
            event_id,
            operation: Operation::Create(task_id),
            causal_parents: parents,
        });
    }

    /// Stages a `Claim` event (attributed to this node) in the outbox.
    pub fn claim_task(&mut self, task_id: TaskId, event_id: EventId, parents: Vec<EventId>) {
        self.outbox.push(Event {
            event_id,
            operation: Operation::Claim(task_id, self.node_id.clone()),
            causal_parents: parents,
        });
    }

    /// Stages a `Complete` event (attributed to this node) in the outbox.
    pub fn complete_task(&mut self, task_id: TaskId, event_id: EventId, parents: Vec<EventId>) {
        self.outbox.push(Event {
            event_id,
            operation: Operation::Complete(task_id, self.node_id.clone()),
            causal_parents: parents,
        });
    }

    /// Pushes a single event into the inbox.
    pub fn receive(&mut self, event: Event) {
        self.inbox.push(event);
    }

    /// Drains the inbox and inserts all events into the local graph.
    pub fn process_inbox(&mut self) {
        for event in self.inbox.drain(..) {
            self.graph.insert(event);
        }
    }

    /// Drains the outbox into a `Message` ready for broadcast.
    pub fn collect_outbox_as_message(&mut self) -> Message {
        Message::from_events(self.outbox.drain(..).collect())
    }

    /// Delivers a `Message` by pushing all its events into the inbox.
    pub fn receive_message(&mut self, message: Message) {
        for event in message.events {
            self.receive(event);
        }
    }
}

#[cfg(test)]
mod tests {
    use crdt_core::message::Message;
    use crdt_core::{Event, EventId, NodeId, Operation, TaskId};

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
            Event {
                event_id: EventId(1),
                operation: Operation::Create(TaskId(1)),
                causal_parents: vec![],
            },
            Event {
                event_id: EventId(2),
                operation: Operation::Create(TaskId(2)),
                causal_parents: vec![],
            },
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
}
