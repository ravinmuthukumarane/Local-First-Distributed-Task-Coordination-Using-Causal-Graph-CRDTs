use crdt_core::causal_graph::CausalGraph;
use crdt_core::message::Message;
use crdt_core::{Event, EventId, NodeId, Operation, TaskId};

use crate::simulation::EventCounter;

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
            operation: Operation::Claim(task_id, self.node_id),
            causal_parents: parents,
        });
    }

    /// Stages a `Complete` event (attributed to this node) in the outbox.
    pub fn complete_task(&mut self, task_id: TaskId, event_id: EventId, parents: Vec<EventId>) {
        self.outbox.push(Event {
            event_id,
            operation: Operation::Complete(task_id, self.node_id),
            causal_parents: parents,
        });
    }

    /// Draws the next id from `counter`, stages a `Create` event with it, and
    /// returns the id so the caller can thread it into a later event's
    /// `causal_parents` without generating the id separately.
    ///
    /// Prefer this over [`Self::create_task`] whenever the caller would
    /// otherwise have to call `counter.next_id()` itself: routing every id
    /// through this method removes the risk of two events being staged with
    /// the same `EventId` by mistake (a plain typo when copying a `next_id()`
    /// result, for example), since the counter is the only source of ids.
    pub fn create_task_next(
        &mut self,
        task_id: TaskId,
        counter: &mut EventCounter,
        parents: Vec<EventId>,
    ) -> EventId {
        let event_id = counter.next_id();
        self.create_task(task_id, event_id, parents);
        event_id
    }

    /// Draws the next id from `counter`, stages a `Claim` event with it, and
    /// returns the id. See [`Self::create_task_next`] for why this is
    /// preferred over [`Self::claim_task`] at call sites that have a counter.
    pub fn claim_task_next(
        &mut self,
        task_id: TaskId,
        counter: &mut EventCounter,
        parents: Vec<EventId>,
    ) -> EventId {
        let event_id = counter.next_id();
        self.claim_task(task_id, event_id, parents);
        event_id
    }

    /// Draws the next id from `counter`, stages a `Complete` event with it,
    /// and returns the id. See [`Self::create_task_next`] for why this is
    /// preferred over [`Self::complete_task`] at call sites that have a counter.
    pub fn complete_task_next(
        &mut self,
        task_id: TaskId,
        counter: &mut EventCounter,
        parents: Vec<EventId>,
    ) -> EventId {
        let event_id = counter.next_id();
        self.complete_task(task_id, event_id, parents);
        event_id
    }

    /// Returns this node's current causal frontier for `task_id`: the ids of
    /// events (for this task) that this node knows of and that have no known
    /// child within the task's history — the "heads" of its local view.
    ///
    /// Considers both `self.graph` (events already merged in) and
    /// `self.outbox` (this node's own not-yet-delivered writes), so a node's
    /// own pending actions are visible to itself immediately rather than
    /// only after a full broadcast/deliver round-trip. Passing this frontier
    /// as `causal_parents` for a new event makes it depend on everything
    /// this node currently knows about the task — the same rule a real
    /// client would apply automatically instead of a scenario wiring
    /// `causal_parents` by hand.
    ///
    /// For a task this node has never seen, the frontier is empty, which is
    /// exactly the right `causal_parents` for that task's `Create` event.
    pub fn frontier_for(&self, task_id: TaskId) -> Vec<EventId> {
        let mut known = self.graph.clone();
        for event in &self.outbox {
            known.insert(event.clone());
        }

        let sub = task_model::extract_task_subgraph(&known, &task_id);
        sub.events()
            .keys()
            .filter(|id| {
                sub.children_of(id)
                    .is_none_or(|children| children.is_empty())
            })
            .copied()
            .collect()
    }

    /// Stages a `Create` event whose `causal_parents` is this node's current
    /// frontier for `task_id` (see [`Self::frontier_for`]) and whose id comes
    /// from `counter`. Returns the new event's id.
    pub fn create_task_auto(&mut self, task_id: TaskId, counter: &mut EventCounter) -> EventId {
        let parents = self.frontier_for(task_id);
        self.create_task_next(task_id, counter, parents)
    }

    /// Stages a `Claim` event whose `causal_parents` is this node's current
    /// frontier for `task_id`. See [`Self::create_task_auto`].
    pub fn claim_task_auto(&mut self, task_id: TaskId, counter: &mut EventCounter) -> EventId {
        let parents = self.frontier_for(task_id);
        self.claim_task_next(task_id, counter, parents)
    }

    /// Stages a `Complete` event whose `causal_parents` is this node's
    /// current frontier for `task_id`. See [`Self::create_task_auto`].
    pub fn complete_task_auto(&mut self, task_id: TaskId, counter: &mut EventCounter) -> EventId {
        let parents = self.frontier_for(task_id);
        self.complete_task_next(task_id, counter, parents)
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
    fn create_task_next_draws_from_counter_and_returns_id() {
        let mut n = node();
        let mut counter = EventCounter::new(5);

        let returned = n.create_task_next(TaskId(1), &mut counter, vec![]);

        assert_eq!(returned, EventId(5));
        assert_eq!(n.outbox.len(), 1);
        assert_eq!(n.outbox[0].event_id, EventId(5));
        assert_eq!(n.outbox[0].operation, Operation::Create(TaskId(1)));
    }

    #[test]
    fn claim_and_complete_task_next_advance_shared_counter() {
        let mut n = node();
        let mut counter = EventCounter::new(1);

        let create_id = n.create_task_next(TaskId(1), &mut counter, vec![]);
        let claim_id = n.claim_task_next(TaskId(1), &mut counter, vec![create_id]);
        let complete_id = n.complete_task_next(TaskId(1), &mut counter, vec![claim_id]);

        // Each _next call draws a fresh id from the same counter, so no two
        // staged events collide even though the caller never picks an id itself.
        assert_eq!(
            [create_id, claim_id, complete_id],
            [EventId(1), EventId(2), EventId(3)]
        );
        assert_eq!(n.outbox.len(), 3);
        assert_eq!(
            n.outbox[1].operation,
            Operation::Claim(TaskId(1), n.node_id)
        );
        assert_eq!(
            n.outbox[2].operation,
            Operation::Complete(TaskId(1), n.node_id)
        );
    }

    #[test]
    fn frontier_for_unknown_task_is_empty() {
        let n = node();
        assert!(n.frontier_for(TaskId(1)).is_empty());
    }

    #[test]
    fn create_task_auto_has_no_parents_for_a_new_task() {
        let mut n = node();
        let mut counter = EventCounter::new(1);

        n.create_task_auto(TaskId(1), &mut counter);

        assert!(n.outbox[0].causal_parents.is_empty());
    }

    #[test]
    fn claim_task_auto_cites_pending_outbox_create_as_parent() {
        // create_task_auto only stages the Create in the outbox — it hasn't
        // reached self.graph yet — so claim_task_auto must still see it via
        // the outbox to pick it up as a parent.
        let mut n = node();
        let mut counter = EventCounter::new(1);

        let create_id = n.create_task_auto(TaskId(1), &mut counter);
        n.claim_task_auto(TaskId(1), &mut counter);

        assert_eq!(n.outbox[1].causal_parents, vec![create_id]);
    }

    #[test]
    fn frontier_advances_past_events_that_have_children() {
        let mut n = node();
        let mut counter = EventCounter::new(1);

        let create_id = n.create_task_auto(TaskId(1), &mut counter);
        assert_eq!(n.frontier_for(TaskId(1)), vec![create_id]);

        let claim_id = n.claim_task_auto(TaskId(1), &mut counter);
        // create_id now has a child (the claim), so it drops out of the
        // frontier in favor of the claim, which has none yet.
        assert_eq!(n.frontier_for(TaskId(1)), vec![claim_id]);
    }

    #[test]
    fn frontier_includes_events_already_merged_into_the_graph() {
        let mut n = node();
        n.graph.insert(Event {
            event_id: EventId(1),
            operation: Operation::Create(TaskId(1)),
            causal_parents: vec![],
        });

        assert_eq!(n.frontier_for(TaskId(1)), vec![EventId(1)]);
    }

    #[test]
    fn frontier_has_multiple_heads_for_concurrent_claims() {
        let mut n = node();
        n.graph.insert(Event {
            event_id: EventId(1),
            operation: Operation::Create(TaskId(1)),
            causal_parents: vec![],
        });
        n.graph.insert(Event {
            event_id: EventId(2),
            operation: Operation::Claim(TaskId(1), NodeId(10)),
            causal_parents: vec![EventId(1)],
        });
        n.graph.insert(Event {
            event_id: EventId(3),
            operation: Operation::Claim(TaskId(1), NodeId(20)),
            causal_parents: vec![EventId(1)],
        });

        let mut frontier = n.frontier_for(TaskId(1));
        frontier.sort_by_key(|id| id.0);
        assert_eq!(frontier, vec![EventId(2), EventId(3)]);
    }

    #[test]
    fn auto_chain_matches_manually_wired_causal_parents() {
        // Building a Create -> Claim -> Complete chain via the _auto methods
        // should produce exactly the same causal_parents a scenario would
        // have wired by hand.
        let mut n = node();
        let mut counter = EventCounter::new(1);

        let create_id = n.create_task_auto(TaskId(1), &mut counter);
        let claim_id = n.claim_task_auto(TaskId(1), &mut counter);
        let complete_id = n.complete_task_auto(TaskId(1), &mut counter);

        assert_eq!(n.outbox[0].causal_parents, Vec::<EventId>::new());
        assert_eq!(n.outbox[1].causal_parents, vec![create_id]);
        assert_eq!(n.outbox[2].causal_parents, vec![claim_id]);
        assert_eq!(complete_id, n.outbox[2].event_id);
    }

    #[test]
    fn two_nodes_start_empty_with_distinct_ids() {
        let a = Node::new(NodeId(1));
        let b = Node::new(NodeId(2));

        assert!(a.graph.events().is_empty());
        assert!(a.outbox.is_empty());
        assert!(a.inbox.is_empty());

        assert!(b.graph.events().is_empty());
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

        assert!(n.graph.events().is_empty());
    }

    #[test]
    fn empty_inbox_does_nothing() {
        let mut n = node();
        n.process_inbox();

        assert!(n.graph.events().is_empty());
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

        assert_eq!(n.graph.events().len(), 1);
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

        assert_eq!(n.graph.events().len(), 3);
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
