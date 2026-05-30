use std::collections::{HashMap, HashSet};

use crate::{Event, EventId};

/// Append-only directed acyclic graph of causal events.
///
/// This is the core CRDT data structure. Events are stored by `EventId` and
/// causal edges are recorded in a separate `children` map so that edges for
/// not-yet-received parents can be stored without blocking insertion.
#[derive(Clone, Default)]
pub struct CausalGraph {
    /// All known events, indexed by their unique `EventId`.
    pub events: HashMap<EventId, Event>,
    /// Maps each parent `EventId` to the set of `EventId`s that list it as a parent.
    pub children: HashMap<EventId, HashSet<EventId>>,
}

impl CausalGraph {
    /// Creates an empty `CausalGraph`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts an event into the graph.
    ///
    /// If an event with the same `event_id` already exists the call is a no-op.
    /// Causal edges are recorded for all entries in `causal_parents` even if
    /// those parent events have not yet been inserted.
    pub fn insert(&mut self, event: Event) {
        if self.events.contains_key(&event.event_id) {
            return;
        }

        for parent_id in &event.causal_parents {
            self.children
                .entry(parent_id.clone())
                .or_default()
                .insert(event.event_id.clone());
        }

        self.events.insert(event.event_id.clone(), event);
    }

    /// Merges another `CausalGraph` into this one.
    ///
    /// The operation is idempotent, commutative, and associative (CRDT join).
    /// Reusing `insert` for events guarantees the duplicate-skip and edge-recording
    /// logic stays in one place. The children union handles edges that `other` recorded
    /// for parents that were absent at insertion time — those edges may not be
    /// re-derived from the events alone, so we must merge the edge sets directly.
    pub fn merge(&mut self, other: &CausalGraph) {
        for event in other.events.values() {
            self.insert(event.clone());
        }

        for (parent_id, child_set) in &other.children {
            self.children
                .entry(parent_id.clone())
                .or_default()
                .extend(child_set.iter().cloned());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NodeId, Operation, TaskId};

    fn create_event(id: u128, op: Operation, parents: Vec<u128>) -> Event {
        Event {
            event_id: EventId(id),
            operation: op,
            causal_parents: parents.into_iter().map(EventId).collect(),
        }
    }

    #[test]
    fn new_graph_is_empty() {
        let graph = CausalGraph::new();
        assert!(graph.events.is_empty());
        assert!(graph.children.is_empty());
    }

    #[test]
    fn insert_single_event_no_parents() {
        let mut graph = CausalGraph::new();
        graph.insert(create_event(1, Operation::Create(TaskId(10)), vec![]));

        assert_eq!(graph.events.len(), 1);
        assert!(graph.children.is_empty());
    }

    #[test]
    fn insert_child_with_existing_parent() {
        let mut graph = CausalGraph::new();
        graph.insert(create_event(1, Operation::Create(TaskId(10)), vec![]));
        graph.insert(create_event(
            2,
            Operation::Claim(TaskId(10), NodeId(20)),
            vec![1],
        ));

        assert_eq!(graph.events.len(), 2);
        assert!(graph.children[&EventId(1)].contains(&EventId(2)));
    }

    #[test]
    fn insert_child_before_parent_arrives() {
        let mut graph = CausalGraph::new();
        graph.insert(create_event(
            2,
            Operation::Claim(TaskId(10), NodeId(20)),
            vec![1],
        ));

        assert_eq!(graph.events.len(), 1);
        assert!(graph.children[&EventId(1)].contains(&EventId(2)));
    }

    #[test]
    fn duplicate_insert_is_ignored() {
        let mut graph = CausalGraph::new();
        graph.insert(create_event(1, Operation::Create(TaskId(10)), vec![]));
        graph.insert(create_event(1, Operation::Create(TaskId(10)), vec![]));

        assert_eq!(graph.events.len(), 1);
    }

    #[test]
    fn merge_two_disjoint_graphs() {
        let mut a = CausalGraph::new();
        a.insert(create_event(1, Operation::Create(TaskId(1)), vec![]));

        let mut b = CausalGraph::new();
        b.insert(create_event(2, Operation::Create(TaskId(2)), vec![]));

        a.merge(&b);

        assert_eq!(a.events.len(), 2);
        assert!(a.events.contains_key(&EventId(1)));
        assert!(a.events.contains_key(&EventId(2)));
    }

    #[test]
    fn merge_overlapping_graphs() {
        let mut a = CausalGraph::new();
        a.insert(create_event(1, Operation::Create(TaskId(1)), vec![]));
        a.insert(create_event(
            2,
            Operation::Claim(TaskId(1), NodeId(1)),
            vec![1],
        ));

        let mut b = CausalGraph::new();
        b.insert(create_event(1, Operation::Create(TaskId(1)), vec![]));
        b.insert(create_event(
            3,
            Operation::Complete(TaskId(1), NodeId(1)),
            vec![1],
        ));

        a.merge(&b);

        assert_eq!(a.events.len(), 3);
    }

    #[test]
    fn merge_empty_into_non_empty() {
        let mut a = CausalGraph::new();
        a.insert(create_event(1, Operation::Create(TaskId(1)), vec![]));

        let empty = CausalGraph::new();
        a.merge(&empty);

        assert_eq!(a.events.len(), 1);
        assert!(a.children.is_empty());
    }

    #[test]
    fn merge_non_empty_into_empty() {
        let mut empty = CausalGraph::new();

        let mut source = CausalGraph::new();
        source.insert(create_event(1, Operation::Create(TaskId(1)), vec![]));
        source.insert(create_event(
            2,
            Operation::Claim(TaskId(1), NodeId(1)),
            vec![1],
        ));

        empty.merge(&source);

        assert_eq!(empty.events.len(), 2);
        assert!(empty.children[&EventId(1)].contains(&EventId(2)));
    }
}
