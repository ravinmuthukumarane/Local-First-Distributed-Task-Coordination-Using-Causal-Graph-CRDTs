use crdt_core::causal_graph::CausalGraph;
use crdt_core::{NodeId, TaskId};

pub use crdt_core::Operation;

/// Extracts all events related to a specific task into a new `CausalGraph`.
///
/// An event belongs to the task if its `operation` field references `task_id`
/// in any variant. Causal edges between included events are preserved; edges
/// that cross task boundaries are dropped.
pub fn extract_task_subgraph(graph: &CausalGraph, task_id: &TaskId) -> CausalGraph {
    let mut sub = CausalGraph::new();

    for event in graph.events.values() {
        let matches = match &event.operation {
            Operation::Create(tid) => tid == task_id,
            Operation::Claim(tid, _) => tid == task_id,
            Operation::Complete(tid, _) => tid == task_id,
        };
        if matches {
            sub.insert(event.clone());
        }
    }

    // Union edges for every event id present in the subgraph. `insert` already
    // re-derives edges from causal_parents, but edges recorded in graph.children
    // for parents absent at insert time are only in the children map, not the
    // event payload — so we must carry them over explicitly.
    for event_id in sub.events.keys() {
        if let Some(child_set) = graph.children.get(event_id) {
            sub.children.entry(event_id.clone()).or_default().extend(
                child_set
                    .iter()
                    .filter(|c| sub.events.contains_key(c))
                    .cloned(),
            );
        }
    }

    sub
}

/// Derived view of a task's lifecycle state, computed from a `CausalGraph`.
#[derive(Debug, PartialEq, Eq)]
pub struct TaskState {
    /// `true` if at least one `Create` event for this task has been observed.
    pub exists: bool,
    /// All `NodeId`s that have claimed this task.
    pub claims: Vec<NodeId>,
    /// All `NodeId`s that have completed this task.
    pub completions: Vec<NodeId>,
    /// `true` if more than one node has claimed or more than one has completed.
    pub has_conflict: bool,
}

/// Derives the current `TaskState` for a task from a `CausalGraph`.
///
/// Calls [`extract_task_subgraph`] internally to scope the scan to relevant events.
/// The result is deterministic and depends only on the graph contents.
pub fn derive_task_state(graph: &CausalGraph, task_id: &TaskId) -> TaskState {
    let sub = extract_task_subgraph(graph, task_id);

    let mut exists = false;
    let mut claims: Vec<NodeId> = Vec::new();
    let mut completions: Vec<NodeId> = Vec::new();

    for event in sub.events.values() {
        match &event.operation {
            Operation::Create(_) => exists = true,
            Operation::Claim(_, node_id) => claims.push(node_id.clone()),
            Operation::Complete(_, node_id) => completions.push(node_id.clone()),
        }
    }

    let has_conflict = claims.len() > 1 || completions.len() > 1;

    TaskState {
        exists,
        claims,
        completions,
        has_conflict,
    }
}

#[cfg(test)]
mod tests {
    use crdt_core::causal_graph::CausalGraph;
    use crdt_core::{Event, EventId, NodeId, Operation, TaskId};

    use super::*;

    fn event(id: u128, op: Operation, parents: Vec<u128>) -> Event {
        Event {
            event_id: EventId(id),
            operation: op,
            causal_parents: parents.into_iter().map(EventId).collect(),
        }
    }

    #[test]
    fn variants_are_not_equal() {
        let create = Operation::Create(TaskId(1));
        let claim = Operation::Claim(TaskId(1), NodeId(2));
        let complete = Operation::Complete(TaskId(1), NodeId(2));

        assert_ne!(create, claim);
        assert_ne!(claim, complete);
        assert_ne!(create, complete);
    }

    #[test]
    fn no_matching_events_returns_empty_subgraph() {
        let mut graph = CausalGraph::new();
        graph.insert(event(1, Operation::Create(TaskId(1)), vec![]));
        graph.insert(event(2, Operation::Claim(TaskId(1), NodeId(1)), vec![1]));

        let sub = extract_task_subgraph(&graph, &TaskId(99));

        assert!(sub.events.is_empty());
        assert!(sub.children.is_empty());
    }

    #[test]
    fn all_events_match_returns_full_subgraph() {
        let mut graph = CausalGraph::new();
        graph.insert(event(1, Operation::Create(TaskId(42)), vec![]));
        graph.insert(event(2, Operation::Claim(TaskId(42), NodeId(1)), vec![1]));
        graph.insert(event(
            3,
            Operation::Complete(TaskId(42), NodeId(1)),
            vec![2],
        ));

        let sub = extract_task_subgraph(&graph, &TaskId(42));

        assert_eq!(sub.events.len(), 3);
    }

    #[test]
    fn mixed_tasks_extracts_correct_partition() {
        let mut graph = CausalGraph::new();
        graph.insert(event(1, Operation::Create(TaskId(1)), vec![]));
        graph.insert(event(2, Operation::Claim(TaskId(1), NodeId(1)), vec![1]));
        graph.insert(event(3, Operation::Create(TaskId(2)), vec![]));
        graph.insert(event(4, Operation::Complete(TaskId(2), NodeId(2)), vec![3]));

        let sub_a = extract_task_subgraph(&graph, &TaskId(1));
        assert_eq!(sub_a.events.len(), 2);
        assert!(sub_a.events.contains_key(&EventId(1)));
        assert!(sub_a.events.contains_key(&EventId(2)));

        let sub_b = extract_task_subgraph(&graph, &TaskId(2));
        assert_eq!(sub_b.events.len(), 2);
        assert!(sub_b.events.contains_key(&EventId(3)));
        assert!(sub_b.events.contains_key(&EventId(4)));
    }

    #[test]
    fn causal_edges_preserved_in_subgraph() {
        let mut graph = CausalGraph::new();
        graph.insert(event(1, Operation::Create(TaskId(7)), vec![]));
        graph.insert(event(2, Operation::Claim(TaskId(7), NodeId(1)), vec![1]));
        graph.insert(event(3, Operation::Complete(TaskId(7), NodeId(1)), vec![2]));

        let sub = extract_task_subgraph(&graph, &TaskId(7));

        assert_eq!(sub.events.len(), 3);
        assert!(sub.children[&EventId(1)].contains(&EventId(2)));
        assert!(sub.children[&EventId(2)].contains(&EventId(3)));
        assert!(!sub.children.contains_key(&EventId(3)));
    }

    #[test]
    fn task_not_created_returns_empty_state() {
        let state = derive_task_state(&CausalGraph::new(), &TaskId(1));

        assert!(!state.exists);
        assert!(state.claims.is_empty());
        assert!(state.completions.is_empty());
        assert!(!state.has_conflict);
    }

    #[test]
    fn task_created_only() {
        let mut graph = CausalGraph::new();
        graph.insert(event(1, Operation::Create(TaskId(1)), vec![]));

        let state = derive_task_state(&graph, &TaskId(1));

        assert!(state.exists);
        assert!(state.claims.is_empty());
        assert!(state.completions.is_empty());
        assert!(!state.has_conflict);
    }

    #[test]
    fn single_claim_no_conflict() {
        let mut graph = CausalGraph::new();
        graph.insert(event(1, Operation::Create(TaskId(1)), vec![]));
        graph.insert(event(2, Operation::Claim(TaskId(1), NodeId(10)), vec![1]));

        let state = derive_task_state(&graph, &TaskId(1));

        assert!(state.exists);
        assert_eq!(state.claims, vec![NodeId(10)]);
        assert!(!state.has_conflict);
    }

    #[test]
    fn concurrent_claims_causes_conflict() {
        let mut graph = CausalGraph::new();
        graph.insert(event(1, Operation::Create(TaskId(1)), vec![]));
        graph.insert(event(2, Operation::Claim(TaskId(1), NodeId(10)), vec![1]));
        graph.insert(event(3, Operation::Claim(TaskId(1), NodeId(20)), vec![1]));

        let state = derive_task_state(&graph, &TaskId(1));

        assert_eq!(state.claims.len(), 2);
        assert!(state.claims.contains(&NodeId(10)));
        assert!(state.claims.contains(&NodeId(20)));
        assert!(state.has_conflict);
    }

    #[test]
    fn single_completion_no_conflict() {
        let mut graph = CausalGraph::new();
        graph.insert(event(1, Operation::Create(TaskId(1)), vec![]));
        graph.insert(event(2, Operation::Claim(TaskId(1), NodeId(10)), vec![1]));
        graph.insert(event(
            3,
            Operation::Complete(TaskId(1), NodeId(10)),
            vec![2],
        ));

        let state = derive_task_state(&graph, &TaskId(1));

        assert_eq!(state.completions, vec![NodeId(10)]);
        assert!(!state.has_conflict);
    }

    #[test]
    fn duplicate_completions_causes_conflict() {
        let mut graph = CausalGraph::new();
        graph.insert(event(1, Operation::Create(TaskId(1)), vec![]));
        graph.insert(event(
            2,
            Operation::Complete(TaskId(1), NodeId(10)),
            vec![1],
        ));
        graph.insert(event(
            3,
            Operation::Complete(TaskId(1), NodeId(20)),
            vec![1],
        ));

        let state = derive_task_state(&graph, &TaskId(1));

        assert_eq!(state.completions.len(), 2);
        assert!(state.has_conflict);
    }

    #[test]
    fn wrong_task_id_returns_empty_state() {
        let mut graph = CausalGraph::new();
        graph.insert(event(1, Operation::Create(TaskId(1)), vec![]));
        graph.insert(event(2, Operation::Claim(TaskId(1), NodeId(10)), vec![1]));

        let state = derive_task_state(&graph, &TaskId(2));

        assert!(!state.exists);
        assert!(state.claims.is_empty());
        assert!(state.completions.is_empty());
        assert!(!state.has_conflict);
    }
}
