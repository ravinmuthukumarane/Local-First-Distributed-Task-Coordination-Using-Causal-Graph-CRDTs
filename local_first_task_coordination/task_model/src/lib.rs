use std::collections::HashSet;

use crdt_core::causal_graph::CausalGraph;
use crdt_core::{EventId, NodeId, TaskId};

pub use crdt_core::Operation;

/// Extracts all events related to a specific task into a new `CausalGraph`.
///
/// An event belongs to the task if its `operation` field references `task_id`
/// in any variant. Causal edges between included events are preserved; edges
/// that cross task boundaries are dropped.
///
/// Looks events up via `graph`'s internal `by_task` index rather than
/// scanning every event in the graph, so cost scales with the number of
/// events for this task, not with the total graph size.
pub fn extract_task_subgraph(graph: &CausalGraph, task_id: &TaskId) -> CausalGraph {
    let mut sub = CausalGraph::new();

    for event in graph.events_for_task(task_id) {
        sub.insert(event.clone());
    }

    // Union edges for every event id present in the subgraph. `insert` already
    // re-derives edges from causal_parents, but edges recorded in graph.children
    // for parents absent at insert time are only in the children map, not the
    // event payload — so we must carry them over explicitly.
    let sub_event_ids: Vec<EventId> = sub.events().keys().copied().collect();
    for event_id in sub_event_ids {
        if let Some(child_set) = graph.children_of(&event_id) {
            for child_id in child_set {
                if sub.contains_event(child_id) {
                    sub.record_edge(event_id, *child_id);
                }
            }
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
    /// `true` if two or more claims (or two or more completions) are
    /// causally concurrent — i.e. neither one is a causal ancestor of the
    /// other. A later claim that causally follows an earlier claim (for
    /// example a re-claim staged after the original claimant's `Complete`)
    /// is *not* a conflict even though `claims.len() > 1`.
    pub has_conflict: bool,
    /// Deterministic conflict-*resolution* winner among `claims`: the
    /// `NodeId` every node converges on as the task's current owner once
    /// they've observed the same events, regardless of the order in which
    /// they arrived. `None` if the task has never been claimed. See
    /// [`resolve`] for the policy; this does not imply `has_conflict` is
    /// `false` — a resolved winner is picked even when claims are conflicting.
    pub resolved_claimant: Option<NodeId>,
    /// The same resolution policy applied to `completions`.
    pub resolved_completer: Option<NodeId>,
}

/// Deterministically resolves a set of same-kind events (e.g. all `Claim`
/// events for one task) to a single winning `NodeId`.
///
/// Policy: an event is "superseded" if some other event in `events`
/// causally follows it. The winner is chosen among the non-superseded
/// ("maximal") events — the ones nothing else in the set causally follows.
/// If exactly one remains, it's the unambiguous winner: a later event in the
/// same causal chain (e.g. a re-claim staged after the original claimant's
/// `Complete`) always beats an earlier one it causally supersedes. If more
/// than one remains, those events are mutually concurrent — a genuine
/// conflict — and the tie is broken by the lowest `NodeId`, so every node
/// that has observed the same events picks the same winner regardless of
/// delivery order. This is a policy choice (lowest id wins), not a
/// correctness requirement; callers wanting a different tie-break can filter
/// to the maximal set themselves using [`extract_task_subgraph`] and their
/// own comparison.
fn resolve(graph: &CausalGraph, events: &[(EventId, NodeId)]) -> Option<NodeId> {
    let maximal_owners: Vec<NodeId> = events
        .iter()
        .filter(|(id, _)| {
            !events
                .iter()
                .any(|(other_id, _)| other_id != id && causally_precedes(graph, id, other_id))
        })
        .map(|(_, node_id)| *node_id)
        .collect();

    maximal_owners.iter().map(|n| n.0).min().map(NodeId)
}

/// Returns `true` if `ancestor` causally precedes `descendant` — i.e.
/// `descendant` lists `ancestor` in its `causal_parents`, directly or
/// transitively through intermediate events.
///
/// Walks `graph` (not a task-scoped subgraph) because an event's
/// `causal_parents` may reference an event that didn't match the task
/// filter in [`extract_task_subgraph`].
fn causally_precedes(graph: &CausalGraph, ancestor: &EventId, descendant: &EventId) -> bool {
    let mut frontier = vec![*descendant];
    let mut visited: HashSet<EventId> = HashSet::new();

    while let Some(current) = frontier.pop() {
        if !visited.insert(current) {
            continue;
        }
        if let Some(event) = graph.events().get(&current) {
            for parent in &event.causal_parents {
                if parent == ancestor {
                    return true;
                }
                frontier.push(*parent);
            }
        }
    }

    false
}

/// Returns `true` if any two events in `event_ids` are causally concurrent
/// (neither is an ancestor of the other) — a genuine conflict, as opposed to
/// a later event that causally supersedes an earlier one.
fn has_concurrent_pair(graph: &CausalGraph, event_ids: &[EventId]) -> bool {
    for i in 0..event_ids.len() {
        for j in (i + 1)..event_ids.len() {
            let (a, b) = (&event_ids[i], &event_ids[j]);
            if !causally_precedes(graph, a, b) && !causally_precedes(graph, b, a) {
                return true;
            }
        }
    }
    false
}

/// Derives the current `TaskState` for a task from a `CausalGraph`.
///
/// Calls [`extract_task_subgraph`] internally to scope the scan to relevant events.
/// The result is deterministic and depends only on the graph contents.
pub fn derive_task_state(graph: &CausalGraph, task_id: &TaskId) -> TaskState {
    let sub = extract_task_subgraph(graph, task_id);

    let mut exists = false;
    let mut claim_events: Vec<(EventId, NodeId)> = Vec::new();
    let mut completion_events: Vec<(EventId, NodeId)> = Vec::new();

    for (event_id, event) in sub.events() {
        match &event.operation {
            Operation::Create(_) => exists = true,
            Operation::Claim(_, node_id) => claim_events.push((*event_id, *node_id)),
            Operation::Complete(_, node_id) => completion_events.push((*event_id, *node_id)),
        }
    }

    let claim_ids: Vec<EventId> = claim_events.iter().map(|(id, _)| *id).collect();
    let completion_ids: Vec<EventId> = completion_events.iter().map(|(id, _)| *id).collect();
    let has_conflict =
        has_concurrent_pair(graph, &claim_ids) || has_concurrent_pair(graph, &completion_ids);

    let resolved_claimant = resolve(graph, &claim_events);
    let resolved_completer = resolve(graph, &completion_events);

    TaskState {
        exists,
        claims: claim_events.into_iter().map(|(_, n)| n).collect(),
        completions: completion_events.into_iter().map(|(_, n)| n).collect(),
        has_conflict,
        resolved_claimant,
        resolved_completer,
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

        assert!(sub.events().is_empty());
        assert!(sub.children().is_empty());
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

        assert_eq!(sub.events().len(), 3);
    }

    #[test]
    fn mixed_tasks_extracts_correct_partition() {
        let mut graph = CausalGraph::new();
        graph.insert(event(1, Operation::Create(TaskId(1)), vec![]));
        graph.insert(event(2, Operation::Claim(TaskId(1), NodeId(1)), vec![1]));
        graph.insert(event(3, Operation::Create(TaskId(2)), vec![]));
        graph.insert(event(4, Operation::Complete(TaskId(2), NodeId(2)), vec![3]));

        let sub_a = extract_task_subgraph(&graph, &TaskId(1));
        assert_eq!(sub_a.events().len(), 2);
        assert!(sub_a.events().contains_key(&EventId(1)));
        assert!(sub_a.events().contains_key(&EventId(2)));

        let sub_b = extract_task_subgraph(&graph, &TaskId(2));
        assert_eq!(sub_b.events().len(), 2);
        assert!(sub_b.events().contains_key(&EventId(3)));
        assert!(sub_b.events().contains_key(&EventId(4)));
    }

    #[test]
    fn causal_edges_preserved_in_subgraph() {
        let mut graph = CausalGraph::new();
        graph.insert(event(1, Operation::Create(TaskId(7)), vec![]));
        graph.insert(event(2, Operation::Claim(TaskId(7), NodeId(1)), vec![1]));
        graph.insert(event(3, Operation::Complete(TaskId(7), NodeId(1)), vec![2]));

        let sub = extract_task_subgraph(&graph, &TaskId(7));

        assert_eq!(sub.events().len(), 3);
        assert!(sub.children()[&EventId(1)].contains(&EventId(2)));
        assert!(sub.children()[&EventId(2)].contains(&EventId(3)));
        assert!(!sub.children().contains_key(&EventId(3)));
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
    fn sequential_reclaim_after_completion_is_not_a_conflict() {
        // Claim(10) -> Complete(10) -> Claim(20): each event causally
        // follows the last, so the two claims are ordered, not concurrent.
        // A naive `claims.len() > 1` check would wrongly flag this.
        let mut graph = CausalGraph::new();
        graph.insert(event(1, Operation::Create(TaskId(1)), vec![]));
        graph.insert(event(2, Operation::Claim(TaskId(1), NodeId(10)), vec![1]));
        graph.insert(event(
            3,
            Operation::Complete(TaskId(1), NodeId(10)),
            vec![2],
        ));
        graph.insert(event(4, Operation::Claim(TaskId(1), NodeId(20)), vec![3]));

        let state = derive_task_state(&graph, &TaskId(1));

        assert_eq!(state.claims.len(), 2);
        assert!(!state.has_conflict);
    }

    #[test]
    fn transitively_ordered_claims_are_not_a_conflict() {
        // Claim(20)'s parent is Complete(10), whose parent is Claim(10) —
        // the ancestry is two hops away, not a direct parent, and must still
        // be detected as ordering rather than concurrency.
        let mut graph = CausalGraph::new();
        graph.insert(event(1, Operation::Create(TaskId(1)), vec![]));
        graph.insert(event(2, Operation::Claim(TaskId(1), NodeId(10)), vec![1]));
        graph.insert(event(
            3,
            Operation::Complete(TaskId(1), NodeId(10)),
            vec![2],
        ));
        graph.insert(event(4, Operation::Claim(TaskId(1), NodeId(20)), vec![3]));
        graph.insert(event(
            5,
            Operation::Complete(TaskId(1), NodeId(20)),
            vec![4],
        ));

        let state = derive_task_state(&graph, &TaskId(1));

        assert!(!state.has_conflict);
    }

    #[test]
    fn resolved_claimant_is_the_only_claimant_when_uncontested() {
        let mut graph = CausalGraph::new();
        graph.insert(event(1, Operation::Create(TaskId(1)), vec![]));
        graph.insert(event(2, Operation::Claim(TaskId(1), NodeId(10)), vec![1]));

        let state = derive_task_state(&graph, &TaskId(1));

        assert_eq!(state.resolved_claimant, Some(NodeId(10)));
    }

    #[test]
    fn resolved_claimant_is_none_when_task_never_claimed() {
        let mut graph = CausalGraph::new();
        graph.insert(event(1, Operation::Create(TaskId(1)), vec![]));

        let state = derive_task_state(&graph, &TaskId(1));

        assert_eq!(state.resolved_claimant, None);
    }

    #[test]
    fn resolved_claimant_prefers_causally_later_reclaim() {
        // Claim(10) -> Complete(10) -> Claim(20): 20's claim causally
        // supersedes 10's, so it wins even though NodeId(10) < NodeId(20)
        // (ruling out "resolution is just picking the lowest NodeId").
        let mut graph = CausalGraph::new();
        graph.insert(event(1, Operation::Create(TaskId(1)), vec![]));
        graph.insert(event(2, Operation::Claim(TaskId(1), NodeId(10)), vec![1]));
        graph.insert(event(
            3,
            Operation::Complete(TaskId(1), NodeId(10)),
            vec![2],
        ));
        graph.insert(event(4, Operation::Claim(TaskId(1), NodeId(20)), vec![3]));

        let state = derive_task_state(&graph, &TaskId(1));

        assert!(!state.has_conflict);
        assert_eq!(state.resolved_claimant, Some(NodeId(20)));
    }

    #[test]
    fn resolved_claimant_breaks_genuine_conflict_by_lowest_node_id() {
        let mut graph = CausalGraph::new();
        graph.insert(event(1, Operation::Create(TaskId(1)), vec![]));
        graph.insert(event(2, Operation::Claim(TaskId(1), NodeId(20)), vec![1]));
        graph.insert(event(3, Operation::Claim(TaskId(1), NodeId(10)), vec![1]));

        let state = derive_task_state(&graph, &TaskId(1));

        assert!(state.has_conflict);
        assert_eq!(state.resolved_claimant, Some(NodeId(10)));
    }

    #[test]
    fn resolved_claimant_is_deterministic_regardless_of_arrival_order() {
        // Two nodes see the same two concurrent claims in opposite insertion
        // order; both must resolve to the same winner.
        let mut forward = CausalGraph::new();
        forward.insert(event(1, Operation::Create(TaskId(1)), vec![]));
        forward.insert(event(2, Operation::Claim(TaskId(1), NodeId(30)), vec![1]));
        forward.insert(event(3, Operation::Claim(TaskId(1), NodeId(5)), vec![1]));

        let mut reverse = CausalGraph::new();
        reverse.insert(event(3, Operation::Claim(TaskId(1), NodeId(5)), vec![1]));
        reverse.insert(event(2, Operation::Claim(TaskId(1), NodeId(30)), vec![1]));
        reverse.insert(event(1, Operation::Create(TaskId(1)), vec![]));

        let state_forward = derive_task_state(&forward, &TaskId(1));
        let state_reverse = derive_task_state(&reverse, &TaskId(1));

        assert_eq!(state_forward.resolved_claimant, Some(NodeId(5)));
        assert_eq!(
            state_forward.resolved_claimant,
            state_reverse.resolved_claimant
        );
    }

    #[test]
    fn resolved_completer_mirrors_the_same_policy() {
        let mut graph = CausalGraph::new();
        graph.insert(event(1, Operation::Create(TaskId(1)), vec![]));
        graph.insert(event(
            2,
            Operation::Complete(TaskId(1), NodeId(20)),
            vec![1],
        ));
        graph.insert(event(
            3,
            Operation::Complete(TaskId(1), NodeId(10)),
            vec![1],
        ));

        let state = derive_task_state(&graph, &TaskId(1));

        assert!(state.has_conflict);
        assert_eq!(state.resolved_completer, Some(NodeId(10)));
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
