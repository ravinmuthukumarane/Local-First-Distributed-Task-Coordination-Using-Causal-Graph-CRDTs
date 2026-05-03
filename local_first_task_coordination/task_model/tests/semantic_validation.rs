use crdt_core::causal_graph::CausalGraph;
use crdt_core::{Event, EventId, NodeId, Operation, TaskId};
use task_model::derive_task_state;

fn event(id: u128, op: Operation, parents: Vec<u128>) -> Event {
    Event {
        event_id: EventId(id),
        operation: op,
        causal_parents: parents.into_iter().map(EventId).collect(),
    }
}

// 1. Causal chain respected: Create → Claim → Complete
#[test]
fn causal_chain_respected() {
    let mut graph = CausalGraph::new();
    graph.insert(event(1, Operation::Create(TaskId(1)), vec![]));
    graph.insert(event(2, Operation::Claim(TaskId(1), NodeId(10)), vec![1]));
    graph.insert(event(3, Operation::Complete(TaskId(1), NodeId(10)), vec![2]));

    let state = derive_task_state(&graph, &TaskId(1));

    assert!(state.exists);
    assert_eq!(state.claims.len(), 1);
    assert_eq!(state.completions.len(), 1);
    assert!(!state.has_conflict);
}

// 2. Concurrent claims after shared create → conflict
#[test]
fn concurrent_claims_after_shared_create() {
    let mut graph = CausalGraph::new();
    graph.insert(event(1, Operation::Create(TaskId(1)), vec![]));
    // Both claims reference only the create — neither knows about the other
    graph.insert(event(2, Operation::Claim(TaskId(1), NodeId(10)), vec![1]));
    graph.insert(event(3, Operation::Claim(TaskId(1), NodeId(20)), vec![1]));

    let state = derive_task_state(&graph, &TaskId(1));

    assert!(state.has_conflict);
    assert_eq!(state.claims.len(), 2);
    assert!(state.claims.contains(&NodeId(10)));
    assert!(state.claims.contains(&NodeId(20)));
}

// 3. Duplicate creates are idempotent — has_conflict driven by claims/completions only
#[test]
fn duplicate_create_is_idempotent() {
    let mut graph = CausalGraph::new();
    graph.insert(event(1, Operation::Create(TaskId(1)), vec![]));
    graph.insert(event(2, Operation::Create(TaskId(1)), vec![]));

    let state = derive_task_state(&graph, &TaskId(1));

    assert!(state.exists);
    assert!(!state.has_conflict);
    assert!(state.claims.is_empty());
    assert!(state.completions.is_empty());
}

// 4. Complete without create — records what happened, does not enforce ordering
#[test]
fn complete_without_create() {
    let mut graph = CausalGraph::new();
    graph.insert(event(1, Operation::Complete(TaskId(1), NodeId(10)), vec![]));

    let state = derive_task_state(&graph, &TaskId(1));

    assert!(!state.exists);
    assert_eq!(state.completions.len(), 1);
}

// 5. Multi-task graph isolation
#[test]
fn multi_task_graph_isolation() {
    let mut graph = CausalGraph::new();
    // Task A lifecycle
    graph.insert(event(1, Operation::Create(TaskId(1)), vec![]));
    graph.insert(event(2, Operation::Claim(TaskId(1), NodeId(10)), vec![1]));
    graph.insert(event(3, Operation::Complete(TaskId(1), NodeId(10)), vec![2]));
    // Task B lifecycle
    graph.insert(event(4, Operation::Create(TaskId(2)), vec![]));
    graph.insert(event(5, Operation::Claim(TaskId(2), NodeId(20)), vec![4]));
    graph.insert(event(6, Operation::Complete(TaskId(2), NodeId(20)), vec![5]));

    let state_a = derive_task_state(&graph, &TaskId(1));
    assert!(state_a.exists);
    assert_eq!(state_a.claims, vec![NodeId(10)]);
    assert_eq!(state_a.completions, vec![NodeId(10)]);
    assert!(!state_a.has_conflict);

    let state_b = derive_task_state(&graph, &TaskId(2));
    assert!(state_b.exists);
    assert_eq!(state_b.claims, vec![NodeId(20)]);
    assert_eq!(state_b.completions, vec![NodeId(20)]);
    assert!(!state_b.has_conflict);
}

// 6. Idempotent merge preserves derived state
#[test]
fn idempotent_merge_preserves_derived_state() {
    let mut graph = CausalGraph::new();
    graph.insert(event(1, Operation::Create(TaskId(1)), vec![]));
    graph.insert(event(2, Operation::Claim(TaskId(1), NodeId(10)), vec![1]));
    graph.insert(event(3, Operation::Complete(TaskId(1), NodeId(10)), vec![2]));

    let state_before = derive_task_state(&graph, &TaskId(1));

    let copy = graph.clone();
    graph.merge(&copy);

    let state_after = derive_task_state(&graph, &TaskId(1));

    assert_eq!(state_before, state_after);
}
