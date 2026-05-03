use std::collections::HashSet;

use crdt_core::causal_graph::CausalGraph;
use crdt_core::{Event, EventId, NodeId, Operation, TaskId};

fn event(id: u128, op: Operation, parents: Vec<u128>) -> Event {
    Event {
        event_id: EventId(id),
        operation: op,
        causal_parents: parents.into_iter().map(EventId).collect(),
    }
}

fn graph_from(events: Vec<Event>) -> CausalGraph {
    let mut g = CausalGraph::new();
    for e in events {
        g.insert(e);
    }
    g
}

// 1. Idempotence: merge(A, A) == A
#[test]
fn idempotence() {
    let a = graph_from(vec![
        event(1, Operation::Create(TaskId(1)), vec![]),
        event(2, Operation::Claim(TaskId(1), NodeId(1)), vec![1]),
        event(3, Operation::Complete(TaskId(1), NodeId(1)), vec![2]),
    ]);

    let before_len = a.events.len();
    let before_children: HashSet<EventId> = a.children.keys().cloned().collect();

    let mut a_mut = a.clone();
    let a2 = a.clone();
    a_mut.merge(&a2);

    assert_eq!(a_mut.events.len(), before_len);
    let after_children: HashSet<EventId> = a_mut.children.keys().cloned().collect();
    assert_eq!(after_children, before_children);
}

// 2. Commutativity: merge(A, B).events == merge(B, A).events
#[test]
fn commutativity() {
    let a = graph_from(vec![
        event(1, Operation::Create(TaskId(1)), vec![]),
        event(2, Operation::Claim(TaskId(1), NodeId(1)), vec![1]),
    ]);
    let b = graph_from(vec![
        event(2, Operation::Claim(TaskId(1), NodeId(1)), vec![1]),
        event(3, Operation::Complete(TaskId(1), NodeId(1)), vec![2]),
    ]);

    let mut left = a.clone();
    left.merge(&b);

    let mut right = b.clone();
    right.merge(&a);

    assert_eq!(left.events.len(), right.events.len());

    let left_keys: HashSet<EventId> = left.events.keys().cloned().collect();
    let right_keys: HashSet<EventId> = right.events.keys().cloned().collect();
    assert_eq!(left_keys, right_keys);
}

// 3. Associativity: merge(merge(A, B), C) == merge(A, merge(B, C))
#[test]
fn associativity() {
    let a = graph_from(vec![
        event(1, Operation::Create(TaskId(1)), vec![]),
        event(99, Operation::Create(TaskId(99)), vec![]),
    ]);
    let b = graph_from(vec![
        event(2, Operation::Claim(TaskId(1), NodeId(1)), vec![1]),
        event(99, Operation::Create(TaskId(99)), vec![]),
    ]);
    let c = graph_from(vec![
        event(3, Operation::Complete(TaskId(1), NodeId(1)), vec![2]),
        event(99, Operation::Create(TaskId(99)), vec![]),
    ]);

    // left: (A merge B) merge C
    let mut left = a.clone();
    left.merge(&b);
    left.merge(&c);

    // right: A merge (B merge C)
    let mut bc = b.clone();
    bc.merge(&c);
    let mut right = a.clone();
    right.merge(&bc);

    assert_eq!(left.events, right.events);
    assert_eq!(left.children, right.children);
}
