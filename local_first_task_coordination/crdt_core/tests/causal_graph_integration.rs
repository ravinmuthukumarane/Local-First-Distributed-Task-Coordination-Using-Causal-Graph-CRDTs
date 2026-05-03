use crdt_core::causal_graph::CausalGraph;
use crdt_core::{Event, EventId, NodeId, Operation, TaskId};

fn event(id: u128, op: Operation, parents: Vec<u128>) -> Event {
    Event {
        event_id: EventId(id),
        operation: op,
        causal_parents: parents.into_iter().map(EventId).collect(),
    }
}

fn create(id: u128) -> Event {
    event(id, Operation::Create(TaskId(id)), vec![])
}

fn child(id: u128, parents: Vec<u128>) -> Event {
    event(id, Operation::Claim(TaskId(id), NodeId(id)), parents)
}

// 1. Empty graph
#[test]
fn empty_graph_has_no_events_or_children() {
    let graph = CausalGraph::new();
    assert!(graph.events.is_empty());
    assert!(graph.children.is_empty());
}

// 2. Single root event
#[test]
fn single_root_event() {
    let mut graph = CausalGraph::new();
    graph.insert(create(1));

    assert_eq!(graph.events.len(), 1);
    assert!(graph.events.contains_key(&EventId(1)));
    assert!(!graph.children.contains_key(&EventId(1)));
}

// 3. Linear chain A → B → C
#[test]
fn linear_chain() {
    let mut graph = CausalGraph::new();
    graph.insert(create(1));
    graph.insert(child(2, vec![1]));
    graph.insert(child(3, vec![2]));

    assert_eq!(graph.events.len(), 3);
    assert!(graph.children[&EventId(1)].contains(&EventId(2)));
    assert!(graph.children[&EventId(2)].contains(&EventId(3)));
    assert!(!graph.children.contains_key(&EventId(3)));
}

// 4. Out-of-order insertion: C arrives before B or A
#[test]
fn out_of_order_insertion() {
    let mut graph = CausalGraph::new();
    graph.insert(child(3, vec![2]));
    graph.insert(child(2, vec![1]));
    graph.insert(create(1));

    assert_eq!(graph.events.len(), 3);
    assert!(graph.events.contains_key(&EventId(1)));
    assert!(graph.events.contains_key(&EventId(2)));
    assert!(graph.events.contains_key(&EventId(3)));
    assert!(graph.children[&EventId(1)].contains(&EventId(2)));
    assert!(graph.children[&EventId(2)].contains(&EventId(3)));
}

// 5. Duplicate insertion
#[test]
fn duplicate_insertion_ignored() {
    let mut graph = CausalGraph::new();
    graph.insert(create(1));
    graph.insert(create(1));
    graph.insert(create(1));

    assert_eq!(graph.events.len(), 1);
}

// 6. Diamond: A → B → D, A → C → D
#[test]
fn concurrent_diamond() {
    let mut graph = CausalGraph::new();
    graph.insert(create(1));
    graph.insert(child(2, vec![1]));
    graph.insert(child(3, vec![1]));
    graph.insert(event(
        4,
        Operation::Complete(TaskId(1), NodeId(1)),
        vec![2, 3],
    ));

    assert_eq!(graph.events.len(), 4);

    let children_of_a = &graph.children[&EventId(1)];
    assert!(children_of_a.contains(&EventId(2)));
    assert!(children_of_a.contains(&EventId(3)));

    assert!(graph.children[&EventId(2)].contains(&EventId(4)));
    assert!(graph.children[&EventId(3)].contains(&EventId(4)));
}
