pub mod causal_graph;
pub mod message;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TaskId(pub u128);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodeId(pub u128);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EventId(pub u128);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Operation {
    Create(TaskId),
    Claim(TaskId, NodeId),
    Complete(TaskId, NodeId),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Event {
    pub event_id: EventId,
    pub operation: Operation,
    pub causal_parents: Vec<EventId>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn ids_are_hashable() {
        let task_id = TaskId(1);
        let node_id = NodeId(2);
        let event_id = EventId(3);

        let mut task_set: HashSet<TaskId> = HashSet::new();
        let mut node_set: HashSet<NodeId> = HashSet::new();
        let mut event_set: HashSet<EventId> = HashSet::new();

        task_set.insert(task_id.clone());
        node_set.insert(node_id.clone());
        event_set.insert(event_id.clone());

        assert!(task_set.contains(&task_id));
        assert!(node_set.contains(&node_id));
        assert!(event_set.contains(&event_id));
    }

    #[test]
    fn events_in_hashset() {
        let e1 = Event {
            event_id: EventId(1),
            operation: Operation::Create(TaskId(10)),
            causal_parents: vec![],
        };
        let e2 = Event {
            event_id: EventId(2),
            operation: Operation::Claim(TaskId(10), NodeId(20)),
            causal_parents: vec![],
        };
        let e3 = Event {
            event_id: EventId(3),
            operation: Operation::Complete(TaskId(10), NodeId(20)),
            causal_parents: vec![EventId(1), EventId(2)],
        };

        let mut set: HashSet<Event> = HashSet::new();
        set.insert(e1);
        set.insert(e2);
        set.insert(e3);

        assert_eq!(set.len(), 3);
    }
}
