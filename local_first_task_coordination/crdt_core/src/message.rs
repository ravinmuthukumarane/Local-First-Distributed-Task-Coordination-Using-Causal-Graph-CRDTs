use std::collections::HashSet;

use crate::Event;

/// A batch of events exchanged between nodes during a synchronisation round.
///
/// Events are stored in a `HashSet` so that duplicates are automatically
/// deduplicated upon insertion.
#[derive(Clone, Debug, Default)]
pub struct Message {
    /// The set of events carried by this message.
    pub events: HashSet<Event>,
}

impl Message {
    /// Creates an empty `Message`.
    pub fn new() -> Message {
        Message::default()
    }

    /// Adds an event to the message, deduplicating by `EventId`.
    pub fn add(&mut self, event: Event) {
        self.events.insert(event);
    }

    /// Constructs a `Message` from a vector of events.
    pub fn from_events(events: Vec<Event>) -> Message {
        let mut m = Message::new();
        for e in events {
            m.add(e);
        }
        m
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EventId, NodeId, Operation, TaskId};

    fn event(id: u128) -> Event {
        Event {
            event_id: EventId(id),
            operation: Operation::Create(TaskId(id)),
            causal_parents: vec![],
        }
    }

    #[test]
    fn empty_message_has_no_events() {
        let m = Message::new();
        assert!(m.events.is_empty());
    }

    #[test]
    fn add_three_events() {
        let mut m = Message::new();
        m.add(event(1));
        m.add(event(2));
        m.add(event(3));

        assert_eq!(m.events.len(), 3);
    }

    #[test]
    fn duplicate_events_deduplicated() {
        let mut m = Message::new();
        m.add(event(1));
        m.add(event(1));

        assert_eq!(m.events.len(), 1);
    }

    #[test]
    fn from_events_constructor() {
        let m = Message::from_events(vec![
            event(1),
            Event {
                event_id: EventId(2),
                operation: Operation::Claim(TaskId(1), NodeId(10)),
                causal_parents: vec![EventId(1)],
            },
            Event {
                event_id: EventId(3),
                operation: Operation::Complete(TaskId(1), NodeId(10)),
                causal_parents: vec![EventId(2)],
            },
        ]);

        assert_eq!(m.events.len(), 3);
    }
}
