pub use crdt_core::Operation;

#[cfg(test)]
mod tests {
    use crdt_core::{NodeId, TaskId};

    use super::*;

    #[test]
    fn variants_are_not_equal() {
        let create = Operation::Create(TaskId(1));
        let claim = Operation::Claim(TaskId(1), NodeId(2));
        let complete = Operation::Complete(TaskId(1), NodeId(2));

        assert_ne!(create, claim);
        assert_ne!(claim, complete);
        assert_ne!(create, complete);
    }
}
