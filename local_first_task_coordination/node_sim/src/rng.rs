//! A minimal deterministic PRNG, used only to reorder message delivery for
//! robustness testing (see [`crate::simulation::Simulation::deliver_all_shuffled`]).
//!
//! The project is intentionally zero-dependency (see the top-level README),
//! so this is a small self-contained xorshift generator rather than a crate
//! like `rand`. It only needs to be deterministic (reproducible from a seed)
//! and reasonably well-distributed — nothing here is security-sensitive.

/// Deterministic xorshift64 PRNG.
pub struct Xorshift64 {
    state: u64,
}

impl Xorshift64 {
    /// Creates a generator seeded with `seed`. xorshift is undefined at
    /// state `0`, so a `0` seed is nudged to a fixed nonzero value.
    pub fn new(seed: u64) -> Self {
        Xorshift64 {
            state: if seed == 0 {
                0x9E37_79B9_7F4A_7C15
            } else {
                seed
            },
        }
    }

    /// Returns the next pseudo-random `u64` and advances the generator.
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    /// Returns a value in `0..n`. Returns `0` if `n == 0` rather than
    /// panicking, so callers don't need to special-case empty ranges.
    pub fn next_range(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next_u64() % n }
    }

    /// Shuffles `items` in place using a Fisher-Yates shuffle.
    pub fn shuffle<T>(&mut self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            let j = self.next_range(i as u64 + 1) as usize;
            items.swap(i, j);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_produces_same_sequence() {
        let mut a = Xorshift64::new(42);
        let mut b = Xorshift64::new(42);

        let seq_a: Vec<u64> = (0..10).map(|_| a.next_u64()).collect();
        let seq_b: Vec<u64> = (0..10).map(|_| b.next_u64()).collect();

        assert_eq!(seq_a, seq_b);
    }

    #[test]
    fn different_seeds_diverge() {
        let mut a = Xorshift64::new(1);
        let mut b = Xorshift64::new(2);

        assert_ne!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn zero_seed_does_not_panic_or_stall() {
        let mut rng = Xorshift64::new(0);
        assert_ne!(rng.next_u64(), 0);
    }

    #[test]
    fn next_range_stays_in_bounds() {
        let mut rng = Xorshift64::new(7);
        for _ in 0..200 {
            assert!(rng.next_range(5) < 5);
        }
    }

    #[test]
    fn next_range_of_zero_returns_zero() {
        let mut rng = Xorshift64::new(7);
        assert_eq!(rng.next_range(0), 0);
    }

    #[test]
    fn shuffle_preserves_multiset() {
        let mut rng = Xorshift64::new(99);
        let mut items = vec![1, 2, 3, 4, 5];
        rng.shuffle(&mut items);

        let mut sorted = items.clone();
        sorted.sort();
        assert_eq!(sorted, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn shuffle_of_empty_or_singleton_is_a_no_op() {
        let mut rng = Xorshift64::new(1);
        let mut empty: Vec<i32> = vec![];
        rng.shuffle(&mut empty);
        assert!(empty.is_empty());

        let mut one = vec![42];
        rng.shuffle(&mut one);
        assert_eq!(one, vec![42]);
    }
}
