// This module contains the definition of `KeyClocks`.
mod keys;

// This module contains the definition of `QuorumClocks`.
mod quorum;

// Re-exports.
pub use keys::{KeyClocks, SequentialKeyClocks};
pub use quorum::QuorumClocks;

use fantoch::id::ProcessId;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
)]
pub struct Clock {
    seq: u64,
    process_id: ProcessId,
}

impl Clock {
    pub fn new(process_id: ProcessId) -> Self {
        Self { seq: 0, process_id }
    }

    pub fn from(seq: u64, process_id: ProcessId) -> Self {
        Self { seq, process_id }
    }

    // Lexicographic join.
    pub fn join(&mut self, other: &Self) {
        match self.seq.cmp(&other.seq) {
            Ordering::Greater => {
                // nothing to do
            }
            Ordering::Less => {
                // take the `other` value
                *self = *other;
            }
            Ordering::Equal => {
                // update the second component
                self.process_id =
                    std::cmp::max(self.process_id, other.process_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // check lexicographic ordering
    fn ord() {
        assert!((Clock::from(10, 1) == Clock::from(10, 1)));
        assert!(!(Clock::from(10, 1) < Clock::from(10, 1)));
        assert!(!(Clock::from(10, 1) > Clock::from(10, 1)));

        assert!(!(Clock::from(10, 1) == Clock::from(10, 2)));
        assert!((Clock::from(10, 1) < Clock::from(10, 2)));
        assert!(!(Clock::from(10, 1) > Clock::from(10, 2)));

        assert!(!(Clock::from(9, 1) == Clock::from(10, 2)));
        assert!((Clock::from(9, 1) < Clock::from(10, 2)));
        assert!(!(Clock::from(9, 1) > Clock::from(10, 2)));

        assert!(!(Clock::from(10, 1) == Clock::from(9, 2)));
        assert!(!(Clock::from(10, 1) < Clock::from(9, 2)));
        assert!((Clock::from(10, 1) > Clock::from(9, 2)));
    }
}
