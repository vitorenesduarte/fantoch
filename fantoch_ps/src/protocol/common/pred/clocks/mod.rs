// This module contains the definition of `KeyClocks`.
mod keys;

// This module contains the definition of `QuorumClocks` and `QuorumRetries`.
mod quorum;

// Re-exports.
pub use keys::{KeyClocks, SequentialKeyClocks};
pub use quorum::{QuorumClocks, QuorumRetries};

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

    pub fn is_zero(&self) -> bool {
        self.seq == 0
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

    #[test]
    fn join() {
        let p1 = 1;
        let p2 = 2;
        let p3 = 3;
        let p4 = 4;
        let mut clock = Clock::new(p1);

        // if we join with something with a higher timestamp, then we take their
        // value
        clock.join(&Clock::from(2, p2));
        assert_eq!(clock, Clock::from(2, p2));

        // if we join with something with the same timestamp, then we take the
        // max of the identifiers
        clock.join(&Clock::from(2, p3));
        assert_eq!(clock, Clock::from(2, p3));

        clock.join(&Clock::from(4, p3));
        assert_eq!(clock, Clock::from(4, p3));
        clock.join(&Clock::from(4, p2));
        assert_eq!(clock, Clock::from(4, p3));

        // if we join with something with a lower timestamp, then nothing
        // happens
        clock.join(&Clock::from(1, p4));
        clock.join(&Clock::from(2, p4));
        clock.join(&Clock::from(3, p4));
    }
}
