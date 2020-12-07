// This module contains the definition of `KeyClocks`.
mod keys;

// // // This module contains the definition of `QuorumClocks`.
// mod quorum;

// Re-exports.
pub use keys::{KeyClocks, SequentialKeyClocks};
// pub use quorum::QuorumClocks;

use fantoch::id::ProcessId;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp {
    seq: u64,
    source: ProcessId,
}

impl Timestamp {
    pub fn new(source: ProcessId) -> Self {
        Self { seq: 0, source }
    }

    pub fn next(&mut self) {
        self.seq += 1;
    }

    pub fn join(&mut self, other: &Self) {
        self.seq = std::cmp::max(self.seq, other.seq);
    }
}
