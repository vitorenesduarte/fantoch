// This module contains the definition of `KeyClocks` and `QuorumClocks`.
mod clocks;

// Re-exports.
pub use clocks::{
    Clock, KeyClocks, QuorumClocks, QuorumRetries, SequentialKeyClocks,
};
