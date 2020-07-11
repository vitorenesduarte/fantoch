// This module contains the definition of `KeyClocks`.
mod keys;

// This module contains the definition of `QuorumClocks`.
mod quorum;

// Re-exports.
pub use keys::{
    AtomicKeyClocks, FineLockedKeyClocks, KeyClocks, LockedKeyClocks,
    SequentialKeyClocks,
};
pub use quorum::QuorumClocks;
