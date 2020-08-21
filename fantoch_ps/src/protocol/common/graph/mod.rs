// This module contains the definition of `KeyDeps` and `QuorumDeps`.
mod deps;

// Re-exports.
pub use deps::{
    Dependency, KeyDeps, LockedKeyDeps, QuorumDeps, SequentialKeyDeps,
};
