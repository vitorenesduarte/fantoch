// This module contains the definition of `KeysClocks` and `QuorumClocks`.
mod clocks;

// This module contains the implementation of a dependency graph.
mod graph;

// Re-exports.
pub use clocks::{KeysClocks, QuorumClocks};
pub use graph::DependencyGraph;
