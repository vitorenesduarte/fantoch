// This module contains the definition of `KeysClocks`.
mod keys;

// This module contains the definition of `QuorumClocks`.
mod quorum;

// Re-export `KeysClocks` and `QuorumClocks`.
pub use keys::KeysClocks;
pub use quorum::QuorumClocks;
