// This module contains definitions common to dependency-based protocols.
pub mod dependency;

// This module contains the implementation of Paxos Synod Protocol.
pub mod synod;

// Re-exports.
pub use synod::{Synod, SynodMessage};
