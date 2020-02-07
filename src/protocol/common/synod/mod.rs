// This module contains the implementation of Paxos single-decree Synod
// Protocols.
mod single;

// This module contains the implementation of Paxos multi-decree Synod
// Protocols.
mod multi;

// Re-exports.
pub use multi::{MultiSynodMessage, MultiSynod};
pub use single::{Synod, SynodMessage};
