// This module contains the implementation of Paxos single-decree Synod
// Protocols.
mod single;

// This module contains the implementation of Paxos multi-decree Synod
// Protocols.
mod multi;

// This module contains common functionality from tracking when it's safe to
// garbage-collect a command, i.e., when it's been committed at all processes.
mod gc;

// Re-exports.
pub use gc::GCTrack;
pub use multi::{MultiSynod, MultiSynodMessage};
pub use single::{Synod, SynodMessage};
