// This module contains the definition of `Shared`.
mod shared;

// This module contains definitions common to dependency-graph-based protocols.
pub mod graph;

// This module contains definitions common to votes-table-based protocols.
pub mod table;

// This module contains definitions common to predecessors-based protocols.
pub mod pred;

// This module contains the implementation of Paxos single and multi-decree
// Synod Protocols.
pub mod synod;
