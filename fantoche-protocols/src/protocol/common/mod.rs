// This module contains the definition of `SharedClocks`.
mod shared_clocks;

// This module contains definitions common to dependency-graph-based protocols.
pub mod graph;

// This module contains definitions common to votes-table-based protocols.
pub mod table;

// This module contains the implementation of Paxos single and multi-decree
// Synod Protocols.
pub mod synod;
