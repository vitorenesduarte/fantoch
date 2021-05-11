#![deny(rust_2018_idioms)]

// This module contains the definition of `Region` and `Planet`.
pub mod planet;

// This module contains the definition of all identifiers and generators of
// identifiers.
pub mod id;

// This module contains the definition of `Client`.
pub mod client;

// This module contains the implementation of a key-value store.
pub mod kvs;

// This module contains the definitions of `Metrics`, `Float` and `Histogram`.
pub mod metrics;

// This module contains the definition of an executor.
pub mod executor;

// This module contains the definition of `Command`, `CommandResult` and
// `Pending`.
pub mod command;

// This module contains the definition of `Config`.
pub mod config;

// This module contains the definition of `ToSend`, `Process` and `BaseProcess`
// and implementations of all protocols supported.
pub mod protocol;

// This module contains the definition of `SharedMap`.
pub mod shared;

// This module contains the definition of trait `SysTime` and its
// implementations.
pub mod time;

// This module contains the definition of `Simulation` and `Runner`.
pub mod sim;

// This module contains the definition of Runner` (that actually runs a given
// `Process`)
#[cfg(feature = "run")]
pub mod run;

pub mod load_balance {
    use crate::id::Dot;

    // the worker index that should be used by leader-based protocols
    pub const LEADER_WORKER_INDEX: usize = 0;

    // the worker index that should be for garbage collection:
    // - it's okay to be the same as the leader index because this value is not
    //   used by leader-based protocols
    // - e.g. in fpaxos, the gc only runs in the acceptor worker
    pub const GC_WORKER_INDEX: usize = 0;

    pub const WORKERS_INDEXES_RESERVED: usize = 2;

    pub fn worker_index_no_shift(index: usize) -> Option<(usize, usize)> {
        // when there's no shift, the index must be either 0 or 1
        assert!(index < WORKERS_INDEXES_RESERVED);
        Some((0, index))
    }

    // note: reserved indexing always reserve the first two workers
    pub const fn worker_index_shift(index: usize) -> Option<(usize, usize)> {
        Some((WORKERS_INDEXES_RESERVED, index))
    }

    pub fn worker_dot_index_shift(dot: &Dot) -> Option<(usize, usize)> {
        worker_index_shift(dot.sequence() as usize)
    }
}

// This module contains some utilitary functions.
pub mod util;

// Re-export `HashMap` and `HashSet`.
pub use hash_map::HashMap;
pub use hash_set::HashSet;

pub mod hash_map {
    pub use hashbrown::hash_map::*;
}

pub mod hash_set {
    pub use hashbrown::hash_set::*;
}
