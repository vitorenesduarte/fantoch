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
pub mod run;

// This module contains some utilitary functions.
pub mod util;

// Re-export `HashMap` and `HashSet`.
pub use hash_map::HashMap;
pub use hash_set::HashSet;

pub mod hash_map {
    #[cfg(feature = "amortize")]
    pub use griddle::hash_map::*;

    #[cfg(not(feature = "amortize"))]
    pub use hashbrown::hash_map::*;
}

pub mod hash_set {
    #[cfg(feature = "amortize")]
    pub use griddle::hash_set::*;

    #[cfg(not(feature = "amortize"))]
    pub use hashbrown::hash_set::*;
}
