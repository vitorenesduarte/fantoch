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

// This module contains the definition of trait `SysTime` and its
// implementations.
pub mod time;

// This module contains the definition of `F64`, `Stats` and `Metrics`.
pub mod metrics;

// This module contains the definition of `Simulation` and `Runner`.
pub mod sim;

// This module contains the definition of Runner` (that actually runs a given
// `Process`)
pub mod run;

// This module contains back-of-the-envelop calculations.
pub mod bote;

// This module contains some utilitary functions.
pub mod util;
