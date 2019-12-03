// Definition of `Key` and `Value` types.
pub type Key = String;
pub type Value = String;

// This module contains the definition of `Key`, `Value` and `KVStore`.
pub mod store;

// This module contains the definition of `Command`, `MultiCommand`,
// `CommandResult` and `MultiCommandResult`.
pub mod command;

// This module contains the definition of `Pending`.
pub mod pending;
