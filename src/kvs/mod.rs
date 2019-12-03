// Definition of `Key` and `Value` types.
pub type Key = String;
pub type Value = String;

// This module contains the definition of `KVStore`.
pub mod store;

// This module contains the definition of `Command`, `MultiCommand`,
// `CommandResult` and `MultiCommandResult`.
pub mod command;

// This module contains the definition of `Pending`.
pub mod pending;

// Re-exports.
pub use pending::Pending;
pub use store::KVStore;
