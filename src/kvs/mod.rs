// Definition of `Key` and `Value` types.
pub type Key = String;
pub type Value = String;

// This module contains the definition of `KVOp`, `KVOpResult`, and `KVStore`.
pub mod store;

// Re-exports.
pub use store::{KVOp, KVOpResult, KVStore};
