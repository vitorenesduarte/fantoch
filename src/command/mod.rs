// This module contains the definition of `Pending`.
pub mod pending;

// Re-exports.
pub use pending::Pending;

use crate::id::Rifl;
use crate::kvs::{KVOp, KVOpResult, Key, Value};
use std::collections::btree_map::{self, BTreeMap};
use std::collections::HashMap;
use std::iter::{self, FromIterator};

#[derive(Debug, Clone, PartialEq)]
pub struct Command {
    rifl: Rifl,
    ops: BTreeMap<Key, KVOp>,
}

impl Command {
    /// Create a new `Command`.
    pub fn new(rifl: Rifl, ops: BTreeMap<Key, KVOp>) -> Self {
        Self { rifl, ops }
    }

    /// Create a new `Command` from an iterator.
    pub fn from<I: IntoIterator<Item = (Key, KVOp)>>(rifl: Rifl, iter: I) -> Self {
        Self::new(rifl, BTreeMap::from_iter(iter))
    }

    /// Creates a get command.
    pub fn get(rifl: Rifl, key: Key) -> Self {
        Self::from(rifl, iter::once((key, KVOp::Get)))
    }

    /// Creates a multi-get command.
    pub fn multi_get(rifl: Rifl, keys: Vec<Key>) -> Self {
        let commands = keys.into_iter().map(|key| (key, KVOp::Get));
        Self::from(rifl, commands)
    }

    /// Creates a put command.
    pub fn put(rifl: Rifl, key: Key, value: Value) -> Self {
        Self::from(rifl, iter::once((key, KVOp::Put(value))))
    }

    /// Returns the command identifier.
    pub fn rifl(&self) -> Rifl {
        self.rifl
    }

    /// Returns references to list of keys modified by this command.
    pub fn keys(&self) -> impl Iterator<Item = &Key> {
        self.ops.iter().map(|(key, _)| key)
    }

    /// Returns the number of keys accessed by this command.
    pub fn key_count(&self) -> usize {
        self.ops.len()
    }
}

impl IntoIterator for Command {
    type Item = (Key, KVOp);
    type IntoIter = btree_map::IntoIter<Key, KVOp>;

    /// Returns a `Command` into-iterator ordered by `Key` (ASC).
    fn into_iter(self) -> Self::IntoIter {
        self.ops.into_iter()
    }
}

/// Structure that aggregates partial results of multi-key commands.
#[derive(Debug, Clone, PartialEq)]
pub struct CommandResult {
    rifl: Rifl,
    key_count: usize,
    results: HashMap<Key, KVOpResult>,
}

impl CommandResult {
    /// Creates a new `CommandResult` given the number of keys accessed by
    /// the command.
    pub fn new(rifl: Rifl, key_count: usize) -> Self {
        CommandResult {
            rifl,
            key_count,
            results: HashMap::with_capacity(key_count),
        }
    }

    /// Adds a partial command result to the overall result.
    /// Returns a boolean indicating whether the full result is ready.
    pub fn add_partial(&mut self, key: Key, result: KVOpResult) -> bool {
        // add op result for `key`
        let res = self.results.insert(key, result);

        // assert there was nothing about this `key` previously
        assert!(res.is_none());

        // we're ready if the number of partial results equals `key_count`
        self.results.len() == self.key_count
    }

    /// Returns the command identifier.
    pub fn rifl(&self) -> Rifl {
        self.rifl
    }

    /// Returns the commands results.
    pub fn results(&self) -> &HashMap<Key, KVOpResult> {
        &self.results
    }
}
