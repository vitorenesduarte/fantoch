use crate::client::Rifl;
use crate::kvs::{Key, Value};
use std::collections::btree_map::{self, BTreeMap};
use std::collections::HashMap;
use std::iter::{self, FromIterator};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Command {
    Get,
    Put(Value),
    Delete,
}

pub type CommandResult = Option<Value>;

#[derive(Debug, Clone, PartialEq)]
pub struct MultiCommand {
    rifl: Rifl,
    commands: BTreeMap<Key, Command>,
}

impl MultiCommand {
    /// Create a new `MultiCommand`.
    pub fn new(rifl: Rifl, commands: BTreeMap<Key, Command>) -> Self {
        Self { rifl, commands }
    }

    /// Create a new `MultiCommand` from an iterator.
    pub fn from<I: IntoIterator<Item = (Key, Command)>>(
        rifl: Rifl,
        iter: I,
    ) -> Self {
        Self::new(rifl, BTreeMap::from_iter(iter))
    }

    /// Creates a get command.
    pub fn get(rifl: Rifl, key: Key) -> Self {
        Self::from(rifl, iter::once((key, Command::Get)))
    }

    /// Creates a multi-get command.
    pub fn multi_get(rifl: Rifl, keys: Vec<Key>) -> Self {
        let commands = keys.into_iter().map(|key| (key, Command::Get));
        Self::from(rifl, commands)
    }

    /// Returns the command identifier.
    pub fn rifl(&self) -> Rifl {
        self.rifl
    }

    /// Returns references to list of keys modified by this command.
    pub fn keys(&self) -> Vec<&Key> {
        self.commands.iter().map(|(key, _)| key).collect()
    }

    /// Returns the number of keys accessed by this command.
    pub fn key_count(&self) -> usize {
        self.commands.len()
    }
}

impl IntoIterator for MultiCommand {
    type Item = (Key, Command);
    type IntoIter = btree_map::IntoIter<Key, Command>;

    /// Returns a `MultiCommand` into-iterator ordered by `Key` (ASC).
    fn into_iter(self) -> Self::IntoIter {
        self.commands.into_iter()
    }
}

/// Structure that aggregates partial results of multi-key commands.
#[derive(Debug, Clone, PartialEq)]
pub struct MultiCommandResult {
    rifl: Rifl,
    key_count: usize,
    results: HashMap<Key, CommandResult>,
}

impl MultiCommandResult {
    /// Creates a new `MultiCommandResult` given the number of keys accessed by
    /// the command.
    pub fn new(rifl: Rifl, key_count: usize) -> Self {
        Self {
            rifl,
            key_count,
            results: HashMap::new(),
        }
    }

    /// Adds a partial command result to the overall result.
    /// Returns a boolean indicating whether the full result is ready.
    pub fn add_partial(&mut self, key: Key, result: CommandResult) -> bool {
        let res = self.results.insert(key, result);
        // assert there was nothing about this key previously
        assert!(res.is_none());

        // we're ready if the number of partial results equals `key_count`
        self.results.len() == self.key_count
    }

    /// Returns the command identifier (RIFL) of this comand.
    pub fn rifl(&self) -> Rifl {
        self.rifl
    }

    /// Returns the commands results.
    pub fn results(&self) -> &HashMap<Key, CommandResult> {
        &self.results
    }
}
