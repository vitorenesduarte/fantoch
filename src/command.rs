use crate::base::Rifl;
use crate::store::{Key, Value};
use std::collections::btree_map::{self, BTreeMap};
use std::collections::HashMap;
use std::iter::FromIterator;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Command {
    Get,
    Put(Value),
    Delete,
}

pub type CommandResult = Option<Value>;

#[derive(Debug, Clone, PartialEq)]
pub struct MultiCommand {
    id: Rifl,
    commands: BTreeMap<Key, Command>,
}

impl MultiCommand {
    /// Create a new `MultiCommand`.
    pub fn new(id: Rifl, commands: BTreeMap<Key, Command>) -> Self {
        MultiCommand { id, commands }
    }

    /// Create a new `MultiCommand` from an iterator.
    pub fn from<I: IntoIterator<Item = (Key, Command)>>(
        id: Rifl,
        iter: I,
    ) -> Self {
        Self::new(id, BTreeMap::from_iter(iter))
    }

    /// Creates a multi-get command.
    pub fn get(id: Rifl, keys: Vec<Key>) -> Self {
        let commands = keys.into_iter().map(|key| (key, Command::Get));
        Self::from(id, commands)
    }

    /// Returns the command identifier.
    pub fn id(&self) -> Rifl {
        self.id
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
    id: Rifl,
    key_count: usize,
    results: HashMap<Key, CommandResult>,
}

impl MultiCommandResult {
    /// Creates a new `MultiCommandResult` given the number of keys accessed by
    /// the command.
    pub fn new(id: Rifl, key_count: usize) -> Self {
        MultiCommandResult {
            id,
            key_count,
            results: HashMap::new(),
        }
    }

    /// Adds a partial command result to the overall result.
    /// Returns a boolean indicating whether the full result is ready.
    fn add(&mut self, key: Key, result: CommandResult) -> bool {
        let res = self.results.insert(key, result);
        // assert there was nothing about this key previously
        assert!(res.is_none());

        // we're ready if the number of partial results equals `key_count`
        self.results.len() == self.key_count
    }

    /// Returns the command identifier (RIFL) of this comand.
    pub fn id(&self) -> Rifl {
        self.id
    }
}

/// Structure that tracks the progress of pending commands.
#[derive(Default)]
pub struct Pending {
    pending: HashMap<Rifl, MultiCommandResult>,
}

impl Pending {
    /// Creates a new `Pending` instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Starts tracking a command submitted by some client.
    pub fn start(&mut self, cmd: &MultiCommand) {
        // create `MultiCommandResult`
        let cmd_result = MultiCommandResult::new(cmd.id(), cmd.key_count());

        // add it to pending
        self.pending.insert(cmd.id(), cmd_result);
    }

    /// Registers a new partial command result.
    pub fn add(
        &mut self,
        id: Rifl,
        key: Key,
        result: CommandResult,
    ) -> Option<MultiCommandResult> {
        // get current result:
        // - if it's not part of pending, then ignore it
        let cmd_result = self.pending.get_mut(&id)?;

        // add partial result:
        // - if it's complete, remove it from pending and return it
        if cmd_result.add(key, result) {
            self.pending.remove(&id)
        } else {
            None
        }
    }
}
