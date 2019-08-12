use crate::store::{Key, Value};
use std::collections::btree_map::{self, BTreeMap};
use std::iter::FromIterator;

#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    Get,
    Put(Value),
}

#[derive(Debug, Clone, PartialEq)]
pub struct MultiCommand {
    commands: BTreeMap<Key, Command>,
}

impl MultiCommand {
    /// Create a new `MultiCommand`.
    pub fn new(commands: BTreeMap<Key, Command>) -> Self {
        MultiCommand { commands }
    }

    /// Create a new `MultiCommand` from an iterator.
    pub fn from<I: IntoIterator<Item = (Key, Command)>>(iter: I) -> Self {
        Self::new(BTreeMap::from_iter(iter))
    }

    /// Creates a multi-get command.
    pub fn get(keys: Vec<Key>) -> Self {
        let commands =
            keys.into_iter().map(|key| (key, Command::Get)).collect();
        Self::new(commands)
    }

    /// Returns references to list of keys modified by this command.
    pub fn keys(&self) -> Vec<&Key> {
        self.commands.iter().map(|(key, _)| key).collect()
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
