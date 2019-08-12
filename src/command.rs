use std::collections::btree_map::{self, BTreeMap};

pub type Key = String;
pub type Value = String;

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

    /// Creates an IntoIter.
    pub fn into_iter(self) -> btree_map::IntoIter<Key, Command> {
        self.commands.into_iter()
    }
}
