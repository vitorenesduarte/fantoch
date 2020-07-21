use crate::id::{Rifl, ShardId};
use crate::kvs::{KVOp, KVOpResult, KVStore, Key, Value};
use crate::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug};
use std::iter::{self, FromIterator};

const DEFAULT_SHARD_ID: ShardId = 0;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Command {
    rifl: Rifl,
    ops: HashMap<Key, (KVOp, ShardId)>,
    shards: HashSet<ShardId>,
}

impl Command {
    /// Create a new `Command`.
    pub fn new(rifl: Rifl, ops: HashMap<Key, (KVOp, ShardId)>) -> Self {
        let shards = ops.iter().map(|(_, (_, shard_id))| *shard_id).collect();
        Self { rifl, ops, shards }
    }

    /// Create a new `Command` from an iterator.
    pub fn from<I: IntoIterator<Item = (Key, KVOp)>>(
        rifl: Rifl,
        iter: I,
    ) -> Self {
        // store all keys in the default shard
        let iter = iter
            .into_iter()
            .map(|(key, op)| (key, (op, DEFAULT_SHARD_ID)));
        Self::new(rifl, HashMap::from_iter(iter))
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

    /// Creates a multi-put command.
    pub fn multi_put(rifl: Rifl, kvs: Vec<(Key, Value)>) -> Self {
        let commands =
            kvs.into_iter().map(|(key, value)| (key, KVOp::Put(value)));
        Self::from(rifl, commands)
    }

    /// Returns the command identifier.
    pub fn rifl(&self) -> Rifl {
        self.rifl
    }

    /// Returns references to list of keys modified by this command.
    pub fn keys(&self) -> impl Iterator<Item = &Key> {
        self.ops.keys()
    }

    /// Returns references to list of keys modified by this command.
    pub fn shards(&self) -> &HashSet<ShardId> {
        &self.shards
    }

    /// Checks if `key` is accessed by this command.
    #[allow(clippy::ptr_arg)]
    pub fn contains_key(&self, key: &Key) -> bool {
        self.ops.contains_key(key)
    }

    /// Checks if a command conflicts with another given command.
    pub fn conflicts(&self, other: &Command) -> bool {
        self.ops.iter().any(|(key, _)| other.ops.contains_key(key))
    }

    /// Returns the number of keys accessed by this command.
    pub fn key_count(&self) -> usize {
        self.ops.len()
    }

    /// Executes self in a `KVStore`, returning the resulting `CommandResult`.
    pub fn execute(
        self,
        shard_id: ShardId,
        store: &mut KVStore,
    ) -> CommandResult {
        let rifl = self.rifl;
        let key_count = self.ops.len();
        let results = self
            .into_iter(shard_id)
            .map(|(key, op)| {
                let partial_result = store.execute(&key, op);
                (key, partial_result)
            })
            .collect();
        CommandResult {
            rifl,
            key_count,
            results,
        }
    }

    // Creates an iterator without ops on keys that do not belong to `shard`.
    pub fn into_iter(
        self,
        shard_id: ShardId,
    ) -> impl Iterator<Item = (Key, KVOp)> {
        self.ops
            .into_iter()
            .filter_map(move |(key, (op, op_shard_id))| {
                if op_shard_id == shard_id {
                    Some((key, op))
                } else {
                    None
                }
            })
    }
}

impl fmt::Debug for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({:?} -> {:?})", self.rifl, self.ops.keys())
    }
}

/// Structure that aggregates partial results of multi-key commands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
            results: HashMap::new(),
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

    pub fn increment_key_count(&mut self) {
        self.key_count += 1;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conflicts() {
        let rifl = Rifl::new(1, 1);
        let cmd_a = Command::multi_get(rifl, vec![String::from("A")]);
        let cmd_b = Command::multi_get(rifl, vec![String::from("B")]);
        let cmd_c = Command::multi_get(rifl, vec![String::from("C")]);
        let cmd_ab = Command::multi_get(
            rifl,
            vec![String::from("A"), String::from("B")],
        );

        // check command a conflicts
        assert!(cmd_a.conflicts(&cmd_a));
        assert!(!cmd_a.conflicts(&cmd_b));
        assert!(!cmd_a.conflicts(&cmd_c));
        assert!(cmd_a.conflicts(&cmd_ab));

        // check command b conflicts
        assert!(!cmd_b.conflicts(&cmd_a));
        assert!(cmd_b.conflicts(&cmd_b));
        assert!(!cmd_b.conflicts(&cmd_c));
        assert!(cmd_b.conflicts(&cmd_ab));

        // check command c conflicts
        assert!(!cmd_c.conflicts(&cmd_a));
        assert!(!cmd_c.conflicts(&cmd_b));
        assert!(cmd_c.conflicts(&cmd_c));
        assert!(!cmd_c.conflicts(&cmd_ab));

        // check command ab conflicts
        assert!(cmd_ab.conflicts(&cmd_a));
        assert!(cmd_ab.conflicts(&cmd_b));
        assert!(!cmd_ab.conflicts(&cmd_c));
        assert!(cmd_ab.conflicts(&cmd_ab));
    }
}
