use crate::executor::ExecutorResult;
use crate::id::{Rifl, ShardId};
use crate::kvs::{KVOp, KVOpResult, KVStore, Key, Value};
use crate::HashMap;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug};
use std::iter::{self, FromIterator};

const DEFAULT_SHARD_ID: ShardId = 0;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Command {
    rifl: Rifl,
    shard_to_ops: HashMap<ShardId, HashMap<Key, KVOp>>,
    // field used to output and empty iterator of keys when rustc can't figure
    // out what we mean
    _empty_keys: HashMap<Key, KVOp>,
}

impl Command {
    /// Create a new `Command`.
    pub fn new(
        rifl: Rifl,
        shard_to_ops: HashMap<ShardId, HashMap<Key, KVOp>>,
    ) -> Self {
        Self {
            rifl,
            shard_to_ops,
            _empty_keys: HashMap::new(),
        }
    }

    /// Create a new `Command` from an iterator.
    pub fn from<I: IntoIterator<Item = (Key, KVOp)>>(
        rifl: Rifl,
        iter: I,
    ) -> Self {
        // store all keys in the default shard
        let inner = HashMap::from_iter(iter);
        let shard_to_ops =
            HashMap::from_iter(std::iter::once((DEFAULT_SHARD_ID, inner)));
        Self::new(rifl, shard_to_ops)
    }

    /// Checks if the command is replicated by `shard_id`.
    pub fn replicated_by(&self, shard_id: &ShardId) -> bool {
        self.shard_to_ops.contains_key(&shard_id)
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

    /// Checks if `key` is accessed by this command.
    #[allow(clippy::ptr_arg)]
    pub fn contains_key(&self, shard_id: ShardId, key: &Key) -> bool {
        self.shard_to_ops
            .get(&shard_id)
            .map(|shard_ops| shard_ops.contains_key(key))
            .unwrap_or(false)
    }

    /// Checks if a command conflicts with another given command.
    pub fn conflicts(&self, other: &Command) -> bool {
        self.shard_to_ops.iter().any(|(shard_id, shard_ops)| {
            shard_ops
                .iter()
                .any(|(key, _)| other.contains_key(*shard_id, key))
        })
    }

    /// Returns the number of keys accessed by this command on the shard
    /// provided.
    pub fn key_count(&self, shard_id: ShardId) -> usize {
        self.shard_to_ops
            .get(&shard_id)
            .map(|shard_ops| shard_ops.len())
            .unwrap_or(0)
    }

    /// Returns references to the keys modified by this command on the shard
    /// provided.
    pub fn keys(&self, shard_id: ShardId) -> impl Iterator<Item = &Key> {
        self.shard_to_ops
            .get(&shard_id)
            .map(|shard_ops| shard_ops.keys())
            .unwrap_or_else(|| self._empty_keys.keys())
    }

    /// Returns the number of shards accessed by this command.
    pub fn shard_count(&self) -> usize {
        self.shard_to_ops.len()
    }

    /// Returns the shards accessed by this command.
    pub fn shards(&self) -> impl Iterator<Item = &ShardId> {
        self.shard_to_ops.keys()
    }

    /// Executes self in a `KVStore`, returning the resulting an iterator of
    /// `ExecutorResult`.
    pub fn execute(
        self,
        shard_id: ShardId,
        store: &mut KVStore,
    ) -> impl Iterator<Item = ExecutorResult> + '_ {
        let rifl = self.rifl;
        self.into_iter(shard_id).map(move |(key, op)| {
            let partial_result = store.execute(&key, op);
            ExecutorResult::new(rifl, key, partial_result)
        })
    }

    // Creates an iterator without ops on keys that do not belong to `shard`.
    pub fn into_iter(
        mut self,
        shard_id: ShardId,
    ) -> impl Iterator<Item = (Key, KVOp)> {
        self.shard_to_ops
            .remove(&shard_id)
            .map(|shard_ops| shard_ops.into_iter())
            .unwrap_or_else(|| self._empty_keys.into_iter())
    }
}

impl fmt::Debug for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let keys: std::collections::BTreeSet<_> = self
            .shard_to_ops
            .iter()
            .flat_map(|(shard_id, ops)| {
                ops.keys().map(move |key| (shard_id, key))
            })
            .collect();
        write!(f, "({:?} -> {:?})", self.rifl, keys)
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
