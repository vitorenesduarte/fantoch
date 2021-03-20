use crate::executor::ExecutorResult;
use crate::id::{Rifl, ShardId};
use crate::kvs::{KVOp, KVOpResult, KVStore, Key};
use crate::HashMap;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug};
use std::iter::FromIterator;
use std::sync::Arc;

pub const DEFAULT_SHARD_ID: ShardId = 0;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Command {
    rifl: Rifl,
    shard_to_ops: HashMap<ShardId, HashMap<Key, Arc<Vec<KVOp>>>>,
    // one of the keys accessed by this command
    elected_key: Key,
    elected_key_shard: ShardId,
    // field used to output and empty iterator of keys when rustc can't figure
    // out what we mean
    _empty_keys: HashMap<Key, Arc<Vec<KVOp>>>,
}

impl Command {
    /// Create a new `Command`.
    pub fn new(
        rifl: Rifl,
        shard_to_ops: HashMap<ShardId, HashMap<Key, Vec<KVOp>>>,
    ) -> Self {
        let mut elected_key_and_shard = None;
        let shard_to_ops = shard_to_ops
            .into_iter()
            .map(|(shard_id, shard_ops)| {
                (
                    shard_id,
                    shard_ops
                        .into_iter()
                        .map(|(key, ops)| {
                            // set elected key in case it's not set
                            if elected_key_and_shard.is_none() {
                                elected_key_and_shard =
                                    Some((key.clone(), shard_id));
                            }
                            (key, Arc::new(ops))
                        })
                        .collect(),
                )
            })
            .collect();
        let (elected_key, elected_key_shard) = elected_key_and_shard
            .expect("command must access at least one key");
        Self {
            rifl,
            shard_to_ops,
            elected_key,
            elected_key_shard,
            _empty_keys: HashMap::new(),
        }
    }

    // Create a new `Command` from an iterator.
    pub fn from<I: IntoIterator<Item = (Key, KVOp)>>(
        rifl: Rifl,
        iter: I,
    ) -> Self {
        // store all keys in the default shard
        let inner = HashMap::from_iter(
            iter.into_iter().map(|(key, op)| (key, vec![op])),
        );
        let shard_to_ops =
            HashMap::from_iter(std::iter::once((DEFAULT_SHARD_ID, inner)));
        Self::new(rifl, shard_to_ops)
    }

    /// Checks if the command is read-only.
    pub fn read_only(&self) -> bool {
        // a command is read-only if all ops are `Get`s
        self.shard_to_ops.values().all(|shard_ops| {
            shard_ops
                .values()
                .all(|ops| ops.iter().all(|op| op == &KVOp::Get))
        })
    }

    /// Checks if the command is replicated by `shard_id`.
    pub fn replicated_by(&self, shard_id: &ShardId) -> bool {
        self.shard_to_ops.contains_key(&shard_id)
    }

    /// Returns the command identifier.
    pub fn rifl(&self) -> Rifl {
        self.rifl
    }

    /// Returns the number of keys accessed by this command on the shard
    /// provided.
    pub fn key_count(&self, shard_id: ShardId) -> usize {
        self.shard_to_ops
            .get(&shard_id)
            .map(|shard_ops| shard_ops.len())
            .unwrap_or(0)
    }

    /// Returns the total number of keys accessed by this command.
    pub fn total_key_count(&self) -> usize {
        self.shard_to_ops.values().map(|ops| ops.len()).sum()
    }

    /// Returns references to the keys accessed by this command on the shard
    /// provided.
    pub fn keys(&self, shard_id: ShardId) -> impl Iterator<Item = &Key> {
        self.shard_to_ops
            .get(&shard_id)
            .map(|shard_ops| shard_ops.keys())
            .unwrap_or_else(|| self._empty_keys.keys())
    }

    /// Returns the elected key.
    pub fn elected_key(&self) -> &Key {
        &self.elected_key
    }

    /// Returns the shard of the elected key.
    pub fn elected_key_shard(&self) -> ShardId {
        self.elected_key_shard
    }

    /// Returns references to all the keys accessed by this command.
    pub fn all_keys(&self) -> impl Iterator<Item = (&ShardId, &Key)> {
        self.shard_to_ops.iter().flat_map(|(shard_id, shard_ops)| {
            shard_ops.keys().map(move |key| (shard_id, key))
        })
    }

    pub fn all_keys_but_elected_key(
        &self,
    ) -> impl Iterator<Item = (ShardId, Key)> + '_ {
        self.shard_to_ops
            .iter()
            .flat_map(|(shard_id, shard_ops)| {
                shard_ops.keys().map(move |key| (shard_id, key))
            })
            .filter(move |(_, key)| key != &&self.elected_key)
            .map(|(shard_id, key)| (*shard_id, key.clone()))
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
    pub fn execute<'a>(
        self,
        shard_id: ShardId,
        store: &'a mut KVStore,
    ) -> impl Iterator<Item = ExecutorResult> + 'a {
        let rifl = self.rifl;
        self.into_iter(shard_id).map(move |(key, ops)| {
            // take the ops inside the arc if we're the last with a
            // reference to it (otherwise, clone them)
            let ops =
                Arc::try_unwrap(ops).unwrap_or_else(|ops| ops.as_ref().clone());
            // execute this op
            let partial_results = store.execute(&key, ops, rifl);
            ExecutorResult::new(rifl, key, partial_results)
        })
    }

    // Creates an iterator with ops on keys that belong to `shard_id`.
    pub fn iter(
        &self,
        shard_id: ShardId,
    ) -> impl Iterator<Item = (&Key, &Arc<Vec<KVOp>>)> {
        self.shard_to_ops
            .get(&shard_id)
            .map(|shard_ops| shard_ops.iter())
            .unwrap_or_else(|| self._empty_keys.iter())
    }

    // Creates an iterator with ops on keys that belong to `shard_id`.
    pub fn into_iter(
        mut self,
        shard_id: ShardId,
    ) -> impl Iterator<Item = (Key, Arc<Vec<KVOp>>)> {
        self.shard_to_ops
            .remove(&shard_id)
            .map(|shard_ops| shard_ops.into_iter())
            .unwrap_or_else(|| self._empty_keys.into_iter())
    }

    /// Checks if a command conflicts with another given command.
    pub fn conflicts(&self, other: &Command) -> bool {
        self.shard_to_ops.iter().any(|(shard_id, shard_ops)| {
            shard_ops
                .iter()
                .any(|(key, _)| other.contains_key(*shard_id, key))
        })
    }

    /// Checks if `key` is accessed by this command.
    fn contains_key(&self, shard_id: ShardId, key: &Key) -> bool {
        self.shard_to_ops
            .get(&shard_id)
            .map(|shard_ops| shard_ops.contains_key(key))
            .unwrap_or(false)
    }

    /// Adds the operations in the `other` command to this command.
    pub fn merge(&mut self, other: Command) {
        for (shard_id, shard_ops) in other.shard_to_ops {
            let current_shard_ops =
                self.shard_to_ops.entry(shard_id).or_default();
            for (key, ops) in shard_ops {
                let ops = Arc::try_unwrap(ops).expect("a command to be merged into another command should have not been cloned");
                let current_ops = current_shard_ops.entry(key).or_default();
                Arc::get_mut(current_ops).expect("a command should only be cloned after all merges have occurred").extend(ops);
            }
        }
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
pub struct CommandResultBuilder {
    rifl: Rifl,
    key_count: usize,
    results: HashMap<Key, Vec<KVOpResult>>,
}

impl CommandResultBuilder {
    /// Creates a new `CommandResultBuilder` given the number of keys accessed
    /// by the command.
    pub fn new(rifl: Rifl, key_count: usize) -> Self {
        CommandResultBuilder {
            rifl,
            key_count,
            results: HashMap::new(),
        }
    }

    /// Adds a partial command result to the overall result.
    /// Returns a boolean indicating whether the full result is ready.
    pub fn add_partial(&mut self, key: Key, partial_results: Vec<KVOpResult>) {
        // add op result for `key`
        let res = self.results.insert(key, partial_results);

        // assert there was nothing about this `key` previously
        assert!(res.is_none());
    }

    pub fn ready(&self) -> bool {
        // we're ready if the number of partial results equals `key_count`
        self.results.len() == self.key_count
    }
}

/// Structure that aggregates partial results of multi-key commands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandResult {
    rifl: Rifl,
    results: HashMap<Key, Vec<KVOpResult>>,
}

impl CommandResult {
    /// Creates a new `CommandResult`.
    pub fn new(rifl: Rifl, results: HashMap<Key, Vec<KVOpResult>>) -> Self {
        CommandResult { rifl, results }
    }

    /// Returns the command identifier.
    pub fn rifl(&self) -> Rifl {
        self.rifl
    }

    /// Returns the commands results.
    pub fn results(&self) -> &HashMap<Key, Vec<KVOpResult>> {
        &self.results
    }
}

impl From<CommandResultBuilder> for CommandResult {
    fn from(cmd_result_builder: CommandResultBuilder) -> Self {
        assert!(cmd_result_builder.ready());
        Self {
            rifl: cmd_result_builder.rifl,
            results: cmd_result_builder.results,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn multi_put(rifl: Rifl, keys: Vec<String>) -> Command {
        Command::from(
            rifl,
            keys.into_iter().map(|key| (key.clone(), KVOp::Put(key))),
        )
    }

    #[test]
    fn conflicts() {
        let rifl = Rifl::new(1, 1);
        let cmd_a = multi_put(rifl, vec![String::from("A")]);
        let cmd_b = multi_put(rifl, vec![String::from("B")]);
        let cmd_c = multi_put(rifl, vec![String::from("C")]);
        let cmd_ab =
            multi_put(rifl, vec![String::from("A"), String::from("B")]);

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
