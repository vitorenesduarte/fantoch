use super::KeyClocks;
use fantoch::command::Command;
use fantoch::id::{Dot, ShardId};
use fantoch::kvs::Key;
use fantoch::{HashMap, HashSet};
use std::collections::BTreeMap;

// timestamps are unique and thus it's enough to store one command `Dot` per
// timestamp.
type CommandsPerKey = BTreeMap<u64, Dot>;

#[derive(Clone)]
pub struct SequentialKeyClocks {
    shard_id: ShardId,
    clocks: HashMap<Key, CommandsPerKey>,
}

impl KeyClocks for SequentialKeyClocks {
    /// Create a new `KeyClocks` instance.
    fn new(shard_id: ShardId) -> Self {
        let clocks = HashMap::new();
        Self { shard_id, clocks }
    }

    /// Computes this command's set of predecessors. From this moment on, this
    /// command will be reported as a predecessor of commands with a higher
    /// timestamp.
    fn predecessors(
        &mut self,
        dot: Dot,
        cmd: &Command,
        clock: u64,
    ) -> HashSet<Dot> {
        // TODO is this data structure ever GCed? otherwise the set that we
        // return here will grow unbounded as the more commands are processed in
        // the system
        let mut pred = HashSet::new();
        cmd.keys(self.shard_id).for_each(|key| {
            // get a mutable reference to current commands
            let current = match self.clocks.get_mut(key) {
                Some(current) => current,
                None => {
                    self.clocks.entry(key.clone()).or_insert_with(BTreeMap::new)
                }
            };

            // collect all `Dot`'s with a timestamp smaller than `clock`:
            current.range(..clock).for_each(|(_, dot)| {
                // we don't assert that doesn't exist already because the same
                // `Dot` might be stored on different keys if we have multi-key
                // commands
                // TODO can we avoid cloning here?
                pred.insert(*dot);
            });

            // add ourselves to the set of commands and assert there was no
            // command with the same timestamp
            let res = current.insert(clock, dot);
            assert!(res.is_none());
        });
        pred
    }

    fn parallel() -> bool {
        false
    }
}
