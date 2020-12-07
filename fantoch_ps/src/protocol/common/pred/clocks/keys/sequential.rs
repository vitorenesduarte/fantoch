use super::{KeyClocks, Timestamp};
use fantoch::command::Command;
use fantoch::id::{Dot, ShardId};
use fantoch::kvs::Key;
use fantoch::{HashMap, HashSet};
use std::collections::BTreeMap;

// timestamps are unique and thus it's enough to store one command `Dot` per
// timestamp.
type CommandsPerKey = BTreeMap<Timestamp, Dot>;

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
    /// timestamp. It also removes the prior instance of this command associated
    /// with its previous clock.
    fn predecessors(
        &mut self,
        dot: Dot,
        cmd: &Command,
        clock: Timestamp,
        previous_clock: Option<Timestamp>,
    ) -> HashSet<Dot> {
        // TODO is this data structure ever GCed? otherwise the set that we
        // return here will grow unbounded as the more commands are processed in
        // the system
        let mut predecessors = HashSet::new();
        cmd.keys(self.shard_id).for_each(|key| {
            // get a mutable reference to current commands
            let current = match self.clocks.get_mut(key) {
                Some(current) => current,
                None => {
                    self.clocks.entry(key.clone()).or_insert_with(BTreeMap::new)
                }
            };

            // collect all `Dot`'s with a timestamp smaller than `clock`:
            current.range(..clock).for_each(|(_, predecessor)| {
                // we don't assert that doesn't exist already because the same
                // `Dot` might be stored on different keys if we have multi-key
                // commands
                // TODO can we avoid cloning here?
                predecessors.insert(*predecessor);
            });

            // add ourselves to the set of commands and assert there was no
            // command with the same timestamp
            let res = current.insert(clock, dot);
            assert!(res.is_none());

            // remove ourselves from the set of commands, in case we've been
            // added before, and assert that we were indeed in the set
            if let Some(previous_clock) = &previous_clock {
                let res = current.remove(previous_clock);
                assert!(res.is_some());
            }
        });
        predecessors
    }

    fn remove(&mut self, cmd: &Command, clock: Timestamp) {
        cmd.keys(self.shard_id).for_each(|key| {
            // get a mutable reference to current commands
            let current = self.clocks.get_mut(key).expect(
                "can't remove a timestamp belonging to a command never added",
            );

            // remove ourselves from the set of commands and assert that we were
            // indeed in the set
            let res = current.remove(&clock);
            assert!(res.is_some());
        });
    }

    fn parallel() -> bool {
        false
    }
}
