use super::{Clock, KeyClocks};
use fantoch::command::Command;
use fantoch::id::{Dot, ProcessId, ShardId};
use fantoch::kvs::Key;
use fantoch::{HashMap, HashSet};
use std::cmp::Ordering;
use std::collections::BTreeMap;

// timestamps are unique and thus it's enough to store one command `Dot` per
// timestamp.
// Note: this `Clock` should correspond to the `clock` stored in Caesar process.
type CommandsPerKey = BTreeMap<Clock, Dot>;

#[derive(Debug, Clone)]
pub struct SequentialKeyClocks {
    process_id: ProcessId,
    shard_id: ShardId,
    seq: u64,
    clocks: HashMap<Key, CommandsPerKey>,
}

impl KeyClocks for SequentialKeyClocks {
    /// Create a new `KeyClocks` instance.
    fn new(process_id: ProcessId, shard_id: ShardId) -> Self {
        Self {
            process_id,
            shard_id,
            seq: 0,
            clocks: HashMap::new(),
        }
    }

    // Generate the next clock.
    fn clock_next(&mut self) -> Clock {
        self.seq += 1;
        Clock::from(self.seq, self.process_id)
    }

    // Joins with remote clock.
    fn clock_join(&mut self, other: &Clock) {
        self.seq = std::cmp::max(self.seq, other.seq);
    }

    // Adds a new command with some tentative timestamp.
    // After this, it starts being reported as a predecessor of other commands
    // with tentative higher timestamps.
    fn add(&mut self, dot: Dot, cmd: &Command, clock: Clock) {
        cmd.keys(self.shard_id).for_each(|key| {
            // add ourselves to the set of commands and assert there was no
            // command with the same timestamp
            let res =
                self.commands(key, |commands| commands.insert(clock, dot));
            assert!(
                res.is_none(),
                "can't add a timestamp belonging to a command already added"
            );
        });
    }

    // Removes a previously added command with some tentative timestamp.
    // After this, it stops being reported as a predecessor of other commands.
    fn remove(&mut self, cmd: &Command, timestamp: Clock) {
        cmd.keys(self.shard_id).for_each(|key| {
            // remove ourselves from the set of commands and assert that we were
            // indeed in the set
            let res =
                self.commands(key, |commands| commands.remove(&timestamp));
            assert!(
                res.is_some(),
                "can't remove a timestamp belonging to a command never added"
            );
        });
    }

    /// Computes all conflicting commands with a timestamp lower than `clock`.
    /// If `higher` is set, it fills it with all the conflicting commands with a
    /// timestamp higher than `clock`.
    fn predecessors(
        &mut self,
        dot: Dot,
        cmd: &Command,
        clock: Clock,
        mut higher: Option<&mut HashSet<Dot>>,
    ) -> HashSet<Dot> {
        // TODO is this data structure ever GCed? otherwise the set that we
        // return here will grow unbounded as the more commands are processed in
        // the system
        let mut predecessors = HashSet::new();
        cmd.keys(self.shard_id).for_each(|key| {
            self.commands(key, |commands| {
                for (cmd_clock, cmd_dot) in commands {
                    match cmd_clock.cmp(&clock) {
                        Ordering::Less => {
                            // if it has a timestamp smaller than `clock`, add
                            // it as a predecessor
                            // - we don't assert that doesn't exist already
                            //   because the same
                            // `Dot` might be stored on different keys if we
                            // have multi-key
                            // commands
                            predecessors.insert(*cmd_dot);
                        }
                        Ordering::Greater => {
                            // if it has a timestamp smaller than `clock`, add
                            // it to `higher` if it's defined
                            if let Some(higher) = higher.as_deref_mut() {
                                higher.insert(*cmd_dot);
                            }
                        }
                        Ordering::Equal => {
                            if *cmd_dot != dot {
                                panic!("found command with the same timestamp")
                            }
                        }
                    }
                }
            })
        });
        predecessors
    }

    fn parallel() -> bool {
        false
    }
}

impl SequentialKeyClocks {
    fn commands<F, R>(&mut self, key: &Key, mut f: F) -> R
    where
        F: FnMut(&mut CommandsPerKey) -> R,
    {
        // get a mutable reference to current commands
        let commands = match self.clocks.get_mut(key) {
            Some(commands) => commands,
            None => self.clocks.entry(key.clone()).or_default(),
        };
        // apply function and return its result
        f(commands)
    }
}
