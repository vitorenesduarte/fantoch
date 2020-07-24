use super::KeyClocks;
use fantoch::command::Command;
use fantoch::id::{Dot, ProcessId, ShardId};
use fantoch::kvs::Key;
use fantoch::HashMap;
use threshold::VClock;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequentialKeyClocks {
    shard_id: ShardId,
    n: usize, // number of processes
    clocks: HashMap<Key, VClock<ProcessId>>,
    noop_clock: VClock<ProcessId>,
}

impl KeyClocks for SequentialKeyClocks {
    /// Create a new `SequentialKeyClocks` instance.
    fn new(shard_id: ShardId, n: usize) -> Self {
        Self {
            shard_id,
            n,
            clocks: HashMap::new(),
            noop_clock: super::bottom_clock(shard_id, n),
        }
    }

    /// Adds a command's `Dot` to the clock of each key touched by the command,
    /// returning the set of local conflicting commands including past in them
    /// in case there's a past.
    fn add(
        &mut self,
        dot: Dot,
        cmd: &Option<Command>,
        past: Option<VClock<ProcessId>>,
    ) -> VClock<ProcessId> {
        // first compute clock
        let clock = match past {
            Some(past) => self.clock_with_past(cmd, past),
            None => self.clock(cmd),
        };
        // then register this command
        self.add(dot, cmd);
        // and finally return the computed clock
        clock
    }

    /// Checks the current `clock` for some command.
    #[cfg(test)]
    fn clock(&self, cmd: &Option<Command>) -> VClock<ProcessId> {
        self.clock(cmd)
    }

    fn parallel() -> bool {
        false
    }
}

impl SequentialKeyClocks {
    /// Adds a command's `Dot` to the clock of each key touched by the command.
    fn add(&mut self, dot: Dot, cmd: &Option<Command>) {
        match cmd {
            Some(cmd) => {
                cmd.keys(self.shard_id).for_each(|key| {
                    // get current clock for this key
                    let clock = match self.clocks.get_mut(key) {
                        Some(clock) => clock,
                        None => {
                            // if key is not present, create bottom vclock for
                            // this key
                            let bottom =
                                super::bottom_clock(self.shard_id, self.n);
                            // and insert it
                            self.clocks.entry(key.clone()).or_insert(bottom)
                        }
                    };
                    // add command dot to each clock
                    clock.add(&dot.source(), dot.sequence());
                });
            }
            None => {
                // add command dot only to the noop clock
                self.noop_clock.add(&dot.source(), dot.sequence());
            }
        }
    }

    /// Checks the current `clock` for some command.
    fn clock(&self, cmd: &Option<Command>) -> VClock<ProcessId> {
        let clock = super::bottom_clock(self.shard_id, self.n);
        self.clock_with_past(cmd, clock)
    }

    /// Computes a clock for some command representing the `Dot`s of all
    /// conflicting commands observed, given an initial clock already with
    /// conflicting commands (that we denote by past).
    fn clock_with_past(
        &self,
        cmd: &Option<Command>,
        mut past: VClock<ProcessId>,
    ) -> VClock<ProcessId> {
        // always join with `self.noop_conf`
        past.join(&self.noop_clock);

        match cmd {
            Some(cmd) => {
                // join with the clocks of all keys touched by `cmd`
                cmd.keys(self.shard_id).for_each(|key| {
                    // if the key is not present, then ignore it
                    if let Some(clock) = self.clocks.get(key) {
                        past.join(clock);
                    }
                });
            }
            None => {
                // join with the clocks of *all keys*
                self.clocks.iter().for_each(|(_key, clock)| {
                    past.join(clock);
                });
            }
        }

        past
    }
}
