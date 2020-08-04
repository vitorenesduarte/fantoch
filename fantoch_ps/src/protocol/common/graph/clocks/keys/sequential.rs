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
    fn add_cmd(
        &mut self,
        dot: Dot,
        cmd: &Command,
        past: Option<VClock<ProcessId>>,
    ) -> VClock<ProcessId> {
        // we start with past in case there's one, or bottom otherwise
        let clock = match past {
            Some(past) => past,
            None => super::bottom_clock(self.shard_id, self.n),
        };
        self.do_add_cmd(dot, cmd, clock)
    }

    fn add_noop(&mut self, dot: Dot) -> VClock<ProcessId> {
        // start with an empty clock
        let clock = super::bottom_clock(self.shard_id, self.n);
        self.do_add_noop(dot, clock)
    }

    /// Checks the current `clock` for some command.
    #[cfg(test)]
    fn cmd_clock(&self, cmd: &Command) -> VClock<ProcessId> {
        // always start from the noop clock:
        let mut clock = super::bottom_clock(self.shard_id, self.n);
        clock.join(&self.noop_clock);
        self.do_cmd_clock(cmd, &mut clock);
        clock
    }

    /// Checks the current noop `clock`.
    #[cfg(test)]
    fn noop_clock(&self) -> VClock<ProcessId> {
        // always start from the noop clock:
        let mut clock = super::bottom_clock(self.shard_id, self.n);
        clock.join(&self.noop_clock);
        self.do_noop_clock(&mut clock);
        clock
    }

    fn parallel() -> bool {
        false
    }
}

impl SequentialKeyClocks {
    /// Adds a command's `Dot` to the clock of each key touched by the command.
    fn do_add_cmd(
        &mut self,
        dot: Dot,
        cmd: &Command,
        mut clock: VClock<ProcessId>,
    ) -> VClock<ProcessId> {
        // include the noop clock
        clock.join(&self.noop_clock);

        cmd.keys(self.shard_id).for_each(|key| {
            // get current clock for this key
            let current_clock = match self.clocks.get_mut(key) {
                Some(clock) => clock,
                None => {
                    // if key is not present, create bottom vclock for
                    // this key
                    let bottom = super::bottom_clock(self.shard_id, self.n);
                    // and insert it
                    self.clocks.entry(key.clone()).or_insert(bottom)
                }
            };
            // merge it with our clock
            clock.join(&current_clock);
            // add `dot` to the clock
            current_clock.add(&dot.source(), dot.sequence());
        });

        // and finally return the computed clock
        clock
    }

    fn do_add_noop(
        &mut self,
        dot: Dot,
        mut clock: VClock<ProcessId>,
    ) -> VClock<ProcessId> {
        // include the noop clock
        clock.join(&self.noop_clock);
        // add `dot to the noop clock
        self.noop_clock.add(&dot.source(), dot.sequence());

        // compute the clock for this noop
        self.do_noop_clock(&mut clock);

        clock
    }

    fn do_noop_clock(&self, clock: &mut VClock<ProcessId>) {
        // join with the clocks of *all keys*
        self.clocks.values().for_each(|current_clock| {
            clock.join(current_clock);
        });
    }

    #[cfg(test)]
    fn do_cmd_clock(&self, cmd: &Command, clock: &mut VClock<ProcessId>) {
        // join with the clocks of all keys touched by `cmd`
        cmd.keys(self.shard_id).for_each(|key| {
            // if the key is not present, then ignore it
            if let Some(current_clock) = self.clocks.get(key) {
                clock.join(current_clock);
            }
        });
    }
}
