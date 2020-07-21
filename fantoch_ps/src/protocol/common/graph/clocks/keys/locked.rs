use super::KeyClocks;
use crate::protocol::common::shared::Shared;
use fantoch::command::Command;
use fantoch::id::{Dot, ProcessId, ShardId};
use parking_lot::RwLock;
use std::sync::Arc;
use threshold::VClock;

// all VClock's are protected by a rw-lock
type Clock = RwLock<VClock<ProcessId>>;

#[derive(Debug, Clone)]
pub struct LockedKeyClocks {
    shard_id: ShardId,
    n: usize, // number of processes
    clocks: Arc<Shared<Clock>>,
    noop_clock: Arc<Clock>,
}

impl KeyClocks for LockedKeyClocks {
    /// Create a new `LockedKeyClocks` instance.
    fn new(shard_id: ShardId, n: usize) -> Self {
        // create shared clocks
        let clocks = Shared::new();
        // wrap them in an arc
        let clocks = Arc::new(clocks);

        // create noop clock
        let noop_clock = super::bottom_clock(shard_id, n);
        // protect it with a rw-lock
        let noop_clock = RwLock::new(noop_clock);
        // wrap it in an arc
        let noop_clock = Arc::new(noop_clock);

        Self {
            shard_id,
            n,
            clocks,
            noop_clock,
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
        // we start with past in case there's one, or bottom otherwise
        let clock = match past {
            Some(past) => past,
            None => super::bottom_clock(self.shard_id, self.n),
        };

        // check if we have a noop or not and compute conflicts accordingly
        match cmd {
            Some(cmd) => self.add_cmd(dot, cmd, clock),
            None => self.add_noop(dot, clock),
        }
    }

    /// Checks the current `clock` for some command.
    #[cfg(test)]
    fn clock(&self, cmd: &Option<Command>) -> VClock<ProcessId> {
        // always start from the noop clock:
        let mut clock = super::bottom_clock(self.shard_id, self.n);
        clock.join(&self.noop_clock.read());
        match cmd {
            Some(cmd) => self.cmd_clock(cmd, &mut clock),
            None => self.noop_clock(&mut clock),
        }
        clock
    }

    fn parallel() -> bool {
        true
    }
}

impl LockedKeyClocks {
    fn add_cmd(
        &self,
        dot: Dot,
        cmd: &Command,
        mut clock: VClock<ProcessId>,
    ) -> VClock<ProcessId> {
        // include the noop clock:
        // - for this operation we only need a read lock
        clock.join(&self.noop_clock.read());

        // iterate through all command keys, grab a write lock, get their
        // current clock and add ourselves to it
        cmd.keys(self.shard_id).for_each(|key| {
            // get current clock
            let clock_entry = self.clocks.get(key);
            // grab a write lock
            let mut current_clock = clock_entry.write();
            // merge it with our clock
            clock.join(&current_clock);
            // add `dot` to the clock
            current_clock.add(&dot.source(), dot.sequence());
            // release the lock
            drop(current_clock);
        });

        // and finally return the computed clock
        clock
    }

    fn add_noop(
        &self,
        dot: Dot,
        mut clock: VClock<ProcessId>,
    ) -> VClock<ProcessId> {
        // grab a write lock to the noop clock and:
        // - include the noop clock in the final `clock`
        // - add ourselves to the noop clock:
        //   * during the next iteration a new key in the map might be created
        //     and we may miss it
        //   * by first adding ourselves to the noop clock we make sure that,
        //     even though we will not see that newly created key, that key will
        //     see us
        let mut noop_clock = self.noop_clock.write();
        clock.join(&noop_clock);
        noop_clock.add(&dot.source(), dot.sequence());
        // release the lock
        drop(noop_clock);

        // compute the clock for this noop
        self.noop_clock(&mut clock);

        clock
    }

    fn noop_clock(&self, clock: &mut VClock<ProcessId>) {
        // iterate through all keys, grab a read lock, and include their current
        // clock in the final `clock`
        self.clocks.iter().for_each(|entry| {
            // grab a read lock
            let current_clock = entry.value().read();
            // merge it with our clock
            clock.join(&current_clock);
            // release the lock
            drop(current_clock);
        });
    }

    #[cfg(test)]
    // TODO this is similar to a loop in `add_cmd`; can we refactor? yes but the
    // code would be more complicated (e.g. we would grab a read or a write lock
    // depending on whether we're adding the command to the current clocks),
    // thus it's probably not worth it
    fn cmd_clock(&self, cmd: &Command, clock: &mut VClock<ProcessId>) {
        // iterate through all command keys, grab a readlock, and include their
        // current clock in the final `clock`
        cmd.keys(self.shard_id).for_each(|key| {
            // get current clock
            let clock_entry = self.clocks.get(key);
            // grab a read lock
            let current_clock = clock_entry.read();
            // merge it with our clock
            clock.join(&current_clock);
            // release the lock
            drop(current_clock);
        });
    }
}
