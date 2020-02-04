use super::KeyClocks;
use crate::command::Command;
use crate::id::{Dot, ProcessId};
use crate::kvs::Key;
use crate::util;
use std::collections::HashMap;
use threshold::VClock;

#[derive(Clone)]
pub struct LockedKeyClocks {
    n: usize, // number of processes
    clocks: HashMap<Key, VClock<ProcessId>>,
    noop_clock: VClock<ProcessId>,
}

impl KeyClocks for LockedKeyClocks {
    /// Create a new `LockedKeyClocks` instance.
    fn new(n: usize) -> Self {
        Self {
            n,
            clocks: HashMap::new(),
            noop_clock: Self::bottom_clock(n),
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

impl LockedKeyClocks {
    /// Adds a command's `Dot` to the clock of each key touched by the command.
    fn add(&mut self, dot: Dot, cmd: &Option<Command>) {
        match cmd {
            Some(cmd) => {
                cmd.keys().for_each(|key| {
                    // get current clock for this key
                    let clock = match self.clocks.get_mut(key) {
                        Some(clock) => clock,
                        None => {
                            // if key is not present, create bottom vclock for
                            // this key
                            let bottom = Self::bottom_clock(self.n);
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
        let clock = Self::bottom_clock(self.n);
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
                cmd.keys().for_each(|key| {
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

    // Creates a bottom clock of size `n`.
    fn bottom_clock(n: usize) -> VClock<ProcessId> {
        let ids = util::process_ids(n);
        VClock::with(ids)
    }
}
