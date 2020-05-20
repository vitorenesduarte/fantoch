use super::KeyClocks;
use crate::protocol::common::shared::Shared;
use fantoch::command::Command;
use fantoch::id::{Dot, ProcessId};
use parking_lot::RwLock;
use std::sync::Arc;
use threshold::VClock;

// all VClock's are protected by a rw-lock
type Clock = RwLock<VClock<ProcessId>>;

#[derive(Clone)]
pub struct LockedKeyClocks {
    n: usize, // number of processes
    clocks: Arc<Shared<Clock>>,
    noop_clock: Arc<Clock>,
}

impl KeyClocks for LockedKeyClocks {
    /// Create a new `LockedKeyClocks` instance.
    fn new(n: usize) -> Self {
        // create shared clocks
        let clocks = Shared::new();
        // wrap them in an arc
        let clocks = Arc::new(clocks);

        // create noop clock
        let noop_clock = super::bottom_clock(n);
        // protect it with a rw-lock
        let noop_clock = RwLock::new(noop_clock);
        // wrap it in an arc
        let noop_clock = Arc::new(noop_clock);

        Self {
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
            None => super::bottom_clock(self.n),
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
        let mut clock = super::bottom_clock(self.n);
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
        cmd.keys().for_each(|key| {
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
        cmd.keys().for_each(|key| {
            // get current clock
            let clock_entry = self.clocks.get(key);
            // grab a read lock
            let current_clock = clock_entry.read();
            // merge it with our clock
            clock.join(&current_clock);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util;
    use fantoch::id::DotGen;
    use rand::Rng;
    use std::collections::HashSet;
    use std::thread;

    #[test]
    fn locked_clocks_test() {
        let min_nthreads = 2;
        let max_nthreads = 8;
        let ops_number = 1000;
        let max_keys_per_command = 4;
        let keys_number = 16;
        let noop_probability = 50;
        for _ in 0..10 {
            let nthreads =
                rand::thread_rng().gen_range(min_nthreads, max_nthreads + 1);
            test(
                nthreads,
                ops_number,
                max_keys_per_command,
                keys_number,
                noop_probability,
            );
        }
    }

    fn test(
        nthreads: usize,
        ops_number: usize,
        max_keys_per_command: usize,
        keys_number: usize,
        noop_probability: usize,
    ) {
        // create clocks:
        // - clocks have on entry per worker and each worker has its own
        //   `DotGen`
        let clocks = LockedKeyClocks::new(nthreads);

        // spawn workers
        let handles: Vec<_> = (1..=nthreads)
            .map(|process_id| {
                let clocks_clone = clocks.clone();
                thread::spawn(move || {
                    worker(
                        process_id as ProcessId,
                        clocks_clone,
                        ops_number,
                        max_keys_per_command,
                        keys_number,
                        noop_probability,
                    )
                })
            })
            .collect();

        // wait for all workers and aggregate their clocks
        let mut all_clocks = Vec::new();
        let mut all_keys = HashSet::new();
        for handle in handles {
            let clocks = handle.join().expect("worker should finish");
            for (dot, cmd, clock) in clocks {
                if let Some(cmd) = &cmd {
                    all_keys.extend(cmd.keys().cloned().map(|key| Some(key)));
                } else {
                    all_keys.insert(None);
                }
                all_clocks.push((dot, cmd, clock));
            }
        }

        // for each key, check that for every two operations that access that
        // key, one is a dependency of the other
        for key in all_keys {
            // get all operations with this color
            let ops: Vec<_> = all_clocks
                .iter()
                .filter_map(|(dot, cmd, clock)| match (&key, cmd) {
                    (Some(key), Some(cmd)) => {
                        // if we have a key and not a noop, include command if
                        // it accesses the key
                        if cmd.contains_key(&key) {
                            Some((dot, clock))
                        } else {
                            None
                        }
                    }
                    _ => {
                        // otherwise, i.e.:
                        // - a key and a noop
                        // - the noop color and an op or noop
                        // always include
                        Some((dot, clock))
                    }
                })
                .collect();

            // check for each possible pair of operations if they conflict
            for i in 0..ops.len() {
                for j in (i + 1)..ops.len() {
                    let (dot_a, clock_a) = ops[i];
                    let (dot_b, clock_b) = ops[j];
                    let conflict = clock_a
                        .contains(&dot_b.source(), dot_b.sequence())
                        || clock_b.contains(&dot_a.source(), dot_a.sequence());
                    assert!(conflict);
                }
            }
        }
    }

    fn worker(
        process_id: ProcessId,
        mut clocks: LockedKeyClocks,
        ops_number: usize,
        max_keys_per_command: usize,
        keys_number: usize,
        noop_probability: usize,
    ) -> Vec<(Dot, Option<Command>, VClock<ProcessId>)> {
        // create dot gen
        let mut dot_gen = DotGen::new(process_id);
        // all clocks worker has generated
        let mut all_clocks = Vec::new();

        for _ in 0..ops_number {
            // generate dot
            let dot = dot_gen.next_id();
            // generate command
            // TODO here we should also generate noops
            let cmd = util::gen_cmd(
                max_keys_per_command,
                keys_number,
                noop_probability,
            );
            // get clock
            let clock = clocks.add(dot, &cmd, None);
            // save clock
            all_clocks.push((dot, cmd, clock));
        }

        all_clocks
    }
}
