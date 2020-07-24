// This module contains the definition of `SequentialKeyClocks`.
mod sequential;

// This module contains the definition of `LockedKeyClocks`.
mod locked;

// Re-exports.
pub use locked::LockedKeyClocks;
pub use sequential::SequentialKeyClocks;

use fantoch::command::Command;
use fantoch::id::{Dot, ProcessId, ShardId};
use fantoch::util;
use std::fmt::Debug;
use threshold::VClock;

pub trait KeyClocks: Debug + Clone {
    /// Create a new `KeyClocks` instance given the number of processes.
    fn new(shard_id: ShardId, n: usize) -> Self;

    /// Adds a command's `Dot` to the clock of each key touched by the command,
    /// returning the set of local conflicting commands including past in them
    /// in case there's a past.
    fn add(
        &mut self,
        dot: Dot,
        cmd: &Option<Command>,
        past: Option<VClock<ProcessId>>,
    ) -> VClock<ProcessId>;

    /// Checks the current `clock` for some command.
    /// Atlas and EPaxos implementation don't actually use this.
    #[cfg(test)]
    fn clock(&self, cmd: &Option<Command>) -> VClock<ProcessId>;

    fn parallel() -> bool;
}

// Creates a bottom clock of size `n`.
fn bottom_clock(shard_id: ShardId, n: usize) -> VClock<ProcessId> {
    let ids = util::process_ids(shard_id, n);
    VClock::with(ids)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util;
    use fantoch::id::{DotGen, Rifl};
    use fantoch::HashSet;
    use std::thread;

    #[test]
    fn sequential_key_clocks() {
        keys_clocks_flow::<SequentialKeyClocks>();
    }

    #[test]
    fn locked_key_clocks() {
        keys_clocks_flow::<LockedKeyClocks>();
    }

    fn keys_clocks_flow<KC: KeyClocks>() {
        // create key clocks
        let shard_id = 0;
        let n = 1;
        let mut clocks = KC::new(shard_id, n);

        // create dot gen
        let process_id = 1;
        let mut dot_gen = DotGen::new(process_id);

        // keys
        let key_a = String::from("A");
        let key_b = String::from("B");
        let key_c = String::from("C");
        let value = String::from("");

        // command a
        let cmd_a_rifl = Rifl::new(100, 1); // client 100, 1st op
        let cmd_a =
            Some(Command::put(cmd_a_rifl, key_a.clone(), value.clone()));

        // command b
        let cmd_b_rifl = Rifl::new(101, 1); // client 101, 1st op
        let cmd_b =
            Some(Command::put(cmd_b_rifl, key_b.clone(), value.clone()));

        // command ab
        let cmd_ab_rifl = Rifl::new(102, 1); // client 102, 1st op
        let cmd_ab = Some(Command::multi_put(
            cmd_ab_rifl,
            vec![
                (key_a.clone(), value.clone()),
                (key_b.clone(), value.clone()),
            ],
        ));

        // command c
        let cmd_c_rifl = Rifl::new(103, 1); // client 103, 1st op
        let cmd_c =
            Some(Command::put(cmd_c_rifl, key_c.clone(), value.clone()));

        // noop
        let noop = None;

        // empty conf for A
        let conf = clocks.clock(&cmd_a);
        assert_eq!(conf, util::vclock(vec![0]));

        // add A with {1,1}
        clocks.add(dot_gen.next_id(), &cmd_a, None);

        // 1. conf with {1,1} for A
        // 2. empty conf for B
        // 3. conf with {1,1} for A-B
        // 4. empty conf for C
        // 5. conf with {1,1} for noop
        assert_eq!(clocks.clock(&cmd_a), util::vclock(vec![1]));
        assert_eq!(clocks.clock(&cmd_b), util::vclock(vec![0]));
        assert_eq!(clocks.clock(&cmd_ab), util::vclock(vec![1]));
        assert_eq!(clocks.clock(&cmd_c), util::vclock(vec![0]));
        assert_eq!(clocks.clock(&noop), util::vclock(vec![1]));

        // add noop with {1,2}
        clocks.add(dot_gen.next_id(), &noop, None);

        // conf with {1,2} for A, B, A-B, C and noop
        assert_eq!(clocks.clock(&cmd_a), util::vclock(vec![2]));
        assert_eq!(clocks.clock(&cmd_b), util::vclock(vec![2]));
        assert_eq!(clocks.clock(&cmd_ab), util::vclock(vec![2]));
        assert_eq!(clocks.clock(&cmd_c), util::vclock(vec![2]));
        assert_eq!(clocks.clock(&noop), util::vclock(vec![2]));

        // add B with {1,3}
        clocks.add(dot_gen.next_id(), &cmd_b, None);

        // 1. conf with {1,2} for A
        // 2. conf with {1,3} for B
        // 3. conf with {1,3} for A-B
        // 4. conf with {1,2} for C
        // 5. conf with {1,3} for noop
        assert_eq!(clocks.clock(&cmd_a), util::vclock(vec![2]));
        assert_eq!(clocks.clock(&cmd_b), util::vclock(vec![3]));
        assert_eq!(clocks.clock(&cmd_ab), util::vclock(vec![3]));
        assert_eq!(clocks.clock(&cmd_c), util::vclock(vec![2]));
        assert_eq!(clocks.clock(&noop), util::vclock(vec![3]));

        // add B with {1,4}
        clocks.add(dot_gen.next_id(), &cmd_b, None);

        // 1. conf with {1,2} for A
        // 2. conf with {1,4} for B
        // 3. conf with {1,4} for A-B
        // 4. conf with {1,2} for C
        // 5. conf with {1,4} for noop
        assert_eq!(clocks.clock(&cmd_a), util::vclock(vec![2]));
        assert_eq!(clocks.clock(&cmd_b), util::vclock(vec![4]));
        assert_eq!(clocks.clock(&cmd_ab), util::vclock(vec![4]));
        assert_eq!(clocks.clock(&cmd_c), util::vclock(vec![2]));
        assert_eq!(clocks.clock(&noop), util::vclock(vec![4]));

        // add A-B with {1,5}
        clocks.add(dot_gen.next_id(), &cmd_ab, None);

        // 1. conf with {1,5} for A
        // 2. conf with {1,5} for B
        // 3. conf with {1,5} for A-B
        // 4. conf with {1,2} for C
        // 5. conf with {1,5} for noop
        assert_eq!(clocks.clock(&cmd_a), util::vclock(vec![5]));
        assert_eq!(clocks.clock(&cmd_b), util::vclock(vec![5]));
        assert_eq!(clocks.clock(&cmd_ab), util::vclock(vec![5]));
        assert_eq!(clocks.clock(&cmd_c), util::vclock(vec![2]));
        assert_eq!(clocks.clock(&noop), util::vclock(vec![5]));

        // add A with {1,6}
        clocks.add(dot_gen.next_id(), &cmd_a, None);

        // 1. conf with {1,6} for A
        // 2. conf with {1,5} for B
        // 3. conf with {1,6} for A-B
        // 4. conf with {1,2} for C
        // 5. conf with {1,6} for noop
        assert_eq!(clocks.clock(&cmd_a), util::vclock(vec![6]));
        assert_eq!(clocks.clock(&cmd_b), util::vclock(vec![5]));
        assert_eq!(clocks.clock(&cmd_ab), util::vclock(vec![6]));
        assert_eq!(clocks.clock(&cmd_c), util::vclock(vec![2]));
        assert_eq!(clocks.clock(&noop), util::vclock(vec![6]));

        // add C with {1,7}
        clocks.add(dot_gen.next_id(), &cmd_c, None);

        // 1. conf with {1,6} for A
        // 2. conf with {1,5} for B
        // 3. conf with {1,1} for A-B
        // 4. conf with {1,7} for C
        // 5. conf with {1,7} for noop
        assert_eq!(clocks.clock(&cmd_a), util::vclock(vec![6]));
        assert_eq!(clocks.clock(&cmd_b), util::vclock(vec![5]));
        assert_eq!(clocks.clock(&cmd_ab), util::vclock(vec![6]));
        assert_eq!(clocks.clock(&cmd_c), util::vclock(vec![7]));
        assert_eq!(clocks.clock(&noop), util::vclock(vec![7]));

        // add noop with {1,8}
        clocks.add(dot_gen.next_id(), &noop, None);

        // conf with {1,8} for A, B, A-B, C and noop
        assert_eq!(clocks.clock(&cmd_a), util::vclock(vec![8]));
        assert_eq!(clocks.clock(&cmd_b), util::vclock(vec![8]));
        assert_eq!(clocks.clock(&cmd_ab), util::vclock(vec![8]));
        assert_eq!(clocks.clock(&cmd_c), util::vclock(vec![8]));
        assert_eq!(clocks.clock(&noop), util::vclock(vec![8]));

        // add B with {1,9}
        clocks.add(dot_gen.next_id(), &cmd_b, None);

        // 1. conf with {1,8} for A
        // 2. conf with {1,9} for B
        // 3. conf with {1,9} for A-B
        // 4. conf with {1,8} for C
        // 5. conf with {1,9} for noop
        assert_eq!(clocks.clock(&cmd_a), util::vclock(vec![8]));
        assert_eq!(clocks.clock(&cmd_b), util::vclock(vec![9]));
        assert_eq!(clocks.clock(&cmd_ab), util::vclock(vec![9]));
        assert_eq!(clocks.clock(&cmd_c), util::vclock(vec![8]));
        assert_eq!(clocks.clock(&noop), util::vclock(vec![9]));
    }

    #[test]
    fn locked_clocks_test() {
        let nthreads = 2;
        let ops_number = 3000;
        let max_keys_per_command = 2;
        let keys_number = 4;
        let noop_probability = 50;
        for _ in 0..10 {
            concurrent_test::<LockedKeyClocks>(
                nthreads,
                ops_number,
                max_keys_per_command,
                keys_number,
                noop_probability,
            );
        }
    }

    fn concurrent_test<K: KeyClocks + Send + Sync + 'static>(
        nthreads: usize,
        ops_number: usize,
        max_keys_per_command: usize,
        keys_number: usize,
        noop_probability: usize,
    ) {
        // create clocks:
        // - clocks have on entry per worker and each worker has its own
        //   `DotGen`
        let shard_id = 0;
        let clocks = K::new(shard_id, nthreads);

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
                    all_keys.extend(
                        cmd.keys(shard_id).cloned().map(|key| Some(key)),
                    );
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
                        if cmd.contains_key(shard_id, &key) {
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

    fn worker<K: KeyClocks>(
        process_id: ProcessId,
        mut clocks: K,
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
