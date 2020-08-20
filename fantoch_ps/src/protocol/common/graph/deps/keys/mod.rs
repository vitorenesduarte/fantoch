// This module contains the definition of `SequentialKeyDeps`.
mod sequential;

// This module contains the definition of `LockedKeyDeps`.
mod locked;

// Re-exports.
pub use locked::LockedKeyDeps;
pub use sequential::SequentialKeyDeps;

use fantoch::command::Command;
use fantoch::id::{Dot, ShardId};
use fantoch::HashSet;
use std::fmt::Debug;

pub trait KeyDeps: Debug + Clone {
    /// Create a new `KeyClocks` instance.
    fn new(shard_id: ShardId) -> Self;

    /// Sets the command's `Dot` as the latest command on each key touched by
    /// the command, returning the set of local conflicting commands
    /// including past in them, in case there's a past.
    fn add_cmd(
        &mut self,
        dot: Dot,
        cmd: &Command,
        past: Option<HashSet<Dot>>,
    ) -> HashSet<Dot>;

    /// Adds a noop.
    fn add_noop(&mut self, dot: Dot) -> HashSet<Dot>;

    /// Checks the current dependencies for some command.
    #[cfg(test)]
    fn cmd_deps(&self, cmd: &Command) -> HashSet<Dot>;

    /// Checks the current dependencies for noops.
    #[cfg(test)]
    fn noop_deps(&self) -> HashSet<Dot>;

    fn parallel() -> bool;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util;
    use fantoch::id::{DotGen, ProcessId, Rifl};
    use fantoch::HashSet;
    use std::iter::FromIterator;
    use std::thread;

    #[test]
    fn sequential_key_deps() {
        key_deps_flow::<SequentialKeyDeps>();
    }

    #[test]
    fn locked_key_deps() {
        key_deps_flow::<LockedKeyDeps>();
    }

    fn key_deps_flow<KD: KeyDeps>() {
        // create key deps
        let shard_id = 0;
        let mut clocks = KD::new(shard_id);

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
        let cmd_a = Command::put(cmd_a_rifl, key_a.clone(), value.clone());

        // command b
        let cmd_b_rifl = Rifl::new(101, 1); // client 101, 1st op
        let cmd_b = Command::put(cmd_b_rifl, key_b.clone(), value.clone());

        // command ab
        let cmd_ab_rifl = Rifl::new(102, 1); // client 102, 1st op
        let cmd_ab = Command::multi_put(
            cmd_ab_rifl,
            vec![
                (key_a.clone(), value.clone()),
                (key_b.clone(), value.clone()),
            ],
        );

        // command c
        let cmd_c_rifl = Rifl::new(103, 1); // client 103, 1st op
        let cmd_c = Command::put(cmd_c_rifl, key_c.clone(), value.clone());

        // empty conf for A
        let conf = clocks.cmd_deps(&cmd_a);
        assert_eq!(conf, HashSet::new());

        // add A with {1,1}
        clocks.add_cmd(dot_gen.next_id(), &cmd_a, None);

        // 1. conf with {1,1} for A
        // 2. empty conf for B
        // 3. conf with {1,1} for A-B
        // 4. empty conf for C
        // 5. conf with {1,1} for noop
        let deps_1_1 = HashSet::from_iter(vec![Dot::new(1, 1)]);
        assert_eq!(clocks.cmd_deps(&cmd_a), deps_1_1);
        assert_eq!(clocks.cmd_deps(&cmd_b), HashSet::new());
        assert_eq!(clocks.cmd_deps(&cmd_ab), deps_1_1);
        assert_eq!(clocks.cmd_deps(&cmd_c), HashSet::new());
        assert_eq!(clocks.noop_deps(), deps_1_1);

        // add noop with {1,2}
        clocks.add_noop(dot_gen.next_id());

        // conf with {1,2} for A, B, A-B, C and noop
        let deps_1_2 = HashSet::from_iter(vec![Dot::new(1, 2)]);
        assert_eq!(clocks.cmd_deps(&cmd_a), deps_1_2);
        assert_eq!(clocks.cmd_deps(&cmd_b), deps_1_2);
        assert_eq!(clocks.cmd_deps(&cmd_ab), deps_1_2);
        assert_eq!(clocks.cmd_deps(&cmd_c), deps_1_2);
        assert_eq!(clocks.noop_deps(), deps_1_2);

        // add B with {1,3}
        clocks.add_cmd(dot_gen.next_id(), &cmd_b, None);

        // 1. conf with {1,2} for A
        // 2. conf with {1,3} for B
        // 3. conf with {1,3} for A-B
        // 4. conf with {1,2} for C
        // 5. conf with {1,3} for noop
        let deps_1_3 = HashSet::from_iter(vec![Dot::new(1, 3)]);
        assert_eq!(clocks.cmd_deps(&cmd_a), deps_1_2);
        assert_eq!(clocks.cmd_deps(&cmd_b), deps_1_3);
        assert_eq!(clocks.cmd_deps(&cmd_ab), deps_1_3);
        assert_eq!(clocks.cmd_deps(&cmd_c), deps_1_2);
        assert_eq!(clocks.noop_deps(), deps_1_3);

        // add B with {1,4}
        clocks.add_cmd(dot_gen.next_id(), &cmd_b, None);

        // 1. conf with {1,2} for A
        // 2. conf with {1,4} for B
        // 3. conf with {1,4} for A-B
        // 4. conf with {1,2} for C
        // 5. conf with {1,4} for noop
        let deps_1_4 = HashSet::from_iter(vec![Dot::new(1, 4)]);
        assert_eq!(clocks.cmd_deps(&cmd_a), deps_1_2);
        assert_eq!(clocks.cmd_deps(&cmd_b), deps_1_4);
        assert_eq!(clocks.cmd_deps(&cmd_ab), deps_1_4);
        assert_eq!(clocks.cmd_deps(&cmd_c), deps_1_2);
        assert_eq!(clocks.noop_deps(), deps_1_4);

        // add A-B with {1,5}
        clocks.add_cmd(dot_gen.next_id(), &cmd_ab, None);

        // 1. conf with {1,5} for A
        // 2. conf with {1,5} for B
        // 3. conf with {1,5} for A-B
        // 4. conf with {1,2} for C
        // 5. conf with {1,5} for noop
        let deps_1_5 = HashSet::from_iter(vec![Dot::new(1, 5)]);
        assert_eq!(clocks.cmd_deps(&cmd_a), deps_1_5);
        assert_eq!(clocks.cmd_deps(&cmd_b), deps_1_5);
        assert_eq!(clocks.cmd_deps(&cmd_ab), deps_1_5);
        assert_eq!(clocks.cmd_deps(&cmd_c), deps_1_2);
        assert_eq!(clocks.noop_deps(), deps_1_5);

        // add A with {1,6}
        clocks.add_cmd(dot_gen.next_id(), &cmd_a, None);

        // 1. conf with {1,6} for A
        // 2. conf with {1,5} for B
        // 3. conf with {1,6} for A-B
        // 4. conf with {1,2} for C
        // 5. conf with {1,6} for noop
        let deps_1_6 = HashSet::from_iter(vec![Dot::new(1, 6)]);
        assert_eq!(clocks.cmd_deps(&cmd_a), deps_1_6);
        assert_eq!(clocks.cmd_deps(&cmd_b), deps_1_5);
        assert_eq!(clocks.cmd_deps(&cmd_ab), deps_1_5);
        assert_eq!(clocks.cmd_deps(&cmd_c), deps_1_2);
        assert_eq!(clocks.noop_deps(), deps_1_6);

        // add C with {1,7}
        clocks.add_cmd(dot_gen.next_id(), &cmd_c, None);

        // 1. conf with {1,6} for A
        // 2. conf with {1,5} for B
        // 3. conf with {1,1} for A-B
        // 4. conf with {1,7} for C
        // 5. conf with {1,7} for noop
        let deps_1_7 = HashSet::from_iter(vec![Dot::new(1, 7)]);
        assert_eq!(clocks.cmd_deps(&cmd_a), deps_1_6);
        assert_eq!(clocks.cmd_deps(&cmd_b), deps_1_5);
        assert_eq!(clocks.cmd_deps(&cmd_ab), deps_1_5);
        assert_eq!(clocks.cmd_deps(&cmd_c), deps_1_7);
        assert_eq!(clocks.noop_deps(), deps_1_7);

        // add noop with {1,8}
        clocks.add_noop(dot_gen.next_id());

        // conf with {1,8} for A, B, A-B, C and noop
        let deps_1_8 = HashSet::from_iter(vec![Dot::new(1, 8)]);
        assert_eq!(clocks.cmd_deps(&cmd_a), deps_1_8);
        assert_eq!(clocks.cmd_deps(&cmd_b), deps_1_8);
        assert_eq!(clocks.cmd_deps(&cmd_ab), deps_1_8);
        assert_eq!(clocks.cmd_deps(&cmd_c), deps_1_8);
        assert_eq!(clocks.noop_deps(), deps_1_8);

        // add B with {1,9}
        clocks.add_cmd(dot_gen.next_id(), &cmd_b, None);

        // 1. conf with {1,8} for A
        // 2. conf with {1,9} for B
        // 3. conf with {1,9} for A-B
        // 4. conf with {1,8} for C
        // 5. conf with {1,9} for noop
        let deps_1_9 = HashSet::from_iter(vec![Dot::new(1, 9)]);
        assert_eq!(clocks.cmd_deps(&cmd_a), deps_1_8);
        assert_eq!(clocks.cmd_deps(&cmd_b), deps_1_9);
        assert_eq!(clocks.cmd_deps(&cmd_ab), deps_1_9);
        assert_eq!(clocks.cmd_deps(&cmd_c), deps_1_8);
        assert_eq!(clocks.noop_deps(), deps_1_9);
    }

    #[test]
    fn locked_test() {
        let nthreads = 2;
        let ops_number = 3000;
        let max_keys_per_command = 2;
        let keys_number = 4;
        let noop_probability = 50;
        for _ in 0..10 {
            concurrent_test::<LockedKeyDeps>(
                nthreads,
                ops_number,
                max_keys_per_command,
                keys_number,
                noop_probability,
            );
        }
    }

    fn concurrent_test<KD: KeyDeps + Send + Sync + 'static>(
        nthreads: usize,
        ops_number: usize,
        max_keys_per_command: usize,
        keys_number: usize,
        noop_probability: usize,
    ) {
        // create key deps
        let shard_id = 0;
        let clocks = KD::new(shard_id);

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
                    let (dot_a, deps_a) = ops[i];
                    let (dot_b, deps_b) = ops[j];
                    let conflict =
                        deps_a.contains(&dot_b) || deps_b.contains(&dot_a);
                    assert!(conflict);
                }
            }
        }
    }

    fn worker<K: KeyDeps>(
        process_id: ProcessId,
        mut clocks: K,
        ops_number: usize,
        max_keys_per_command: usize,
        keys_number: usize,
        noop_probability: usize,
    ) -> Vec<(Dot, Option<Command>, HashSet<Dot>)> {
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
            let clock = match cmd.as_ref() {
                Some(cmd) => {
                    // add as command
                    clocks.add_cmd(dot, &cmd, None)
                }
                None => {
                    // add as noop
                    clocks.add_noop(dot)
                }
            };
            // save clock
            all_clocks.push((dot, cmd, clock));
        }

        all_clocks
    }
}
