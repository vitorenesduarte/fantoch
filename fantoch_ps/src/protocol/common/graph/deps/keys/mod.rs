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
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Dependency {
    pub dot: Dot,
    pub shards: Option<BTreeSet<ShardId>>,
}

impl Dependency {
    pub fn from_cmd(dot: Dot, cmd: &Command) -> Self {
        Self {
            dot,
            shards: Some(cmd.shards().cloned().collect()),
        }
    }

    pub fn from_noop(dot: Dot) -> Self {
        Self { dot, shards: None }
    }
}

pub trait KeyDeps: Debug + Clone {
    /// Create a new `KeyDeps` instance.
    fn new(shard_id: ShardId) -> Self;

    /// Sets the command's `Dot` as the latest command on each key touched by
    /// the command, returning the set of local conflicting commands
    /// including past in them, in case there's a past.
    fn add_cmd(
        &mut self,
        dot: Dot,
        cmd: &Command,
        past: Option<HashSet<Dependency>>,
    ) -> HashSet<Dependency>;

    /// Adds a noop.
    fn add_noop(&mut self, dot: Dot) -> HashSet<Dependency>;

    /// Checks the current dependencies for some command.
    #[cfg(test)]
    fn cmd_deps(&self, cmd: &Command) -> HashSet<Dot>;

    /// Checks the current dependencies for noops.
    #[cfg(test)]
    fn noop_deps(&self) -> HashSet<Dot>;

    fn parallel() -> bool;
}

#[cfg(test)]
fn extract_dots(deps: HashSet<Dependency>) -> HashSet<Dot> {
    deps.into_iter().map(|dep| dep.dot).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util;
    use fantoch::id::{DotGen, ProcessId, Rifl};
    use fantoch::kvs::KVOp;
    use fantoch::{HashMap, HashSet};
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

    fn multi_put(rifl: Rifl, keys: Vec<String>, value: String) -> Command {
        Command::from(
            rifl,
            keys.into_iter()
                .map(|key| (key.clone(), KVOp::Put(value.clone()))),
        )
    }

    fn key_deps_flow<KD: KeyDeps>() {
        // create key deps
        let shard_id = 0;
        let mut key_deps = KD::new(shard_id);

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
        let cmd_a = multi_put(cmd_a_rifl, vec![key_a.clone()], value.clone());

        // command b
        let cmd_b_rifl = Rifl::new(101, 1); // client 101, 1st op
        let cmd_b = multi_put(cmd_b_rifl, vec![key_b.clone()], value.clone());

        // command ab
        let cmd_ab_rifl = Rifl::new(102, 1); // client 102, 1st op
        let cmd_ab = multi_put(
            cmd_ab_rifl,
            vec![key_a.clone(), key_b.clone()],
            value.clone(),
        );

        // command c
        let cmd_c_rifl = Rifl::new(103, 1); // client 103, 1st op
        let cmd_c = multi_put(cmd_c_rifl, vec![key_c.clone()], value.clone());

        // empty conf for A
        let conf = key_deps.cmd_deps(&cmd_a);
        assert_eq!(conf, HashSet::new());

        // add A with {1,1}
        key_deps.add_cmd(dot_gen.next_id(), &cmd_a, None);

        // 1. conf with {1,1} for A
        // 2. empty conf for B
        // 3. conf with {1,1} for A-B
        // 4. empty conf for C
        // 5. conf with {1,1} for noop
        let deps_1_1 = HashSet::from_iter(vec![Dot::new(1, 1)]);
        assert_eq!(key_deps.cmd_deps(&cmd_a), deps_1_1);
        assert_eq!(key_deps.cmd_deps(&cmd_b), HashSet::new());
        assert_eq!(key_deps.cmd_deps(&cmd_ab), deps_1_1);
        assert_eq!(key_deps.cmd_deps(&cmd_c), HashSet::new());
        assert_eq!(key_deps.noop_deps(), deps_1_1);

        // add noop with {1,2}
        key_deps.add_noop(dot_gen.next_id());

        // 1. conf with {1,2}|{1,1} for A
        // 2. conf with {1,2}| for B
        // 3. conf with {1,2}|{1,1} for A-B
        // 4. conf with {1,2}| for C
        // 5. conf with {1,2}|{1,1} for noop
        let deps_1_2 = HashSet::from_iter(vec![Dot::new(1, 2)]);
        let deps_1_2_and_1_1 =
            HashSet::from_iter(vec![Dot::new(1, 1), Dot::new(1, 2)]);
        assert_eq!(key_deps.cmd_deps(&cmd_a), deps_1_2_and_1_1);
        assert_eq!(key_deps.cmd_deps(&cmd_b), deps_1_2);
        assert_eq!(key_deps.cmd_deps(&cmd_ab), deps_1_2_and_1_1);
        assert_eq!(key_deps.cmd_deps(&cmd_c), deps_1_2);
        assert_eq!(key_deps.noop_deps(), deps_1_2_and_1_1);

        // add B with {1,3}
        key_deps.add_cmd(dot_gen.next_id(), &cmd_b, None);

        // 1. conf with {1,2}|{1,1} for A
        // 2. conf with {1,2}|{1,3} for B
        // 3. conf with {1,2}|{1,1} and {1,3} for A-B
        // 4. conf with {1,2}| for C
        // 5. conf with {1,2}|{1,1} and {1,3} for noop
        let deps_1_2_and_1_3 =
            HashSet::from_iter(vec![Dot::new(1, 2), Dot::new(1, 3)]);
        let deps_1_2_and_1_1_and_1_3 = HashSet::from_iter(vec![
            Dot::new(1, 1),
            Dot::new(1, 2),
            Dot::new(1, 3),
        ]);
        assert_eq!(key_deps.cmd_deps(&cmd_a), deps_1_2_and_1_1);
        assert_eq!(key_deps.cmd_deps(&cmd_b), deps_1_2_and_1_3);
        assert_eq!(key_deps.cmd_deps(&cmd_ab), deps_1_2_and_1_1_and_1_3);
        assert_eq!(key_deps.cmd_deps(&cmd_c), deps_1_2);
        assert_eq!(key_deps.noop_deps(), deps_1_2_and_1_1_and_1_3);

        // add B with {1,4}
        key_deps.add_cmd(dot_gen.next_id(), &cmd_b, None);

        // 1. conf with {1,2}|{1,1} for A
        // 2. conf with {1,2}|{1,4} for B
        // 3. conf with {1,2}|{1,1} and {1,4} for A-B
        // 4. conf with {1,2}| for C
        // 5. conf with {1,2}|{1,1} and {1,4} for noop
        let deps_1_2_and_1_4 =
            HashSet::from_iter(vec![Dot::new(1, 2), Dot::new(1, 4)]);
        let deps_1_2_1_1_and_1_4 = HashSet::from_iter(vec![
            Dot::new(1, 1),
            Dot::new(1, 2),
            Dot::new(1, 4),
        ]);
        assert_eq!(key_deps.cmd_deps(&cmd_a), deps_1_2_and_1_1);
        assert_eq!(key_deps.cmd_deps(&cmd_b), deps_1_2_and_1_4);
        assert_eq!(key_deps.cmd_deps(&cmd_ab), deps_1_2_1_1_and_1_4);
        assert_eq!(key_deps.cmd_deps(&cmd_c), deps_1_2);
        assert_eq!(key_deps.noop_deps(), deps_1_2_1_1_and_1_4);

        // add A-B with {1,5}
        key_deps.add_cmd(dot_gen.next_id(), &cmd_ab, None);

        // 1. conf with {1,2}|{1,5} for A
        // 2. conf with {1,2}|{1,5} for B
        // 3. conf with {1,2}|{1,5} for A-B
        // 4. conf with {1,2}| for C
        // 5. conf with {1,2}|{1,5} for noop
        let deps_1_2_and_1_5 =
            HashSet::from_iter(vec![Dot::new(1, 2), Dot::new(1, 5)]);
        assert_eq!(key_deps.cmd_deps(&cmd_a), deps_1_2_and_1_5);
        assert_eq!(key_deps.cmd_deps(&cmd_b), deps_1_2_and_1_5);
        assert_eq!(key_deps.cmd_deps(&cmd_ab), deps_1_2_and_1_5);
        assert_eq!(key_deps.cmd_deps(&cmd_c), deps_1_2);
        assert_eq!(key_deps.noop_deps(), deps_1_2_and_1_5);

        // add A with {1,6}
        key_deps.add_cmd(dot_gen.next_id(), &cmd_a, None);

        // 1. conf with {1,2}|{1,6} for A
        // 2. conf with {1,2}|{1,5} for B
        // 3. conf with {1,2}|{1,6} and {1,5} for A-B
        // 4. conf with {1,2}| for C
        // 5. conf with {1,2}|{1,6} and {1,5} for noop
        let deps_1_2_and_1_6 =
            HashSet::from_iter(vec![Dot::new(1, 2), Dot::new(1, 6)]);
        let deps_1_2_and_1_5_and_1_6 = HashSet::from_iter(vec![
            Dot::new(1, 2),
            Dot::new(1, 5),
            Dot::new(1, 6),
        ]);
        assert_eq!(key_deps.cmd_deps(&cmd_a), deps_1_2_and_1_6);
        assert_eq!(key_deps.cmd_deps(&cmd_b), deps_1_2_and_1_5);
        assert_eq!(key_deps.cmd_deps(&cmd_ab), deps_1_2_and_1_5_and_1_6);
        assert_eq!(key_deps.cmd_deps(&cmd_c), deps_1_2);
        assert_eq!(key_deps.noop_deps(), deps_1_2_and_1_5_and_1_6);

        // add C with {1,7}
        key_deps.add_cmd(dot_gen.next_id(), &cmd_c, None);

        // 1. conf with {1,2}|{1,6} for A
        // 2. conf with {1,2}|{1,5} for B
        // 3. conf with {1,2}|{1,6} and {1,5} for A-B
        // 4. conf with {1,2}|{1,7} for C
        // 5. conf with {1,2}|{1,6} and {1,5} and {1,7} for noop
        let deps_1_2_and_1_7 =
            HashSet::from_iter(vec![Dot::new(1, 2), Dot::new(1, 7)]);
        let deps_1_2_and_1_5_and_1_6_and_1_7 = HashSet::from_iter(vec![
            Dot::new(1, 2),
            Dot::new(1, 5),
            Dot::new(1, 6),
            Dot::new(1, 7),
        ]);
        assert_eq!(key_deps.cmd_deps(&cmd_a), deps_1_2_and_1_6);
        assert_eq!(key_deps.cmd_deps(&cmd_b), deps_1_2_and_1_5);
        assert_eq!(key_deps.cmd_deps(&cmd_ab), deps_1_2_and_1_5_and_1_6);
        assert_eq!(key_deps.cmd_deps(&cmd_c), deps_1_2_and_1_7);
        assert_eq!(key_deps.noop_deps(), deps_1_2_and_1_5_and_1_6_and_1_7);

        // add noop with {1,8}
        key_deps.add_noop(dot_gen.next_id());

        // 1. conf with {1,8}|{1,6} for A
        // 2. conf with {1,8}|{1,5} for B
        // 3. conf with {1,8}|{1,6} and {1,5} for A-B
        // 4. conf with {1,8}|{1,7} for C
        // 5. conf with {1,8}|{1,6} and {1,5} and {1,7} for noop
        let deps_1_8_and_1_5 =
            HashSet::from_iter(vec![Dot::new(1, 8), Dot::new(1, 5)]);
        let deps_1_8_and_1_6 =
            HashSet::from_iter(vec![Dot::new(1, 8), Dot::new(1, 6)]);
        let deps_1_8_and_1_7 =
            HashSet::from_iter(vec![Dot::new(1, 8), Dot::new(1, 7)]);
        let deps_1_8_and_1_5_and_1_6 = HashSet::from_iter(vec![
            Dot::new(1, 8),
            Dot::new(1, 5),
            Dot::new(1, 6),
        ]);
        let deps_1_8_and_1_5_and_1_6_and_1_7 = HashSet::from_iter(vec![
            Dot::new(1, 8),
            Dot::new(1, 5),
            Dot::new(1, 6),
            Dot::new(1, 7),
        ]);
        assert_eq!(key_deps.cmd_deps(&cmd_a), deps_1_8_and_1_6);
        assert_eq!(key_deps.cmd_deps(&cmd_b), deps_1_8_and_1_5);
        assert_eq!(key_deps.cmd_deps(&cmd_ab), deps_1_8_and_1_5_and_1_6);
        assert_eq!(key_deps.cmd_deps(&cmd_c), deps_1_8_and_1_7);
        assert_eq!(key_deps.noop_deps(), deps_1_8_and_1_5_and_1_6_and_1_7);

        // add B with {1,9}
        key_deps.add_cmd(dot_gen.next_id(), &cmd_b, None);

        // 1. conf with {1,8}|{1,6} for A
        // 2. conf with {1,8}|{1,9} for B
        // 3. conf with {1,8}|{1,6} and {1,9} for A-B
        // 4. conf with {1,8}|{1,7} for C
        // 5. conf with {1,8}|{1,6} and {1,9} and {1,7} for noop
        let deps_1_8_and_1_6 =
            HashSet::from_iter(vec![Dot::new(1, 8), Dot::new(1, 6)]);
        let deps_1_8_and_1_9 =
            HashSet::from_iter(vec![Dot::new(1, 8), Dot::new(1, 9)]);
        let deps_1_8_and_1_6_and_1_9 = HashSet::from_iter(vec![
            Dot::new(1, 8),
            Dot::new(1, 6),
            Dot::new(1, 9),
        ]);
        let deps_1_8_and_1_6_and_1_7_and_1_9 = HashSet::from_iter(vec![
            Dot::new(1, 8),
            Dot::new(1, 6),
            Dot::new(1, 7),
            Dot::new(1, 9),
        ]);
        assert_eq!(key_deps.cmd_deps(&cmd_a), deps_1_8_and_1_6);
        assert_eq!(key_deps.cmd_deps(&cmd_b), deps_1_8_and_1_9);
        assert_eq!(key_deps.cmd_deps(&cmd_ab), deps_1_8_and_1_6_and_1_9);
        assert_eq!(key_deps.cmd_deps(&cmd_c), deps_1_8_and_1_7);
        assert_eq!(key_deps.noop_deps(), deps_1_8_and_1_6_and_1_7_and_1_9);
    }

    #[test]
    fn concurrent_locked_test() {
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
        let key_deps = KD::new(shard_id);

        // spawn workers
        let handles: Vec<_> = (1..=nthreads)
            .map(|process_id| {
                let key_deps_clone = key_deps.clone();
                thread::spawn(move || {
                    worker(
                        process_id as ProcessId,
                        key_deps_clone,
                        ops_number,
                        max_keys_per_command,
                        keys_number,
                        noop_probability,
                    )
                })
            })
            .collect();

        // wait for all workers and aggregate their deps
        let mut all_deps = HashMap::new();
        for handle in handles {
            let results = handle.join().expect("worker should finish");
            for (dot, cmd, deps) in results {
                let res = all_deps.insert(dot, (cmd, deps));
                assert!(res.is_none());
            }
        }

        // get all dots
        let dots: Vec<_> = all_deps.keys().cloned().collect();

        // check for each possible pair of operations if they conflict
        for i in 0..dots.len() {
            for j in (i + 1)..dots.len() {
                let dot_a = dots[i];
                let dot_b = dots[j];
                let (cmd_a, _) =
                    all_deps.get(&dot_a).expect("dot_a must exist");
                let (cmd_b, _) =
                    all_deps.get(&dot_b).expect("dot_b must exist");

                let should_conflict = match (cmd_a, cmd_b) {
                    (Some(cmd_a), Some(cmd_b)) => {
                        // neither command is a noop
                        cmd_a.conflicts(&cmd_b)
                    }
                    _ => {
                        // at least one of the command is a noop, and thus they
                        // conflict
                        true
                    }
                };

                if should_conflict {
                    let conflict = is_dep(dot_a, dot_b, &all_deps)
                        || is_dep(dot_b, dot_a, &all_deps);
                    assert!(conflict, "dot {:?} should be a dependency of {:?} (or the other way around); but that was not the case: {:?}", dot_a, dot_b, all_deps);
                }
            }
        }
    }

    fn is_dep(
        dot: Dot,
        dep: Dot,
        all_deps: &HashMap<Dot, (Option<Command>, HashSet<Dot>)>,
    ) -> bool {
        // check if it's direct dependency, and if it's not a direct dependency,
        // do depth-first-search
        let (_, deps) = all_deps.get(&dot).expect("dot must exist");
        deps.contains(&dep)
            || deps.iter().any(|dep| is_dep(dot, *dep, all_deps))
    }

    fn worker<K: KeyDeps>(
        process_id: ProcessId,
        mut key_deps: K,
        ops_number: usize,
        max_keys_per_command: usize,
        keys_number: usize,
        noop_probability: usize,
    ) -> Vec<(Dot, Option<Command>, HashSet<Dot>)> {
        // create dot gen
        let mut dot_gen = DotGen::new(process_id);
        // all deps worker has generated
        let mut all_deps = Vec::new();

        for _ in 0..ops_number {
            // generate dot
            let dot = dot_gen.next_id();
            // generate command
            let cmd = util::gen_cmd(
                max_keys_per_command,
                keys_number,
                noop_probability,
            );
            // compute deps
            let deps = match cmd.as_ref() {
                Some(cmd) => {
                    // add as command
                    key_deps.add_cmd(dot, &cmd, None)
                }
                None => {
                    // add as noop
                    key_deps.add_noop(dot)
                }
            };
            // save deps
            all_deps.push((dot, cmd, extract_dots(deps)));
        }

        all_deps
    }
}
