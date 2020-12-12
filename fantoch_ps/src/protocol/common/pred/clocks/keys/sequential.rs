use super::{Clock, KeyClocks};
use fantoch::command::Command;
use fantoch::id::{Dot, ProcessId, ShardId};
use fantoch::kvs::Key;
use fantoch::{HashMap, HashSet};
use std::cmp::Ordering;

// timestamps are unique and thus it's enough to store one command `Dot` per
// timestamp.
// Note: this `Clock` should correspond to the `clock` stored in Caesar process.
type CommandsPerKey = HashMap<Clock, Dot>;

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
            let res = self
                .update_commands(key, |commands| commands.insert(clock, dot));
            assert!(
                res.is_none(),
                "can't add a timestamp belonging to a command already added"
            );
        });
    }

    // Removes a previously added command with some tentative timestamp.
    // After this, it stops being reported as a predecessor of other commands.
    fn remove(&mut self, cmd: &Command, clock: Clock) {
        cmd.keys(self.shard_id).for_each(|key| {
            // remove ourselves from the set of commands and assert that we were
            // indeed in the set
            let res =
                self.update_commands(key, |commands| commands.remove(&clock));
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
        &self,
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
            self.apply_if_commands_contains_key(key, |commands| {
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
                                panic!("found different command with the same timestamp")
                            }
                        }
                    }
                }
            });
        });
        predecessors
    }

    fn parallel() -> bool {
        false
    }
}

impl SequentialKeyClocks {
    fn apply_if_commands_contains_key<F, R>(
        &self,
        key: &Key,
        mut f: F,
    ) -> Option<R>
    where
        F: FnMut(&CommandsPerKey) -> R,
    {
        // get a reference to current commands
        self.clocks.get(key).map(|commands| {
            // apply function and return its result
            f(commands)
        })
    }

    fn update_commands<F, R>(&mut self, key: &Key, mut f: F) -> R
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

#[cfg(test)]
mod tests {
    use super::*;
    use fantoch::id::Rifl;
    use fantoch::kvs::KVOp;
    use std::iter::FromIterator;

    fn deps(deps: Vec<Dot>) -> HashSet<Dot> {
        HashSet::from_iter(deps)
    }

    #[test]
    fn clock_test() {
        let p1 = 1;
        let p2 = 2;
        let shard_id = 0;
        let mut key_clocks = SequentialKeyClocks::new(p1, shard_id);

        assert_eq!(key_clocks.clock_next(), Clock::from(1, p1));
        assert_eq!(key_clocks.clock_next(), Clock::from(2, p1));

        // if we merge with an lower clock, everything remains as is
        key_clocks.clock_join(&Clock::from(1, p2));
        assert_eq!(key_clocks.clock_next(), Clock::from(3, p1));
        assert_eq!(key_clocks.clock_next(), Clock::from(4, p1));

        // if we merge with a higher clock, the next clock generated will be
        // higher than that
        key_clocks.clock_join(&Clock::from(10, p2));
        assert_eq!(key_clocks.clock_next(), Clock::from(11, p1));
        assert_eq!(key_clocks.clock_next(), Clock::from(12, p1));
    }

    #[test]
    fn predecessors_test() {
        let p1 = 1;
        let shard_id = 0;
        let mut key_clocks = SequentialKeyClocks::new(p1, shard_id);

        // create command on key A
        let cmd_a = Command::from(
            Rifl::new(1, 1),
            vec![(String::from("A"), KVOp::Put(String::new()))],
        );

        // create command on key B
        let cmd_b = Command::from(
            Rifl::new(1, 1),
            vec![(String::from("B"), KVOp::Put(String::new()))],
        );

        // create command on key C
        let cmd_c = Command::from(
            Rifl::new(1, 1),
            vec![(String::from("C"), KVOp::Put(String::new()))],
        );

        // create command on keys A and C
        let cmd_ac = Command::from(
            Rifl::new(1, 1),
            vec![
                (String::from("A"), KVOp::Put(String::new())),
                (String::from("C"), KVOp::Put(String::new())),
            ],
        );

        // create dots and clocks
        let dot = Dot::new(p1, 0); // some dot, doesn't matter
        let dot_1 = Dot::new(p1, 1);
        let dot_3 = Dot::new(p1, 3);
        let clock_1 = Clock::from(1, p1);
        let clock_2 = Clock::from(2, p1);
        let clock_3 = Clock::from(3, p1);
        let clock_4 = Clock::from(4, p1);

        let check = |key_clocks: &SequentialKeyClocks,
                     cmd: &Command,
                     clock: Clock,
                     expected_blocking: HashSet<Dot>,
                     expected_predecessors: HashSet<Dot>| {
            let mut blocking = HashSet::new();
            let predecessors =
                key_clocks.predecessors(dot, cmd, clock, Some(&mut blocking));
            assert_eq!(blocking, expected_blocking);
            assert_eq!(predecessors, expected_predecessors);
        };

        // in the beginning, nothing is reported
        check(&key_clocks, &cmd_a, clock_2, deps(vec![]), deps(vec![]));

        // --------------------------------------
        // add dot_1 with clock_1 on key a
        key_clocks.add(dot_1, &cmd_a, clock_1);

        // i. dot_1 is reported for command a with clock 2
        check(
            &key_clocks,
            &cmd_a,
            clock_2,
            deps(vec![]),
            deps(vec![dot_1]),
        );

        // ii. dot_1 is *not* reported for command b with clock 2
        check(&key_clocks, &cmd_b, clock_2, deps(vec![]), deps(vec![]));

        // iii. dot_1 is *not* reported for command c with clock 2
        check(&key_clocks, &cmd_c, clock_2, deps(vec![]), deps(vec![]));

        // iv. dot_1 is reported for command ac with clock 2
        check(
            &key_clocks,
            &cmd_ac,
            clock_2,
            deps(vec![]),
            deps(vec![dot_1]),
        );

        // --------------------------------------
        // add dot_3 with clock_3 on keys a and c
        key_clocks.add(dot_3, &cmd_ac, clock_3);

        // 1. check that nothing changed if we check again with clock 2
        // i. dot_1 is reported for command a with clock 2, and dot_3 blocks
        check(
            &key_clocks,
            &cmd_a,
            clock_2,
            deps(vec![dot_3]),
            deps(vec![dot_1]),
        );

        // ii. no dot is reported for command b with clock 2
        check(&key_clocks, &cmd_b, clock_2, deps(vec![]), deps(vec![]));

        // iii. dot_1 is *not* reported for command c with clock 2, but dot_3
        //      blocks
        check(
            &key_clocks,
            &cmd_c,
            clock_2,
            deps(vec![dot_3]),
            deps(vec![]),
        );

        // iv. dot_1 is reported for command ac with clock 2, and dot_3 blocks
        check(
            &key_clocks,
            &cmd_ac,
            clock_2,
            deps(vec![dot_3]),
            deps(vec![dot_1]),
        );

        // 2. now check for clock 4
        // i. dot_1 and dot_3 are reported for command a with clock 4
        check(
            &key_clocks,
            &cmd_a,
            clock_4,
            deps(vec![]),
            deps(vec![dot_1, dot_3]),
        );

        // ii. no dot is reported for command b with clock 4
        check(&key_clocks, &cmd_b, clock_4, deps(vec![]), deps(vec![]));

        // iii. only dot_3 is reported for command c with clock 4
        check(
            &key_clocks,
            &cmd_c,
            clock_4,
            deps(vec![]),
            deps(vec![dot_3]),
        );

        // iv. dot_1 and dot_3 are reported for command ac with clock 4
        check(
            &key_clocks,
            &cmd_ac,
            clock_4,
            deps(vec![]),
            deps(vec![dot_1, dot_3]),
        );

        // --------------------------------------
        // remove clock_1 on key a
        key_clocks.remove(&cmd_a, clock_1);

        // 1. check for clock 2
        // i. no dot is reported for command a with clock 2, and dot_3 blocks
        check(
            &key_clocks,
            &cmd_a,
            clock_2,
            deps(vec![dot_3]),
            deps(vec![]),
        );

        // ii. no dot is reported for command b with clock 2
        check(&key_clocks, &cmd_b, clock_2, deps(vec![]), deps(vec![]));

        // iii. no dot is reported for command c with clock 2, but dot_3
        //      blocks
        check(
            &key_clocks,
            &cmd_c,
            clock_2,
            deps(vec![dot_3]),
            deps(vec![]),
        );

        // iv. no dot is reported for command ac with clock 2, and dot_3 blocks
        check(
            &key_clocks,
            &cmd_ac,
            clock_2,
            deps(vec![dot_3]),
            deps(vec![]),
        );

        // 2. check for clock 4
        // i. only dot_3 is reported for command a with clock 4
        check(
            &key_clocks,
            &cmd_a,
            clock_4,
            deps(vec![]),
            deps(vec![dot_3]),
        );

        // ii. neither dot is reported for command b with clock 4
        check(&key_clocks, &cmd_b, clock_4, deps(vec![]), deps(vec![]));

        // iii. only dot_3 is reported for command c with clock 4
        check(
            &key_clocks,
            &cmd_c,
            clock_4,
            deps(vec![]),
            deps(vec![dot_3]),
        );

        // iv. only dot_3 are reported for command ac with clock 4
        check(
            &key_clocks,
            &cmd_ac,
            clock_4,
            deps(vec![]),
            deps(vec![dot_3]),
        );

        // --------------------------------------
        // remove clock_3 on key a c
        key_clocks.remove(&cmd_ac, clock_3);

        // check only for clock 4 that no dot is reported for any command
        check(&key_clocks, &cmd_a, clock_4, deps(vec![]), deps(vec![]));
        check(&key_clocks, &cmd_b, clock_4, deps(vec![]), deps(vec![]));
        check(&key_clocks, &cmd_c, clock_4, deps(vec![]), deps(vec![]));
        check(&key_clocks, &cmd_ac, clock_4, deps(vec![]), deps(vec![]));
    }
}
