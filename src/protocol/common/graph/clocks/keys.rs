use crate::command::Command;
use crate::id::{Dot, ProcessId};
use crate::kvs::Key;
use crate::util;
use std::collections::HashMap;
use threshold::VClock;

#[derive(Clone)]
pub struct KeysClocks {
    n: usize, // number of processes
    clocks: HashMap<Key, VClock<ProcessId>>,
    noop_clock: VClock<ProcessId>,
}

impl KeysClocks {
    /// Create a new `KeysClocks` instance.
    pub fn new(n: usize) -> Self {
        Self {
            n,
            clocks: HashMap::new(),
            noop_clock: Self::bottom_clock(n),
        }
    }

    /// Adds a command's `Dot` to the clock of each key touched by the command.
    pub fn add(&mut self, dot: Dot, cmd: &Option<Command>) {
        match cmd {
            Some(cmd) => {
                cmd.keys().for_each(|key| {
                    // get current clock for this key
                    let clock = match self.clocks.get_mut(key) {
                        Some(clock) => clock,
                        None => {
                            // if key is not present, create bottom vclock for this key
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

    /// Computes a clock for some command representing the `Dot`s of all conflicting commands
    /// observed.
    pub fn clock(&self, cmd: &Option<Command>) -> VClock<ProcessId> {
        let clock = Self::bottom_clock(self.n);
        self.clock_with_past(cmd, clock)
    }

    /// Computes a clock for some command representing the `Dot`s of all conflicting commands
    /// observed, given an initial clock already with conflicting commands (that we denote by past).
    pub fn clock_with_past(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{DotGen, Rifl};

    #[test]
    fn keys_clocks_flow() {
        // create key clocks
        let n = 1;
        let mut clocks = KeysClocks::new(n);

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
        let cmd_a = Some(Command::put(cmd_a_rifl, key_a.clone(), value.clone()));

        // command b
        let cmd_b_rifl = Rifl::new(101, 1); // client 101, 1st op
        let cmd_b = Some(Command::put(cmd_b_rifl, key_b.clone(), value.clone()));

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
        let cmd_c = Some(Command::put(cmd_c_rifl, key_c.clone(), value.clone()));

        // noop
        let noop = None;

        // empty conf for A
        let conf = clocks.clock(&cmd_a);
        assert_eq!(conf, util::vclock(vec![0]));

        // add A with {1,1}
        clocks.add(dot_gen.next_id(), &cmd_a);

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
        clocks.add(dot_gen.next_id(), &noop);

        // conf with {1,2} for A, B, A-B, C and noop
        assert_eq!(clocks.clock(&cmd_a), util::vclock(vec![2]));
        assert_eq!(clocks.clock(&cmd_b), util::vclock(vec![2]));
        assert_eq!(clocks.clock(&cmd_ab), util::vclock(vec![2]));
        assert_eq!(clocks.clock(&cmd_c), util::vclock(vec![2]));
        assert_eq!(clocks.clock(&noop), util::vclock(vec![2]));

        // add B with {1,3}
        clocks.add(dot_gen.next_id(), &cmd_b);

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
        clocks.add(dot_gen.next_id(), &cmd_b);

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
        clocks.add(dot_gen.next_id(), &cmd_ab);

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
        clocks.add(dot_gen.next_id(), &cmd_a);

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
        clocks.add(dot_gen.next_id(), &cmd_c);

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
        clocks.add(dot_gen.next_id(), &noop);

        // conf with {1,8} for A, B, A-B, C and noop
        assert_eq!(clocks.clock(&cmd_a), util::vclock(vec![8]));
        assert_eq!(clocks.clock(&cmd_b), util::vclock(vec![8]));
        assert_eq!(clocks.clock(&cmd_ab), util::vclock(vec![8]));
        assert_eq!(clocks.clock(&cmd_c), util::vclock(vec![8]));
        assert_eq!(clocks.clock(&noop), util::vclock(vec![8]));

        // add B with {1,9}
        clocks.add(dot_gen.next_id(), &cmd_b);

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
}
