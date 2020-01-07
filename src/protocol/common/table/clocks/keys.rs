use crate::command::Command;
use crate::id::ProcessId;
use crate::kvs::Key;
use crate::protocol::common::table::{ProcessVotes, VoteRange};
use std::collections::HashMap;

pub struct KeysClocks {
    id: ProcessId,
    clocks: HashMap<Key, u64>,
}

impl KeysClocks {
    /// Create a new `KeysClocks` instance.
    pub fn new(id: ProcessId) -> Self {
        Self {
            id,
            clocks: HashMap::new(),
        }
    }

    /// Retrieves the current clock for some command.
    /// If the command touches multiple keys, returns the maximum between the
    /// clocks associated with each key.
    pub fn clock(&self, cmd: &Command) -> u64 {
        cmd.keys()
            .map(|key| self.key_clock(key))
            .max()
            .expect("there must be at least one key in the command")
    }

    /// Vote up-to `clock`.
    pub fn process_votes(&mut self, cmd: &Command, clock: u64) -> ProcessVotes {
        cmd.keys()
            .filter_map(|key| {
                // get a mutable reference to current clock value
                // TODO refactoring the following match block into a function will not work due to
                // limitations in the borrow-checker
                // - see: https://rust-lang.github.io/rfcs/2094-nll.html#problem-case-3-conditional-control-flow-across-functions
                // - this is supposed to be fixed by polonius: http://smallcultfollowing.com/babysteps/blog/2018/06/15/mir-based-borrow-check-nll-status-update/#polonius
                let current = match self.clocks.get_mut(key) {
                    Some(value) => value,
                    None => self.clocks.entry(key.clone()).or_insert(0),
                };

                // if we should vote
                if *current < clock {
                    // vote from the current clock value + 1 until `clock`
                    let vr = VoteRange::new(self.id, *current + 1, clock);
                    // update current clock to be `clock`
                    *current = clock;
                    Some((key.clone(), vr))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Retrieves the current clock for `key`.
    fn key_clock(&self, key: &Key) -> u64 {
        self.clocks.get(key).cloned().unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::Rifl;

    // Returns the list of votes on some key.
    fn get_key_votes(key: &Key, votes: &ProcessVotes) -> Vec<u64> {
        let vr = votes
            .get(key)
            .expect("process should have voted on this key");
        (vr.start()..=vr.end()).collect()
    }

    #[test]
    fn keys_clocks_flow() {
        // create key clocks
        let mut clocks = KeysClocks::new(1);

        // keys
        let key_a = String::from("A");
        let key_b = String::from("B");

        // command a
        let cmd_a_rifl = Rifl::new(100, 1); // client 100, 1st op
        let cmd_a = Command::get(cmd_a_rifl, key_a.clone());

        // command b
        let cmd_b_rifl = Rifl::new(101, 1); // client 101, 1st op
        let cmd_b = Command::get(cmd_b_rifl, key_b.clone());

        // command ab
        let cmd_ab_rifl = Rifl::new(102, 1); // client 102, 1st op
        let cmd_ab = Command::multi_get(cmd_ab_rifl, vec![key_a.clone(), key_b.clone()]);

        // -------------------------
        // first clock for command a
        let clock = clocks.clock(&cmd_a);
        assert_eq!(clock, 0);

        // newt behaviour: current clock + 1
        let clock = clock + 1;

        // get process votes
        let process_votes = clocks.process_votes(&cmd_a, clock);
        assert_eq!(process_votes.len(), 1); // single key
        assert_eq!(get_key_votes(&key_a, &process_votes), vec![1]);

        // -------------------------
        // second clock for command a
        let clock = clocks.clock(&cmd_a);
        assert_eq!(clock, 1);

        // newt behaviour: current clock + 1
        let clock = clock + 1;

        // get process votes
        let process_votes = clocks.process_votes(&cmd_a, clock);
        assert_eq!(process_votes.len(), 1); // single key
        assert_eq!(get_key_votes(&key_a, &process_votes), vec![2]);

        // -------------------------
        // first clock for command ab
        let clock = clocks.clock(&cmd_ab);
        assert_eq!(clock, 2);

        // newt behaviour: current clock + 1
        let clock = clock + 1;

        // get process votes
        let process_votes = clocks.process_votes(&cmd_ab, clock);
        assert_eq!(process_votes.len(), 2); // two keys
        assert_eq!(get_key_votes(&key_a, &process_votes), vec![3]);
        assert_eq!(get_key_votes(&key_b, &process_votes), vec![1, 2, 3]);

        // -------------------------
        // first clock for command b
        let clock = clocks.clock(&cmd_b);
        assert_eq!(clock, 3);

        // newt behaviour: current clock + 1
        let clock = clock + 1;

        // get process votes
        let process_votes = clocks.process_votes(&cmd_a, clock);
        assert_eq!(process_votes.len(), 1); // single key
        assert_eq!(get_key_votes(&key_a, &process_votes), vec![4]);
    }

    #[test]
    fn keys_clocks_no_double_votes() {
        // create key clocks
        let mut clocks = KeysClocks::new(1);

        // command
        let key = String::from("A");
        let cmd_rifl = Rifl::new(100, 1);
        let cmd = Command::get(cmd_rifl, key.clone());

        // get process votes up to 5
        let process_votes = clocks.process_votes(&cmd, 5);
        assert_eq!(process_votes.len(), 1); // single key
        assert_eq!(get_key_votes(&key, &process_votes), vec![1, 2, 3, 4, 5]);

        // get process votes up to 5 again: should get no votes
        let process_votes = clocks.process_votes(&cmd, 5);
        assert!(process_votes.is_empty());

        // get process votes up to 6
        let process_votes = clocks.process_votes(&cmd, 6);
        assert_eq!(process_votes.len(), 1); // single key
        assert_eq!(get_key_votes(&key, &process_votes), vec![6]);

        // get process votes up to 2: should get no votes
        let process_votes = clocks.process_votes(&cmd, 2);
        assert!(process_votes.is_empty());

        // get process votes up to 3: should get no votes
        let process_votes = clocks.process_votes(&cmd, 3);
        assert!(process_votes.is_empty());

        // get process votes up to 10
        let process_votes = clocks.process_votes(&cmd, 10);
        assert_eq!(process_votes.len(), 1); // single key
        assert_eq!(get_key_votes(&key, &process_votes), vec![7, 8, 9, 10]);
    }
}
