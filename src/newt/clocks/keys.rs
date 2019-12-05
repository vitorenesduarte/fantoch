use crate::base::ProcId;
use crate::command::Command;
use crate::kvs::Key;
use crate::newt::votes::{ProcVotes, VoteRange};
use std::collections::HashMap;

pub struct KeysClocks {
    id: ProcId,
    clocks: HashMap<Key, u64>,
}

impl KeysClocks {
    /// Create a new `KeysClocks` instance.
    pub fn new(id: ProcId) -> Self {
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
            .unwrap_or(0)
    }

    /// Computes the votes consumed by this command.
    pub fn proc_votes(&mut self, cmd: &Command, highest: u64) -> ProcVotes {
        cmd.keys()
            .map(|key| {
                // vote from the current clock value + 1 until the highest vote
                // (i.e. the maximum between all key's clocks)
                let previous = self.key_clock_swap(key, highest);

                // create vote if we should
                let vote = if previous < highest {
                    let vr = VoteRange::new(self.id, previous + 1, highest);
                    Some(vr)
                } else {
                    None
                };
                (key.clone(), vote)
            })
            .collect()
    }

    /// Retrieves the current clock for a single `key`.
    fn key_clock(&self, key: &Key) -> u64 {
        self.clocks.get(key).cloned().unwrap_or(0)
    }

    /// Updates the clock of this `key` to be `value` and returns the previous
    /// clock value.
    fn key_clock_swap(&mut self, key: &Key, value: u64) -> u64 {
        self.clocks.insert(key.clone(), value).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Rifl;

    #[test]
    fn key_clocks_flow() {
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
        let cmd_ab =
            Command::multi_get(cmd_ab_rifl, vec![key_a.clone(), key_b.clone()]);

        // closure to retrieve the votes on some key
        let get_key_votes = |votes: &ProcVotes, key: &Key| {
            votes.get(key).unwrap().as_ref().unwrap().votes()
        };

        // -------------------------
        // first clock for command a
        let clock = clocks.clock(&cmd_a);
        assert_eq!(clock, 0);

        // newt behaviour: current clock + 1
        let clock = clock + 1;

        // get proc votes
        let proc_votes = clocks.proc_votes(&cmd_a, clock);
        assert_eq!(proc_votes.len(), 1); // single key
        assert_eq!(get_key_votes(&proc_votes, &key_a), vec![1]);

        // -------------------------
        // second clock for command a
        let clock = clocks.clock(&cmd_a);
        assert_eq!(clock, 1);

        // newt behaviour: current clock + 1
        let clock = clock + 1;

        // get proc votes
        let proc_votes = clocks.proc_votes(&cmd_a, clock);
        assert_eq!(proc_votes.len(), 1); // single key
        assert_eq!(get_key_votes(&proc_votes, &key_a), vec![2]);

        // -------------------------
        // first clock for command ab
        let clock = clocks.clock(&cmd_ab);
        assert_eq!(clock, 2);

        // newt behaviour: current clock + 1
        let clock = clock + 1;

        // get proc votes
        let proc_votes = clocks.proc_votes(&cmd_ab, clock);
        assert_eq!(proc_votes.len(), 2); // two keys
        assert_eq!(get_key_votes(&proc_votes, &key_a), vec![3]);
        assert_eq!(get_key_votes(&proc_votes, &key_b), vec![1, 2, 3]);

        // -------------------------
        // first clock for command b
        let clock = clocks.clock(&cmd_b);
        assert_eq!(clock, 3);

        // newt behaviour: current clock + 1
        let clock = clock + 1;

        // get proc votes
        let proc_votes = clocks.proc_votes(&cmd_a, clock);
        assert_eq!(proc_votes.len(), 1); // single key
        assert_eq!(get_key_votes(&proc_votes, &key_a), vec![4]);
    }
}
