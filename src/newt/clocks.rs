use crate::base::ProcId;
use crate::command::MultiCommand;
use crate::newt::votes::{ProcVotes, VoteRange};
use crate::store::Key;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Clocks {
    id: ProcId,
    clocks: HashMap<Key, u64>,
}

impl Clocks {
    /// Create a new `Clocks` instance.
    pub fn new(id: ProcId) -> Self {
        Clocks {
            id,
            clocks: HashMap::new(),
        }
    }

    /// Compute the clock of this command.
    pub fn clock(&self, cmd: &MultiCommand) -> u64 {
        // compute the maximum between all clocks of the keys accessed by this
        // command
        cmd.keys()
            .iter()
            .map(|key| self.key_clock(key))
            .max()
            .unwrap_or(0)
    }

    /// Retrives the current clock of some key.
    fn key_clock(&self, key: &Key) -> u64 {
        self.clocks.get(key).cloned().unwrap_or(0)
    }

    /// Computes `ProcVotes`.
    pub fn proc_votes(&self, cmd: &MultiCommand, clock: u64) -> ProcVotes {
        cmd.keys()
            .into_iter()
            .map(|key| {
                // vote from the current clock value + 1 until the highest vote
                // (i.e. the maximum between all key's clocks)
                let vr =
                    VoteRange::new(self.id, self.key_clock(key) + 1, clock);
                (key.clone(), vr)
            })
            .collect()
    }

    /// Bump all keys clocks to `clock`.
    pub fn bump_to(&mut self, cmd: &MultiCommand, clock: u64) {
        for key in cmd.keys() {
            self.clocks.insert(key.clone(), clock);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::command::MultiCommand;
    use crate::newt::clocks::Clocks;

    #[test]
    fn clocks_flow() {
        // create clocks
        let mut clocks = Clocks::new(0);

        // keys and commands
        let key_a = String::from("A");
        let key_b = String::from("B");
        let cmd_a = MultiCommand::get(vec![key_a.clone()]);
        let cmd_b = MultiCommand::get(vec![key_b.clone()]);
        let cmd_ab = MultiCommand::get(vec![key_a.clone(), key_b.clone()]);

        // -------------------------
        // first clock for command a
        let clock = clocks.clock(&cmd_a);
        assert_eq!(clock, 0);

        // newt behaviour: current clock + 1
        let clock = clock + 1;

        // get proc votes
        let proc_votes = clocks.proc_votes(&cmd_a, clock);
        assert_eq!(proc_votes.len(), 1); // single key
        assert_eq!(proc_votes.get(&key_a).unwrap().votes(), vec![1]);

        // bump clocks
        clocks.bump_to(&cmd_a, clock);

        // -------------------------
        // second clock for command a
        let clock = clocks.clock(&cmd_a);
        assert_eq!(clock, 1);

        // newt behaviour: current clock + 1
        let clock = clock + 1;

        // get proc votes
        let proc_votes = clocks.proc_votes(&cmd_a, clock);
        assert_eq!(proc_votes.len(), 1); // single key
        assert_eq!(proc_votes.get(&key_a).unwrap().votes(), vec![2]);

        // bump clocks
        clocks.bump_to(&cmd_a, clock);

        // -------------------------
        // first clock for command ab
        let clock = clocks.clock(&cmd_ab);
        assert_eq!(clock, 2);

        // newt behaviour: current clock + 1
        let clock = clock + 1;

        // get proc votes
        let proc_votes = clocks.proc_votes(&cmd_ab, clock);
        assert_eq!(proc_votes.len(), 2); // two keys
        assert_eq!(proc_votes.get(&key_a).unwrap().votes(), vec![3]);
        assert_eq!(proc_votes.get(&key_b).unwrap().votes(), vec![1, 2, 3]);

        // bump clock
        clocks.bump_to(&cmd_ab, clock);

        // -------------------------
        // first clock for command b
        let clock = clocks.clock(&cmd_b);
        assert_eq!(clock, 3);

        // newt behaviour: current clock + 1
        let clock = clock + 1;

        // get proc votes
        let proc_votes = clocks.proc_votes(&cmd_a, clock);
        assert_eq!(proc_votes.len(), 1); // single key
        assert_eq!(proc_votes.get(&key_a).unwrap().votes(), vec![4]);
    }
}
