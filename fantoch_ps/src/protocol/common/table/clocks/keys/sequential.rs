use super::KeyClocks;
use fantoch::command::Command;
use fantoch::id::ProcessId;
use fantoch::kvs::Key;
use crate::protocol::common::table::{VoteRange, Votes};
use std::cmp;
use std::collections::HashMap;

#[derive(Clone)]
pub struct SequentialKeyClocks {
    id: ProcessId,
    clocks: HashMap<Key, u64>,
}

impl KeyClocks for SequentialKeyClocks {
    /// Create a new `SequentialKeyClocks` instance.
    fn new(id: ProcessId) -> Self {
        let clocks = HashMap::new();
        Self { id, clocks }
    }

    fn bump_and_vote(&mut self, cmd: &Command, min_clock: u64) -> (u64, Votes) {
        // bump to at least `min_clock`
        let clock = cmp::max(min_clock, self.clock(cmd) + 1);

        // compute votes up to that clock
        let votes = self.vote(cmd, clock);

        // return both
        (clock, votes)
    }

    fn vote(&mut self, cmd: &Command, clock: u64) -> Votes {
        // create votes
        let mut votes = Votes::new(Some(cmd));

        // vote on each key
        cmd.keys().for_each(|key| {
            // get a mutable reference to current clock value
            let current = match self.clocks.get_mut(key) {
                Some(current) => current,
                None => self.clocks.entry(key.clone()).or_insert(0),
            };

            // if we should vote
            if *current < clock {
                // vote from the current clock value + 1 until `clock`
                let vr = VoteRange::new(self.id, *current + 1, clock);
                // update current clock to be `clock`
                *current = clock;
                votes.add(key, vr);
            }
        });

        // return votes
        votes
    }

    fn parallel() -> bool {
        false
    }
}

impl SequentialKeyClocks {
    /// Retrieves the current clock for some command.
    /// If the command touches multiple keys, returns the maximum between the
    /// clocks associated with each key.
    fn clock(&self, cmd: &Command) -> u64 {
        cmd.keys()
            .filter_map(|key| self.clocks.get(key))
            .max()
            .cloned()
            // if keys don't exist yet, we may have no maximum; in that case we
            // should return 0
            .unwrap_or(0)
    }
}
