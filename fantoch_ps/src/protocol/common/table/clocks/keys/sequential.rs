use super::KeyClocks;
use crate::protocol::common::table::{VoteRange, Votes};
use fantoch::command::Command;
use fantoch::id::ProcessId;
use fantoch::kvs::Key;
use fantoch::HashMap;
use std::cmp;

#[derive(Debug, Clone)]
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

    fn init_clocks(&mut self, cmd: &Command) {
        cmd.keys().for_each(|key| {
            // create entry if key not present yet
            if !self.clocks.contains_key(key) {
                self.clocks.insert(key.clone(), 0);
            }
        });
    }

    fn bump_and_vote(&mut self, cmd: &Command, min_clock: u64) -> (u64, Votes) {
        // bump to at least `min_clock`
        let clock = cmp::max(min_clock, self.clock(cmd) + 1);

        // compute votes up to that clock
        let votes = self.vote(cmd, clock);

        // return both
        (clock, votes)
    }

    fn vote(&mut self, cmd: &Command, up_to: u64) -> Votes {
        // create votes
        let mut votes = Votes::with_capacity(cmd.key_count());

        // vote on each key
        cmd.keys().for_each(|key| {
            // get a mutable reference to current clock value
            let current = match self.clocks.get_mut(key) {
                Some(current) => current,
                None => self.clocks.entry(key.clone()).or_insert(0),
            };

            Self::maybe_bump(self.id, key, current, up_to, &mut votes);
        });

        // return votes
        votes
    }

    fn vote_all(&mut self, up_to: u64) -> Votes {
        // create votes
        let mut votes = Votes::with_capacity(self.clocks.len());

        // vote on each key
        let id = self.id;
        self.clocks.iter_mut().for_each(|(key, current)| {
            Self::maybe_bump(id, key, current, up_to, &mut votes);
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

    fn maybe_bump(
        id: ProcessId,
        key: &Key,
        current: &mut u64,
        up_to: u64,
        votes: &mut Votes,
    ) {
        // if we should vote
        if *current < up_to {
            // vote from the current clock value + 1 until `clock`
            let vr = VoteRange::new(id, *current + 1, up_to);
            // update current clock to be `clock`
            *current = up_to;
            votes.set(key.clone(), vec![vr]);
        }
    }
}
