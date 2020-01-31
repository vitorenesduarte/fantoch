use super::Clocks;
use super::KeyClocks;
use crate::command::Command;
use crate::id::ProcessId;
use crate::protocol::common::table::{VoteRange, Votes};
use std::cmp;

#[derive(Clone)]
pub struct SequentialKeyClocks {
    id: ProcessId,
    clocks: Clocks<u64>,
}

impl KeyClocks for SequentialKeyClocks {
    /// Create a new `SequentialKeyClocks` instance.
    fn new(id: ProcessId, key_buckets_power: usize) -> Self {
        let clocks = Clocks::new(key_buckets_power);
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

        // TODO copy here to please the borrow-checker
        let id = self.id;

        // vote on each key
        cmd.keys().for_each(|key| {
            // get a mutable reference to current clock value
            let current = self.clocks.get_mut(key);

            // if we should vote
            if *current < clock {
                // vote from the current clock value + 1 until `clock`
                let vr = VoteRange::new(id, *current + 1, clock);
                // update current clock to be `clock`
                *current = clock;
                votes.add(key, vr);
            }
        });

        // return votes
        votes
    }
}

impl SequentialKeyClocks {
    /// Retrieves the current clock for some command.
    /// If the command touches multiple keys, returns the maximum between the
    /// clocks associated with each key.
    fn clock(&self, cmd: &Command) -> u64 {
        cmd.keys()
            .map(|key| *self.clocks.get(key))
            .max()
            .expect("there must be at least one key in the command")
    }
}
