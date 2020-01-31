#![feature(no_more_cas)]

use super::Clocks;
use super::KeyClocks;
use crate::command::Command;
use crate::id::ProcessId;
use crate::protocol::common::table::{ProcessVotes, VoteRange};
use std::cmp;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub struct AtomicKeyClocks {
    id: ProcessId,
    clocks: Arc<Clocks<AtomicU64>>,
}

impl KeyClocks for AtomicKeyClocks {
    /// Create a new `AtomicKeyClocks` instance.
    fn new(id: ProcessId, key_buckets_power: usize) -> Self {
        let clocks = Clocks::new(key_buckets_power);
        Self {
            id,
            clocks: Arc::new(clocks),
        }
    }

    fn bump_and_vote(
        &mut self,
        cmd: &Command,
        min_clock: u64,
    ) -> (u64, ProcessVotes) {
        // create a list of votes with the maximum possible number of votes:
        // - in the sequential version, we *must* have `cmd.key_count()` votes
        // - here, since we do two rounds of voting, we can have *almost* `2 *
        //   cmd.key_count()`
        // - in the second round we don't generate votes *at least* in the key
        //   responsible for the highest clock, and thus, at most we have `2 *
        //   cmd.key_count() - 1` votes
        let max_vote_count = 2 * cmd.key_count() - 1;
        let mut votes = Vec::with_capacity(max_vote_count);

        // first round of votes and compute highest sequence
        let clock = cmd
            .into_iter()
            .map(|key| {
                let previous_value = self
                    .clocks
                    .get(key)
                    .fetch_update(
                        |value| Some(cmp::max(min_clock, value + 1)),
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .expect("updates always succeed");

                // compute vote start and vote end
                let vote_start = previous_value + 1;
                let vote_end = cmp::max(min_clock, previous_value + 1);

                // create vote range and save it
                let vr = VoteRange::new(self.id, vote_start, vote_end);
                votes.push(vr);

                // return vote end
                vote_end
            })
            .max()
            .expect("there should be a maximum sequence");

        let new_votes: Vec<_> = votes
            .iter()
            .filter_map(|(key, _vote_start, vote_end)| {
                // check if we should vote more
                if *vote_end < max_sequence {
                    let result = self.keys[*key].fetch_update(
                        |value| {
                            if value < max_sequence {
                                Some(max_sequence)
                            } else {
                                None
                            }
                        },
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    );
                    // check if we generated more votes (maybe votes by other
                    // threads have been generated and it's
                    // no longer possible to generate votes)
                    if let Ok(previous_value) = result {
                        let vote_start = previous_value + 1;
                        let vote_end = max_sequence;
                        return Some((*key, vote_start, vote_end));
                    }
                }
                None
            })
            .collect();

        votes.extend(new_votes);
        assert_eq!(votes.capacity(), max_vote_count);
        votes
    }

    fn vote(&mut self, cmd: &Command, clock: u64) -> ProcessVotes {
        // TODO copy here to please the borrow-checker
        let id = self.id;
        cmd.keys()
            .filter_map(|key| {
                // get a mutable reference to current clock value
                let current = self.clocks.get_mut(key);

                // if we should vote
                if *current < clock {
                    // vote from the current clock value + 1 until `clock`
                    let vr = VoteRange::new(id, *current + 1, clock);
                    // update current clock to be `clock`
                    *current = clock;
                    Some((key.clone(), vr))
                } else {
                    None
                }
            })
            .collect()
    }
}

impl AtomicKeyClocks {
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
