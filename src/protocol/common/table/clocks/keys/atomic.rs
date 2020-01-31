use super::Clocks;
use super::KeyClocks;
use crate::command::Command;
use crate::id::ProcessId;
use crate::protocol::common::table::{VoteRange, Votes};
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

    fn bump_and_vote(&mut self, cmd: &Command, min_clock: u64) -> (u64, Votes) {
        // first round of votes:
        // - vote on each key and compute the highest clock seen
        let mut first_round_votes = Vec::with_capacity(cmd.key_count());
        let highest = cmd
            .keys()
            .map(|key| {
                let previous_value = self
                    .clocks
                    .get(key)
                    .fetch_update(
                        |value| Some(cmp::max(min_clock, value + 1)),
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .expect("first-round atomic always succeed as we must bump the clock");

                // compute vote start and vote end
                let vote_start = previous_value + 1;
                let vote_end = cmp::max(min_clock, previous_value + 1);

                // create vote range and save it
                let vr = VoteRange::new(self.id, vote_start, vote_end);
                first_round_votes.push((key.clone(), vr));

                // return vote end
                vote_end
            })
            .max()
            .expect("there should be a maximum sequence");

        // create votes
        let mut votes = Votes::new(Some(cmd));

        // second round of votes:
        // - vote on the keys that have a clock lower than the compute `highest`
        first_round_votes.into_iter().for_each(|(key, first_vr)| {
            // check if we should vote more
            if first_vr.end() < highest {
                let result = self.clocks.get(&key).fetch_update(
                    |value| {
                        if value < highest {
                            Some(highest)
                        } else {
                            None
                        }
                    },
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                );
                // check if we generated more votes (maybe votes by other
                // threads have been generated and it's no longer possible to
                // generate votes)
                if let Ok(previous_value) = result {
                    // compute vote start and vote end
                    let vote_start = previous_value + 1;
                    let vote_end = highest;

                    // create second vote range and save it
                    let second_vr =
                        VoteRange::new(self.id, vote_start, vote_end);
                    // save the two votes on this key
                    // TODO try to compress the two vote ranges
                    votes.set(key, vec![first_vr, second_vr]);
                } else {
                    // save the single vote on this key
                    votes.set(key, vec![first_vr]);
                }
            }
        });
        (highest, votes)
    }

    fn vote(&mut self, cmd: &Command, clock: u64) -> Votes {
        // create votes
        let mut votes = Votes::new(Some(cmd));

        cmd.keys().for_each(|key| {
            let result = self.clocks.get(&key).fetch_update(
                |value| {
                    if value < clock {
                        Some(clock)
                    } else {
                        None
                    }
                },
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
            if let Ok(previous_value) = result {
                // compute vote start and vote end
                let vote_start = previous_value + 1;
                let vote_end = clock;

                // create second vote range and save it
                let vr = VoteRange::new(self.id, vote_start, vote_end);
                votes.set(key.clone(), vec![vr]);
            }
        });

        votes
    }
}
