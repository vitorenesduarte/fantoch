use super::Clocks;
use super::KeyClocks;
use crate::command::Command;
use crate::id::ProcessId;
use crate::kvs::Key;
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
                // bump the `key` clock
                let previous_value = self.bump(key, min_clock);
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
                // try to bump up to `highest`
                // - we really mean try because maybe votes by other threads
                //   have been generated and it's no longer possible to generate
                //   votes below `highest`
                if let Some(previous_value) = self.maybe_bump(&key, highest) {
                    // compute vote start and vote end
                    let vote_start = previous_value + 1;
                    let vote_end = highest;

                    // create second vote range and save it
                    let second_vr =
                        VoteRange::new(self.id, vote_start, vote_end);
                    // save the two votes on this key
                    votes
                        .set(key, VoteRange::try_compress(first_vr, second_vr));
                    return;
                }
            }
            // if we didn't vote agian, then simply save the single vote from
            // the first round
            votes.set(key, vec![first_vr]);
        });
        (highest, votes)
    }

    fn vote(&mut self, cmd: &Command, clock: u64) -> Votes {
        // create votes
        let mut votes = Votes::new(Some(cmd));

        cmd.keys().for_each(|key| {
            if let Some(previous_value) = self.maybe_bump(key, clock) {
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

impl AtomicKeyClocks {
    // Bump the `key` clock to at least `min_clock`.
    fn bump(&self, key: &Key, min_clock: u64) -> u64 {
        self.clocks
            .get(key)
            .fetch_update(
                |value| Some(cmp::max(min_clock, value + 1)),
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .expect("atomic bump should always succeed")
    }

    // Bump the `key` clock if lower than `up_to`.
    fn maybe_bump(&self, key: &Key, up_to: u64) -> Option<u64> {
        self.clocks
            .get(&key)
            .fetch_update(
                |value| {
                    if value < up_to {
                        Some(up_to)
                    } else {
                        None
                    }
                },
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .ok()
    }
}
