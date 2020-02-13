use super::KeyClocks;
use crate::protocol::common::shared_clocks::SharedClocks;
use crate::protocol::common::table::{VoteRange, Votes};
use fantoche::command::Command;
use fantoche::id::ProcessId;
use fantoche::kvs::Key;
use std::cmp;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub struct AtomicKeyClocks {
    id: ProcessId,
    clocks: Arc<SharedClocks<AtomicU64>>,
}

impl KeyClocks for AtomicKeyClocks {
    /// Create a new `AtomicKeyClocks` instance.
    fn new(id: ProcessId) -> Self {
        // create shared clocks
        let clocks = SharedClocks::new();
        // wrap them in an arc
        let clocks = Arc::new(clocks);

        Self { id, clocks }
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
        // - vote on the keys that have a clock lower than the computed
        //   `highest`
        first_round_votes.into_iter().for_each(|(key, first_vr)| {
            // check if we should vote more
            if first_vr.end() < highest {
                // try to bump up to `highest`
                // - we really mean try because maybe votes by other threads
                //   have been generated and it's no longer possible to generate
                //   votes below `highest`
                // - this means that this second round is actually optional is
                //   it's only here for performance reasons (i.e. reduce
                //   execution delay)
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

    fn parallel() -> bool {
        true
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

    // Bump the `key` clock to `up_to` if lower than `up_to`.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util;
    use rand::Rng;
    use std::collections::BTreeSet;
    use std::iter::FromIterator;
    use std::thread;

    #[test]
    fn atomic_clocks_test() {
        let min_nthreads = 2;
        let max_nthreads = 8;
        let ops_number = 1000;
        let max_keys_per_command = 4;
        let keys_number = 16;
        for _ in 0..200 {
            let nthreads =
                rand::thread_rng().gen_range(min_nthreads, max_nthreads + 1);
            test(nthreads, ops_number, max_keys_per_command, keys_number);
        }
    }

    fn test(
        nthreads: usize,
        ops_number: usize,
        max_keys_per_command: usize,
        keys_number: usize,
    ) {
        // create clocks
        let process_id = 1;
        let clocks = AtomicKeyClocks::new(process_id);

        // spawn workers
        let handles: Vec<_> = (0..nthreads)
            .map(|_| {
                let clocks_clone = clocks.clone();
                thread::spawn(move || {
                    worker(
                        clocks_clone,
                        ops_number,
                        max_keys_per_command,
                        keys_number,
                    )
                })
            })
            .collect();

        // wait for all workers and aggregate their votes
        let mut all_votes = Votes::new(None);
        for handle in handles {
            let votes = handle.join().expect("worker should finish");
            all_votes.merge(votes);
        }

        // verify votes
        for (_, key_votes) in all_votes {
            // create set will all votes expanded
            let mut expanded = BTreeSet::new();
            for vote_range in key_votes {
                for vote in vote_range.votes() {
                    // insert vote and check it hasn't been added before
                    expanded.insert(vote);
                }
            }

            // check that we have all votes (i.e. we don't have gaps that would
            // prevent timestamp-stability)
            let vote_count = expanded.len();
            // we should have all votes from 1 to `vote_cound`
            assert_eq!(
                expanded,
                BTreeSet::from_iter((1..=vote_count).map(|vote| vote as u64))
            );
        }
    }

    fn worker(
        mut clocks: AtomicKeyClocks,
        ops_number: usize,
        max_keys_per_command: usize,
        keys_number: usize,
    ) -> Votes {
        // all votes worker has generated
        let mut all_votes = Votes::new(None);

        // highest clock seen
        let mut highest = 0;

        for _ in 0..ops_number {
            // TODO increase noop probability
            let noop_probability = 0;
            let cmd = util::gen_cmd(
                max_keys_per_command,
                keys_number,
                noop_probability,
            )
            .expect(
                "command shouldn't be a noop since the noop probability is 0",
            );
            // get votes
            let (new_highest, votes) = clocks.bump_and_vote(&cmd, highest);
            // update highest
            highest = new_highest;
            // save votes
            all_votes.merge(votes);
        }

        all_votes
    }
}
