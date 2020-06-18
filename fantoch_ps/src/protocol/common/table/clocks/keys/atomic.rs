use super::KeyClocks;
use crate::protocol::common::shared::Shared;
use crate::protocol::common::table::{VoteRange, Votes};
use fantoch::command::Command;
use fantoch::id::ProcessId;
use fantoch::kvs::Key;
use std::cmp;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct AtomicKeyClocks {
    id: ProcessId,
    clocks: Arc<Shared<AtomicU64>>,
}

impl KeyClocks for AtomicKeyClocks {
    /// Create a new `AtomicKeyClocks` instance.
    fn new(id: ProcessId) -> Self {
        // create shared clocks
        let clocks = Shared::new();
        // wrap them in an arc
        let clocks = Arc::new(clocks);

        Self { id, clocks }
    }

    fn init_clocks(&mut self, cmd: &Command) {
        cmd.keys().for_each(|key| {
            // get initializes the key to the default value, and that's exactly
            // what we want
            let _ = self.clocks.get(key);
        });
    }

    fn bump_and_vote(&mut self, cmd: &Command, min_clock: u64) -> (u64, Votes) {
        // single round of votes:
        // - vote on each key and compute the highest clock seen
        // - this means that if we have more than one key, then we don't
        //   necessarily end up with all key clocks equal
        let mut votes = Votes::with_capacity(cmd.key_count());
        let highest = cmd
            .keys()
            .map(|key| {
                // bump the `key` clock
                let previous_value = self.bump_key(key, min_clock);
                // compute vote start and vote end
                let vote_start = previous_value + 1;
                let vote_end = cmp::max(min_clock, previous_value + 1);

                // create vote range and save it
                let vr = VoteRange::new(self.id, vote_start, vote_end);
                votes.set(key.clone(), vec![vr]);

                // return vote end
                vote_end
            })
            .max()
            .expect("there should be a maximum sequence");
        (highest, votes)
    }

    fn vote(&mut self, cmd: &Command, up_to: u64) -> Votes {
        let key_count = cmd.key_count();
        let keys = cmd.keys().cloned();
        self.maybe_bump_keys(keys, key_count, up_to)
    }

    fn vote_all(
        &mut self,
        window: usize,
        total_windows: usize,
        up_to: u64,
    ) -> Votes {
        let key_count = self.clocks.len();
        let (to_skip, to_take) =
            super::window_bounds(window, total_windows, key_count);

        // create votes
        let mut votes = Votes::with_capacity(key_count);

        self.clocks
            .iter()
            .skip(to_skip)
            .take(to_take)
            .for_each(|entry| {
                let clock = entry.value();
                if let Some(vote_range) =
                    Self::maybe_bump(self.id, clock, up_to)
                {
                    votes.set(entry.key().clone(), vec![vote_range]);
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
    fn bump_key(&self, key: &Key, min_clock: u64) -> u64 {
        let clock = self.clocks.get(key);
        Self::bump(&clock, min_clock)
    }

    // Bump the `key` clock to `up_to` if lower than `up_to`.
    fn maybe_bump_key(&self, key: &Key, up_to: u64) -> Option<VoteRange> {
        let clock = self.clocks.get(key);
        Self::maybe_bump(self.id, &clock, up_to)
    }

    fn maybe_bump_keys<I>(&self, keys: I, key_count: usize, up_to: u64) -> Votes
    where
        I: Iterator<Item = Key>,
    {
        // create votes
        let mut votes = Votes::with_capacity(key_count);

        keys.for_each(|key| {
            if let Some(vote_range) = self.maybe_bump_key(&key, up_to) {
                votes.set(key, vec![vote_range]);
            }
        });

        votes
    }

    // Bump the clock to at least `min_clock`.
    fn bump(clock: &AtomicU64, min_clock: u64) -> u64 {
        clock
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(cmp::max(min_clock, value + 1))
            })
            .expect("atomic bump should always succeed")
    }

    // Bump the clock to `up_to` if lower than `up_to`.
    fn maybe_bump(
        id: ProcessId,
        clock: &AtomicU64,
        up_to: u64,
    ) -> Option<VoteRange> {
        clock
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                if value < up_to {
                    Some(up_to)
                } else {
                    None
                }
            })
            .ok()
            .map(|previous_value| {
                // compute vote start and vote end
                let vote_start = previous_value + 1;
                let vote_end = up_to;

                // create second vote range and save it
                VoteRange::new(id, vote_start, vote_end)
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util;
    use std::collections::BTreeSet;
    use std::iter::FromIterator;
    use std::thread;

    #[test]
    fn atomic_clocks_test() {
        let nthreads = 2;
        let ops_number = 10000;
        let max_keys_per_command = 2;
        let keys_number = 4;
        for _ in 0..100 {
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
        let mut all_votes = Votes::new();
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
        let mut all_votes = Votes::new();

        // highest clock seen
        let mut highest = 0;

        for _ in 0..ops_number {
            // there are no noop's
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
