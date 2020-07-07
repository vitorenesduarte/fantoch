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
                let clock = self.clocks.get(key);
                let previous_value = Self::bump(&clock, min_clock);

                // create vote range and save it
                let current_value = cmp::max(min_clock, previous_value + 1);
                let vr = VoteRange::new(self.id, previous_value + 1, current_value);
                votes.set(key.clone(), vec![vr]);

                // return "current" clock value
                current_value
            })
            .max()
            .expect("there should be a maximum sequence");
        (highest, votes)
    }

    fn vote(&mut self, cmd: &Command, up_to: u64) -> Votes {
        // create votes
        let mut votes = Votes::with_capacity(cmd.key_count());
        for key in cmd.keys() {
            let clock = self.clocks.get(key);
            Self::maybe_bump(self.id, key, &clock, up_to, &mut votes);
        }
        votes
    }

    fn vote_all(&mut self, up_to: u64) -> Votes {
        let key_count = self.clocks.len();
        // create votes
        let mut votes = Votes::with_capacity(key_count);

        self.clocks.iter().for_each(|entry| {
            let key = entry.key();
            let clock = entry.value();
            Self::maybe_bump(self.id, key, &clock, up_to, &mut votes);
        });

        votes
    }

    fn parallel() -> bool {
        true
    }
}

impl AtomicKeyClocks {
    // Bump the clock to at least `min_clock`.
    fn bump(clock: &AtomicU64, min_clock: u64) -> u64 {
        let fetch_update =
            clock.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(cmp::max(min_clock, value + 1))
            });
        match fetch_update {
            Ok(previous_value) => previous_value,
            Err(_) => {
                panic!("atomic bump should always succeed");
            }
        }
    }

    // Bump the clock to `up_to` if lower than `up_to`.
    fn maybe_bump(
        id: ProcessId,
        key: &Key,
        clock: &AtomicU64,
        up_to: u64,
        votes: &mut Votes,
    ) {
        let fetch_update =
            clock.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                if value < up_to {
                    Some(up_to)
                } else {
                    None
                }
            });
        if let Ok(previous_value) = fetch_update {
            let vr = VoteRange::new(id, previous_value + 1, up_to);
            votes.set(key.clone(), vec![vr]);
        }
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
