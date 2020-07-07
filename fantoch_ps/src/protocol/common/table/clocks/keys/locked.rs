use super::KeyClocks;
use crate::protocol::common::shared::Shared;
use crate::protocol::common::table::{VoteRange, Votes};
use fantoch::command::Command;
use fantoch::id::ProcessId;
use fantoch::kvs::Key;
use parking_lot::Mutex;
use std::cmp;
use std::sync::Arc;

// all clock's are protected by a rw-lock
type Clock = Mutex<u64>;

#[derive(Debug, Clone)]
pub struct LockedKeyClocks {
    id: ProcessId,
    clocks: Arc<Shared<Clock>>,
}

impl KeyClocks for LockedKeyClocks {
    /// Create a new `LockedKeyClocks` instance.
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
        })
    }

    fn bump_and_vote(&mut self, cmd: &Command, min_clock: u64) -> (u64, Votes) {
        // find all the locks
        let mut locks = Vec::with_capacity(cmd.key_count());
        for key in cmd.keys() {
            let key_lock = self.clocks.get(key);
            locks.push((key, key_lock));
        }

        // keep track of which clock we should bump to
        let mut up_to = min_clock;

        // acquire the lock on all keys (we won't deadlock since `cmd.keys()`
        // will return them sorted)
        // - NOTE that this loop and the above cannot be merged due to
        //   lifetimes: `let guard = key_lock.write()` borrows `key_lock` and
        //   the borrow checker doesn't not understand that it's fine to move
        //   both the `guard` and `key_lock` into a `Vec`. For that reason, we
        //   have two loops. One that fetches the locks and another one (the one
        //   that follows) that actually acquires the locks.
        let mut guards = Vec::with_capacity(cmd.key_count());
        for (_, key_lock) in &locks {
            let guard = key_lock.lock();
            up_to = cmp::max(up_to, *guard + 1);
            guards.push(guard);
        }

        // create votes
        let mut votes = Votes::with_capacity(cmd.key_count());
        for entry in locks.iter().zip(guards.iter_mut()) {
            // the following two lines are awkward but the compiler is
            // complaining if I try to match in the for loop
            let (key, key_lock) = entry.0;
            let guard = entry.1;
            Self::maybe_bump(self.id, key, guard, up_to, &mut votes);
            // release the lock
            drop(guard);
            drop(key_lock);
        }
        (up_to, votes)
    }

    fn vote(&mut self, cmd: &Command, up_to: u64) -> Votes {
        // create votes
        let mut votes = Votes::with_capacity(cmd.key_count());
        for key in cmd.keys() {
            let key_lock = self.clocks.get(key);
            let mut current = key_lock.lock();
            Self::maybe_bump(self.id, key, &mut current, up_to, &mut votes);
            // release the lock
            drop(current);
            drop(key_lock);
        }
        votes
    }

    fn vote_all(&mut self, up_to: u64) -> Votes {
        let key_count = self.clocks.len();
        // create votes
        let mut votes = Votes::with_capacity(key_count);

        self.clocks.iter().for_each(|entry| {
            let key = entry.key();
            let key_lock = entry.value();
            let mut current = key_lock.lock();
            Self::maybe_bump(self.id, key, &mut current, up_to, &mut votes);
            // release the lock
            drop(current);
            drop(key_lock);
        });

        votes
    }

    fn parallel() -> bool {
        true
    }
}

impl LockedKeyClocks {
    fn maybe_bump(
        id: ProcessId,
        key: &Key,
        current: &mut u64,
        up_to: u64,
        votes: &mut Votes,
    ) {
        // if we should vote
        if *current < up_to {
            // vote from the current clock value + 1 until `up_to`
            let vr = VoteRange::new(id, *current + 1, up_to);
            // update current clock to be `clock`
            *current = up_to;
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
    fn locked_clocks_test() {
        let nthreads = 2;
        let ops_number = 10000;
        let max_keys_per_command = 2;
        let keys_number = 4;
        for _ in 0..10 {
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
        let clocks = LockedKeyClocks::new(process_id);

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
        mut clocks: LockedKeyClocks,
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
