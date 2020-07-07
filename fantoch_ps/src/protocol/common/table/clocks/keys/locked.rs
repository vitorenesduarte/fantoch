use super::KeyClocks;
use crate::protocol::common::shared::Shared;
use crate::protocol::common::table::Votes;
use fantoch::command::Command;
use fantoch::id::ProcessId;
use parking_lot::RwLock;
use std::sync::Arc;

// all clock's are protected by a rw-lock
type Clock = RwLock<u64>;

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
        todo!()
    }

    fn vote(&mut self, cmd: &Command, up_to: u64) -> Votes {
        todo!()
    }

    fn vote_all(&mut self, up_to: u64) -> Votes {
        todo!()
    }

    fn parallel() -> bool {
        true
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
