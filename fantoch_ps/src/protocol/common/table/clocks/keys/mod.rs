// This module contains the definition of `SequentialKeyClocks`.
mod sequential;

// This module contains the definition of `AtomicKeyClocks`.
mod atomic;

// This module contains the definition of `LockedKeyClocks`.
mod locked;

// Re-exports.
pub use atomic::AtomicKeyClocks;
pub use locked::FineLockedKeyClocks;
pub use locked::LockedKeyClocks;
pub use sequential::SequentialKeyClocks;

use crate::protocol::common::table::Votes;
use fantoch::command::Command;
use fantoch::id::ProcessId;
use std::fmt::Debug;

pub trait KeyClocks: Debug + Clone {
    /// Create a new `KeyClocks` instance given the local process identifier.
    fn new(id: ProcessId) -> Self;

    /// Makes sure there's a clock for each key in the command.
    fn init_clocks(&mut self, cmd: &Command);

    /// Bump clocks to at least `min_clock` and return the new clock (that might
    /// be `min_clock` in case it was higher than any of the local clocks). Also
    /// returns the consumed votes.
    fn bump_and_vote(&mut self, cmd: &Command, min_clock: u64) -> (u64, Votes);

    /// Votes up to `clock` and returns the consumed votes.
    fn vote(&mut self, cmd: &Command, clock: u64) -> Votes;

    /// Votes up to `clock` on all keys and returns the consumed votes.
    fn vote_all(&mut self, clock: u64) -> Votes;

    fn parallel() -> bool;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util;
    use fantoch::id::Rifl;
    use fantoch::kvs::Key;
    use std::collections::BTreeSet;
    use std::iter::FromIterator;
    use std::thread;

    #[test]
    fn sequential_key_clocks() {
        keys_clocks_flow::<SequentialKeyClocks>(true);
        keys_clocks_no_double_votes::<SequentialKeyClocks>();
    }

    #[test]
    fn atomic_key_clocks() {
        keys_clocks_flow::<AtomicKeyClocks>(true);
        keys_clocks_no_double_votes::<AtomicKeyClocks>();
    }

    #[test]
    fn locked_key_clocks() {
        keys_clocks_flow::<LockedKeyClocks>(true);
        keys_clocks_no_double_votes::<LockedKeyClocks>();
    }

    #[test]
    fn fine_locked_key_clocks() {
        keys_clocks_flow::<FineLockedKeyClocks>(false);
        keys_clocks_no_double_votes::<FineLockedKeyClocks>();
    }

    #[test]
    fn concurrent_atomic_key_clocks() {
        let nthreads = 2;
        let ops_number = 10000;
        let max_keys_per_command = 2;
        let keys_number = 4;
        for _ in 0..100 {
            concurrent_test::<AtomicKeyClocks>(
                nthreads,
                ops_number,
                max_keys_per_command,
                keys_number,
            );
        }
    }

    #[test]
    fn concurrent_locked_key_clocks() {
        let nthreads = 2;
        let ops_number = 10000;
        let max_keys_per_command = 2;
        let keys_number = 4;
        for _ in 0..100 {
            concurrent_test::<LockedKeyClocks>(
                nthreads,
                ops_number,
                max_keys_per_command,
                keys_number,
            );
        }
    }

    fn keys_clocks_flow<KC: KeyClocks>(all_clocks_match: bool) {
        // create key clocks
        let mut clocks = KC::new(1);

        // keys
        let key_a = String::from("A");
        let key_b = String::from("B");

        // command a
        let cmd_a_rifl = Rifl::new(100, 1); // client 100, 1st op
        let cmd_a = Command::get(cmd_a_rifl, key_a.clone());

        // command b
        let cmd_b_rifl = Rifl::new(101, 1); // client 101, 1st op
        let cmd_b = Command::get(cmd_b_rifl, key_b.clone());

        // command ab
        let cmd_ab_rifl = Rifl::new(102, 1); // client 102, 1st op
        let cmd_ab =
            Command::multi_get(cmd_ab_rifl, vec![key_a.clone(), key_b.clone()]);

        // -------------------------
        // first clock and votes for command a
        let (clock, process_votes) = clocks.bump_and_vote(&cmd_a, 0);
        assert_eq!(clock, 1);
        assert_eq!(process_votes.len(), 1); // single key
        assert_eq!(get_key_votes(&key_a, &process_votes), vec![1]);

        // -------------------------
        // second clock and votes for command a
        let (clock, process_votes) = clocks.bump_and_vote(&cmd_a, 0);
        assert_eq!(clock, 2);
        assert_eq!(process_votes.len(), 1); // single key
        assert_eq!(get_key_votes(&key_a, &process_votes), vec![2]);

        // -------------------------
        // first clock and votes for command ab
        let (clock, process_votes) = clocks.bump_and_vote(&cmd_ab, 0);
        assert_eq!(clock, 3);
        assert_eq!(process_votes.len(), 2); // two keys
        assert_eq!(get_key_votes(&key_a, &process_votes), vec![3]);
        let key_votes = get_key_votes(&key_b, &process_votes);
        let mut which = 0;
        if all_clocks_match {
            assert_eq!(key_votes, vec![1, 2, 3]);
        } else {
            // NOTE it's possible that, even though not all clocks values have
            // to match, they may match; this happens when the highest clock (of
            // all keys being accessed) happens to be the first one to be
            // iterated; this is not deterministic since we iterate keys in
            // their HashMap order, which is not a "stable"
            match key_votes.as_slice() {
                [1] => which = 1,
                [1, 2, 3] => which = 123,
                _ => panic!("unexpected key votes vote: {:?}", key_votes),
            }
        }

        // -------------------------
        // first clock and votes for command b
        let (clock, process_votes) = clocks.bump_and_vote(&cmd_b, 0);
        if all_clocks_match {
            assert_eq!(clock, 4);
        } else {
            match which {
                1 => assert_eq!(clock, 2),
                123 => assert_eq!(clock, 4),
                _ => unreachable!("impossible 'which' value: {}", which),
            }
        }
        assert_eq!(process_votes.len(), 1); // single key
        let key_votes = get_key_votes(&key_b, &process_votes);
        if all_clocks_match {
            assert_eq!(key_votes, vec![4]);
        } else {
            match which {
                1 => assert_eq!(key_votes, vec![2]),
                123 => assert_eq!(key_votes, vec![4]),
                _ => unreachable!("impossible 'which' value: {}", which),
            }
        }
    }

    fn keys_clocks_no_double_votes<KC: KeyClocks>() {
        // create key clocks
        let mut clocks = KC::new(1);

        // command
        let key = String::from("A");
        let cmd_rifl = Rifl::new(100, 1);
        let cmd = Command::get(cmd_rifl, key.clone());

        // get process votes up to 5
        let process_votes = clocks.vote(&cmd, 5);
        assert_eq!(process_votes.len(), 1); // single key
        assert_eq!(get_key_votes(&key, &process_votes), vec![1, 2, 3, 4, 5]);

        // get process votes up to 5 again: should get no votes
        let process_votes = clocks.vote(&cmd, 5);
        assert!(process_votes.is_empty());

        // get process votes up to 6
        let process_votes = clocks.vote(&cmd, 6);
        assert_eq!(process_votes.len(), 1); // single key
        assert_eq!(get_key_votes(&key, &process_votes), vec![6]);

        // get process votes up to 2: should get no votes
        let process_votes = clocks.vote(&cmd, 2);
        assert!(process_votes.is_empty());

        // get process votes up to 3: should get no votes
        let process_votes = clocks.vote(&cmd, 3);
        assert!(process_votes.is_empty());

        // get process votes up to 10
        let process_votes = clocks.vote(&cmd, 10);
        assert_eq!(process_votes.len(), 1); // single key
        assert_eq!(get_key_votes(&key, &process_votes), vec![7, 8, 9, 10]);
    }

    // Returns the list of votes on some key.
    fn get_key_votes(key: &Key, votes: &Votes) -> Vec<u64> {
        let ranges = votes
            .get(key)
            .expect("process should have voted on this key");
        ranges
            .into_iter()
            .flat_map(|range| range.start()..=range.end())
            .collect()
    }

    fn concurrent_test<K: KeyClocks + Send + Sync + 'static>(
        nthreads: usize,
        ops_number: usize,
        max_keys_per_command: usize,
        keys_number: usize,
    ) {
        // create clocks
        let process_id = 1;
        let clocks = K::new(process_id);

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

    fn worker<K: KeyClocks>(
        mut clocks: K,
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
