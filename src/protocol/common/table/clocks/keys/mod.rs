// This module contains the definition of `SequentialKeyClocks`.
mod sequential;

// This module contains the definition of `AtomicKeyClocks`.
mod atomic;

// Re-exports.
pub use atomic::AtomicKeyClocks;
pub use sequential::SequentialKeyClocks;

use crate::command::Command;
use crate::id::ProcessId;
use crate::kvs::Key;
use crate::protocol::common::table::Votes;
use dashmap::mapref::one::Ref;
use dashmap::DashMap;
use parking_lot::Mutex;
use std::sync::Arc;

pub trait KeyClocks: Clone {
    /// Create a new `KeyClocks` instance given the local process identifier.
    fn new(id: ProcessId) -> Self;

    /// Bump clocks to at least `min_clock` and return the new clock (that might
    /// be `min_clock` in case it was higher than any of the local clocks). Also
    /// returns the consumed votes.
    fn bump_and_vote(&mut self, cmd: &Command, min_clock: u64) -> (u64, Votes);

    /// Votes up to `clock` and returns the consumed votes.
    fn vote(&mut self, cmd: &Command, clock: u64) -> Votes;

    fn parallel() -> bool;
}

#[derive(Clone)]
struct SharedClocks<V> {
    insert_lock: Arc<Mutex<()>>,
    clocks: DashMap<Key, V>,
}

impl<V> SharedClocks<V>
where
    V: Default,
{
    // Function to be used by the implementors of `KeyClocks` to create their
    // clocks.
    fn new() -> Self {
        // create insert lock
        let insert_lock = Arc::new(Mutex::new(()));
        // create clocks
        let clocks = DashMap::new();
        Self {
            insert_lock,
            clocks,
        }
    }

    fn get(&self, key: &Key) -> Ref<Key, V> {
        match self.clocks.get(key) {
            Some(value) => value,
            None => {
                self.maybe_insert(key);
                self.get(key)
            }
        }
    }

    fn maybe_insert(&self, key: &Key) {
        // acquire the write lock
        let _lock = self.insert_lock.lock();
        // insert entry if it doesn't yet exist:
        // - maybe another thread tried to `maybe_insert` and was able to insert
        //   before us
        self.clocks.entry(key.clone()).or_default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::Rifl;
    use crate::kvs::Key;

    #[test]
    fn sequential_key_clocks() {
        keys_clocks_flow::<SequentialKeyClocks>();
        keys_clocks_no_double_votes::<SequentialKeyClocks>();
    }

    #[test]
    fn atomic_key_clocks() {
        keys_clocks_flow::<AtomicKeyClocks>();
        keys_clocks_no_double_votes::<AtomicKeyClocks>();
    }

    fn keys_clocks_flow<KC: KeyClocks>() {
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
        assert_eq!(get_key_votes(&key_b, &process_votes), vec![1, 2, 3]);

        // -------------------------
        // first clock and votes for command b
        let (clock, process_votes) = clocks.bump_and_vote(&cmd_b, 0);
        assert_eq!(clock, 4);
        assert_eq!(process_votes.len(), 1); // single key
        assert_eq!(get_key_votes(&key_b, &process_votes), vec![4]);
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
        // check that there's only one vote
        assert_eq!(ranges.len(), 1);
        let start = ranges[0].start();
        let end = ranges[0].end();
        (start..=end).collect()
    }
}
