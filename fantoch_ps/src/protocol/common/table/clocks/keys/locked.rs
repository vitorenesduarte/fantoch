use super::KeyClocks;
use crate::protocol::common::shared::Shared;
use crate::protocol::common::table::{VoteRange, Votes};
use fantoch::command::Command;
use fantoch::id::ProcessId;
use fantoch::kvs::Key;
use parking_lot::Mutex;
use std::cmp;
use std::collections::BTreeSet;
use std::iter::FromIterator;
use std::sync::Arc;

// all clock's are protected by a mutex
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
        // make sure locks will be acquired in some pre-determined order to
        // avoid deadlocks
        let keys = BTreeSet::from_iter(cmd.keys());
        self.clocks.get_all(&keys, &mut locks);

        // keep track of which clock we should bump to
        let mut up_to = min_clock;

        // acquire the lock on all keys
        // - NOTE that this loop and the above cannot be merged due to
        //   lifetimes: `let guard = key_lock.lock()` borrows `key_lock` and the
        //   borrow checker doesn't not understand that it's fine to move both
        //   the `guard` and `key_lock` into e.g. a `Vec`. For that reason, we
        //   have two loops. One that fetches the locks and another one (the one
        //   that follows) that actually acquires the locks.
        let mut guards = Vec::with_capacity(cmd.key_count());
        for (_key, key_lock) in &locks {
            let guard = key_lock.lock();
            up_to = cmp::max(up_to, *guard + 1);
            guards.push(guard);
        }

        // create votes
        let mut votes = Votes::with_capacity(cmd.key_count());
        for entry in locks.iter().zip(guards.into_iter()) {
            let (key, _key_lock) = entry.0;
            let mut guard = entry.1;
            Self::maybe_bump(self.id, key, &mut guard, up_to, &mut votes);
            // release the lock
            drop(guard);
        }
        (up_to, votes)
    }

    fn vote(&mut self, cmd: &Command, up_to: u64) -> Votes {
        // create votes
        let mut votes = Votes::with_capacity(cmd.key_count());
        for key in cmd.keys() {
            let key_lock = self.clocks.get(key);
            let mut guard = key_lock.lock();
            Self::maybe_bump(self.id, key, &mut guard, up_to, &mut votes);
            // release the lock
            drop(guard);
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
            let mut guard = key_lock.lock();
            Self::maybe_bump(self.id, key, &mut guard, up_to, &mut votes);
            // release the lock
            drop(guard);
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
