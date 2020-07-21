use super::KeyClocks;
use crate::protocol::common::shared::Shared;
use crate::protocol::common::table::{VoteRange, Votes};
use fantoch::command::Command;
use fantoch::id::{ProcessId, ShardId};
use fantoch::kvs::Key;
use parking_lot::Mutex;
use std::cmp;
use std::collections::BTreeSet;
use std::iter::FromIterator;
use std::sync::Arc;

// all clock's are protected by a mutex
type Clock = Mutex<u64>;
type Clocks = Arc<Shared<Clock>>;

/// `bump_and_vote` grabs all locks before any change
#[derive(Debug, Clone)]
pub struct LockedKeyClocks {
    id: ProcessId,
    shard_id: ShardId,
    clocks: Clocks,
}

/// `bump_and_vote` grabs one lock at a time
#[derive(Debug, Clone)]
pub struct FineLockedKeyClocks {
    id: ProcessId,
    shard_id: ShardId,
    clocks: Clocks,
}

impl KeyClocks for LockedKeyClocks {
    /// Create a new `LockedKeyClocks` instance.
    fn new(id: ProcessId, shard_id: ShardId) -> Self {
        Self {
            id,
            shard_id,
            clocks: common::new(),
        }
    }

    fn init_clocks(&mut self, cmd: &Command) {
        common::init_clocks(self.shard_id, &self.clocks, cmd)
    }

    fn bump_and_vote(&mut self, cmd: &Command, min_clock: u64) -> (u64, Votes) {
        // make sure locks will be acquired in some pre-determined order to
        // avoid deadlocks
        let keys = BTreeSet::from_iter(cmd.keys(self.shard_id));
        let key_count = keys.len();
        // find all the locks
        let mut locks = Vec::with_capacity(key_count);
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
        let mut guards = Vec::with_capacity(key_count);
        for (_key, key_lock) in &locks {
            let guard = key_lock.lock();
            up_to = cmp::max(up_to, *guard + 1);
            guards.push(guard);
        }

        // create votes
        let mut votes = Votes::with_capacity(key_count);
        for entry in locks.iter().zip(guards.into_iter()) {
            let (key, _key_lock) = entry.0;
            let mut guard = entry.1;
            common::maybe_bump(self.id, key, &mut guard, up_to, &mut votes);
            // release the lock
            drop(guard);
        }
        (up_to, votes)
    }

    fn vote(&mut self, cmd: &Command, up_to: u64) -> Votes {
        common::vote(self.id, self.shard_id, &self.clocks, cmd, up_to)
    }

    fn vote_all(&mut self, up_to: u64) -> Votes {
        common::vote_all(self.id, &self.clocks, up_to)
    }

    fn parallel() -> bool {
        true
    }
}

impl KeyClocks for FineLockedKeyClocks {
    /// Create a new `FineLockedKeyClocks` instance.
    fn new(id: ProcessId, shard_id: ShardId) -> Self {
        Self {
            id,
            shard_id,
            clocks: common::new(),
        }
    }

    fn init_clocks(&mut self, cmd: &Command) {
        common::init_clocks(self.shard_id, &self.clocks, cmd)
    }

    fn bump_and_vote(&mut self, cmd: &Command, min_clock: u64) -> (u64, Votes) {
        // single round of votes:
        // - vote on each key and compute the highest clock seen
        // - this means that if we have more than one key, then we don't
        //   necessarily end up with all key clocks equal
        let keys: Vec<_> = cmd.keys(self.shard_id).collect();
        let key_count = keys.len();
        let mut votes = Votes::with_capacity(key_count);
        let highest = keys
            .into_iter()
            .map(|key| {
                let key_lock = self.clocks.get(key);
                let mut guard = key_lock.lock();
                let previous_value = *guard;
                let current_value = cmp::max(min_clock, previous_value + 1);
                *guard = current_value;
                // drop the lock
                drop(guard);

                // create vote range
                let vr =
                    VoteRange::new(self.id, previous_value + 1, current_value);
                votes.set(key.clone(), vec![vr]);

                // return "current" clock value
                current_value
            })
            .max()
            .expect("there should be a maximum sequence");
        (highest, votes)
    }

    fn vote(&mut self, cmd: &Command, up_to: u64) -> Votes {
        common::vote(self.id, self.shard_id, &self.clocks, cmd, up_to)
    }

    fn vote_all(&mut self, up_to: u64) -> Votes {
        common::vote_all(self.id, &self.clocks, up_to)
    }

    fn parallel() -> bool {
        true
    }
}

mod common {
    use super::*;

    pub(super) fn new() -> Clocks {
        // create shared clocks
        let clocks = Shared::new();
        // wrap them in an arc
        Arc::new(clocks)
    }

    pub(super) fn init_clocks(
        shard_id: ShardId,
        clocks: &Clocks,
        cmd: &Command,
    ) {
        cmd.keys(shard_id).for_each(|key| {
            // get initializes the key to the default value, and that's exactly
            // what we want
            let _ = clocks.get(key);
        })
    }

    pub(super) fn vote(
        id: ProcessId,
        shard_id: ShardId,
        clocks: &Clocks,
        cmd: &Command,
        up_to: u64,
    ) -> Votes {
        let keys: Vec<_> = cmd.keys(shard_id).collect();
        let key_count = keys.len();
        // create votes
        let mut votes = Votes::with_capacity(key_count);
        for key in keys {
            let key_lock = clocks.get(key);
            let mut guard = key_lock.lock();
            maybe_bump(id, key, &mut guard, up_to, &mut votes);
            // release the lock
            drop(guard);
        }
        votes
    }

    pub(super) fn vote_all(
        id: ProcessId,
        clocks: &Clocks,
        up_to: u64,
    ) -> Votes {
        let key_count = clocks.len();
        // create votes
        let mut votes = Votes::with_capacity(key_count);

        clocks.iter().for_each(|entry| {
            let key = entry.key();
            let key_lock = entry.value();
            let mut guard = key_lock.lock();
            maybe_bump(id, key, &mut guard, up_to, &mut votes);
            // release the lock
            drop(guard);
        });

        votes
    }

    pub(super) fn maybe_bump(
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
