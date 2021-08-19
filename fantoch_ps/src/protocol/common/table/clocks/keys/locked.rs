use super::KeyClocks;
use crate::protocol::common::table::{VoteRange, Votes};
use fantoch::command::Command;
use fantoch::id::{ProcessId, Rifl, ShardId};
use fantoch::kvs::Key;
use fantoch::shared::SharedMap;
use fantoch::HashSet;
use parking_lot::Mutex;
use std::cmp;
use std::collections::BTreeSet;
use std::iter::FromIterator;
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
struct ClockAndPendingReads {
    clock: u64,
    pending_reads: HashSet<Rifl>,
}
// all clock's are protected by a mutex
type Clocks = Arc<SharedMap<Key, Mutex<ClockAndPendingReads>>>;

/// `bump_and_vote` grabs all locks before any change
#[derive(Debug, Clone)]
pub struct LockedKeyClocks {
    process_id: ProcessId,
    shard_id: ShardId,
    nfr: bool,
    clocks: Clocks,
}

impl KeyClocks for LockedKeyClocks {
    /// Create a new `LockedKeyClocks` instance.
    fn new(process_id: ProcessId, shard_id: ShardId, nfr: bool) -> Self {
        Self {
            process_id,
            shard_id,
            nfr,
            clocks: common::new(),
        }
    }

    fn init_clocks(&mut self, cmd: &Command) {
        common::init_clocks(self.shard_id, &self.clocks, cmd)
    }

    fn proposal(&mut self, cmd: &Command, min_clock: u64) -> (u64, Votes) {
        // make sure locks will be acquired in some pre-determined order to
        // avoid deadlocks
        let keys = BTreeSet::from_iter(cmd.keys(self.shard_id));
        let key_count = keys.len();
        // find all the locks
        // - NOTE that the following loop and the one below cannot be merged due
        //   to lifetimes: `let guard = key_lock.lock()` borrows `key_lock` and
        //   the borrow checker doesn't not understand that it's fine to move
        //   both the `guard` and `key_lock` into e.g. a `Vec`. For that reason,
        //   we have two loops. One that fetches the locks (the following one)
        //   and another one (the one below it ) that actually acquires the
        //   locks.
        let mut locks = Vec::with_capacity(key_count);
        self.clocks
            .get_or_all(&keys, &mut locks, || Mutex::default());

        if cmd.read_only() {
            // if the command is read-only, the simply read the current clock
            // value
            // TODO: reads have to be processed differently as this violates
            //       linearizability
            let mut clock = min_clock;
            for (_key, key_lock) in &locks {
                let guard = key_lock.lock();
                clock = cmp::max(clock, guard.clock);
            }
            (clock, Votes::new())
        } else {
            // keep track of which clock we should bump to
            let mut up_to = min_clock;

            // acquire the lock on all keys
            let mut guards = Vec::with_capacity(key_count);
            for (_key, key_lock) in &locks {
                let guard = key_lock.lock();
                up_to = cmp::max(up_to, guard.clock + 1);
                guards.push(guard);
            }

            // create votes
            let mut votes = Votes::with_capacity(key_count);
            for entry in locks.iter().zip(guards.into_iter()) {
                let (key, _key_lock) = entry.0;
                let mut guard = entry.1;
                common::maybe_bump(
                    self.process_id,
                    key,
                    &mut guard.clock,
                    up_to,
                    &mut votes,
                );
                // release the lock
                drop(guard);
            }
            (up_to, votes)
        }
    }

    fn detached(&mut self, cmd: &Command, up_to: u64, votes: &mut Votes) {
        common::detached(
            self.process_id,
            self.shard_id,
            &self.clocks,
            cmd,
            up_to,
            votes,
        )
    }

    fn detached_all(&mut self, up_to: u64, votes: &mut Votes) {
        common::detached_all(self.process_id, &self.clocks, up_to, votes)
    }

    fn parallel() -> bool {
        true
    }
}

mod common {
    use super::*;

    pub(super) fn new() -> Clocks {
        // create shared clocks
        let clocks = SharedMap::new();
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
            let _ = clocks.get_or(key, || Mutex::default());
        })
    }

    pub(super) fn detached(
        id: ProcessId,
        shard_id: ShardId,
        clocks: &Clocks,
        cmd: &Command,
        up_to: u64,
        votes: &mut Votes,
    ) {
        for key in cmd.keys(shard_id) {
            let key_lock = clocks.get_or(key, || Mutex::default());
            let mut guard = key_lock.lock();
            maybe_bump(id, key, &mut guard.clock, up_to, votes);
            // release the lock
            drop(guard);
        }
    }

    pub(super) fn detached_all(
        id: ProcessId,
        clocks: &Clocks,
        up_to: u64,
        votes: &mut Votes,
    ) {
        clocks.iter().for_each(|entry| {
            let key = entry.key();
            let key_lock = entry.value();
            let mut guard = key_lock.lock();
            maybe_bump(id, key, &mut guard.clock, up_to, votes);
            // release the lock
            drop(guard);
        });
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
            votes.add(key, vr);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fantoch::kvs::KVOp;

    #[test]
    fn bump_test() {
        let process_id = 1;
        let shard_id = 0;
        let nfr = false;
        let mut clocks = LockedKeyClocks::new(process_id, shard_id, nfr);

        // create rifl
        let client_id = 1;
        let rifl = Rifl::new(client_id, 1);

        // read-only commmands do not bump clocks
        let ro_cmd = Command::from(rifl, vec![(String::from("K"), KVOp::Get)]);
        let (clock, votes) = clocks.proposal(&ro_cmd, 0);
        assert_eq!(clock, 0);
        assert!(votes.is_empty());

        // update command bump the clock
        let cmd = Command::from(
            rifl,
            vec![(String::from("K"), KVOp::Put(String::new()))],
        );
        let (clock, votes) = clocks.proposal(&cmd, 0);
        assert_eq!(clock, 1);
        assert!(!votes.is_empty());

        // read-only commmands do not bump clocks
        let ro_cmd = Command::from(rifl, vec![(String::from("K"), KVOp::Get)]);
        let (clock, votes) = clocks.proposal(&ro_cmd, 0);
        assert_eq!(clock, 1);
        assert!(votes.is_empty());
    }
}
