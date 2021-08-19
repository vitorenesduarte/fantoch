use super::KeyClocks;
use crate::protocol::common::table::{VoteRange, Votes};
use fantoch::command::Command;
use fantoch::id::{ProcessId, ShardId};
use fantoch::kvs::Key;
use fantoch::HashMap;
use std::cmp;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequentialKeyClocks {
    process_id: ProcessId,
    shard_id: ShardId,
    nfr: bool,
    clocks: HashMap<Key, u64>,
}

impl KeyClocks for SequentialKeyClocks {
    /// Create a new `SequentialKeyClocks` instance.
    fn new(process_id: ProcessId, shard_id: ShardId, nfr: bool) -> Self {
        let clocks = HashMap::new();
        Self {
            process_id,
            nfr,
            shard_id,
            clocks,
        }
    }

    fn init_clocks(&mut self, cmd: &Command) {
        cmd.keys(self.shard_id).for_each(|key| {
            // create entry if key not present yet
            if !self.clocks.contains_key(key) {
                self.clocks.insert(key.clone(), 0);
            }
        });
    }

    fn proposal(&mut self, cmd: &Command, min_clock: u64) -> (u64, Votes) {
        // if NFR with a read-only single-key command, then don't bump the clock
        let should_not_bump = self.nfr && cmd.nfr_allowed();
        let next_clock = if should_not_bump {
            self.clock(cmd)
        } else {
            self.clock(cmd) + 1
        };

        // bump to at least `min_clock`
        let clock = cmp::max(min_clock, next_clock);

        // compute votes up to that clock
        let key_count = cmd.key_count(self.shard_id);
        let mut votes = Votes::with_capacity(key_count);
        self.detached(cmd, clock, &mut votes);

        // return both
        (clock, votes)
    }

    fn detached(&mut self, cmd: &Command, up_to: u64, votes: &mut Votes) {
        // vote on each key
        cmd.keys(self.shard_id).for_each(|key| {
            // get a mutable reference to current clock value
            let current = match self.clocks.get_mut(key) {
                Some(current) => current,
                None => self.clocks.entry(key.clone()).or_insert(0),
            };

            Self::maybe_bump(self.process_id, key, current, up_to, votes);
        });
    }

    fn detached_all(&mut self, up_to: u64, votes: &mut Votes) {
        // vote on each key
        let id = self.process_id;
        self.clocks.iter_mut().for_each(|(key, current)| {
            Self::maybe_bump(id, key, current, up_to, votes);
        });
    }

    fn parallel() -> bool {
        false
    }
}

impl SequentialKeyClocks {
    /// Retrieves the current clock for some command.
    /// If the command touches multiple keys, returns the maximum between the
    /// clocks associated with each key.
    fn clock(&self, cmd: &Command) -> u64 {
        cmd.keys(self.shard_id)
            .filter_map(|key| self.clocks.get(key))
            .max()
            .cloned()
            // if keys don't exist yet, we may have no maximum; in that case we
            // should return 0
            .unwrap_or(0)
    }

    fn maybe_bump(
        id: ProcessId,
        key: &Key,
        current: &mut u64,
        up_to: u64,
        votes: &mut Votes,
    ) {
        // if we should vote
        if *current < up_to {
            // vote from the current clock value + 1 until `clock`
            let vr = VoteRange::new(id, *current + 1, up_to);
            // update current clock to be `clock`
            *current = up_to;
            votes.add(key, vr);
        }
    }
}
