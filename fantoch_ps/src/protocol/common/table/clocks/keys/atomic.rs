use super::KeyClocks;
use crate::protocol::common::shared::Shared;
use crate::protocol::common::table::{VoteRange, Votes};
use fantoch::command::Command;
use fantoch::id::{ProcessId, ShardId};
use fantoch::HashSet;
use std::cmp;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct AtomicKeyClocks {
    id: ProcessId,
    shard_id: ShardId,
    clocks: Arc<Shared<AtomicU64>>,
}

impl KeyClocks for AtomicKeyClocks {
    /// Create a new `AtomicKeyClocks` instance.
    fn new(id: ProcessId, shard_id: ShardId) -> Self {
        // create shared clocks
        let clocks = Shared::new();
        // wrap them in an arc
        let clocks = Arc::new(clocks);

        Self {
            id,
            shard_id,
            clocks,
        }
    }

    fn init_clocks(&mut self, cmd: &Command) {
        cmd.keys(self.shard_id).for_each(|key| {
            // get initializes the key to the default value, and that's exactly
            // what we want
            let _ = self.clocks.get(key);
        });
    }

    fn bump_and_vote(&mut self, cmd: &Command, min_clock: u64) -> (u64, Votes) {
        // first round of votes:
        // - vote on each key and compute the highest clock seen
        // - this means that if we have more than one key, then we don't
        //   necessarily end up with all key clocks equal
        // OPTIMIZATION: keep track of the highest bumped-to value; if we have
        // to iterate in a way that the highest clock is iterated first, this
        // will be almost equivalent to `LockedKeyClocks`

        let keys: Vec<_> = cmd.keys(self.shard_id).collect();
        let key_count = keys.len();
        let mut clocks = HashSet::with_capacity(key_count);
        let mut votes = Votes::with_capacity(key_count);
        let mut up_to = min_clock;
        keys.iter().for_each(|&key| {
            // bump the `key` clock
            let clock = self.clocks.get(key);
            let previous_value = Self::bump(&clock, up_to);

            // create vote range and save it
            up_to = cmp::max(up_to, previous_value + 1);
            let vr = VoteRange::new(self.id, previous_value + 1, up_to);
            votes.set(key.clone(), vec![vr]);

            // save final clock value
            clocks.insert(up_to);
        });

        // second round of votes:
        // - if not all clocks match (i.e. we didn't get a result equivalent to
        //   `LockedKeyClocks`), try to make them match
        if clocks.len() > 1 {
            keys.iter().for_each(|key| {
                let clock = self.clocks.get(key);
                if let Some(vr) = Self::maybe_bump(self.id, &clock, up_to) {
                    votes.add(key, vr);
                }
            })
        }

        (up_to, votes)
    }

    fn vote(&mut self, cmd: &Command, up_to: u64) -> Votes {
        // create votes
        let keys: Vec<_> = cmd.keys(self.shard_id).collect();
        let key_count = keys.len();
        let mut votes = Votes::with_capacity(key_count);
        for key in keys {
            let clock = self.clocks.get(key);
            if let Some(vr) = Self::maybe_bump(self.id, &clock, up_to) {
                votes.set(key.clone(), vec![vr]);
            }
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
            if let Some(vr) = Self::maybe_bump(self.id, &clock, up_to) {
                votes.set(key.clone(), vec![vr]);
            }
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
        clock: &AtomicU64,
        up_to: u64,
    ) -> Option<VoteRange> {
        let fetch_update =
            clock.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                if value < up_to {
                    Some(up_to)
                } else {
                    None
                }
            });
        fetch_update
            .ok()
            .map(|previous_value| VoteRange::new(id, previous_value + 1, up_to))
    }
}
