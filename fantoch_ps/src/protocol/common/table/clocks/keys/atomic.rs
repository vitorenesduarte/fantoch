use super::KeyClocks;
use crate::protocol::common::table::{VoteRange, Votes};
use fantoch::shared::SharedMap;
use fantoch::command::Command;
use fantoch::id::{ProcessId, ShardId};
use fantoch::kvs::Key;
use fantoch::HashSet;
use std::cmp;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct AtomicKeyClocks {
    process_id: ProcessId,
    shard_id: ShardId,
    clocks: Arc<SharedMap<Key, AtomicU64>>,
}

impl KeyClocks for AtomicKeyClocks {
    /// Create a new `AtomicKeyClocks` instance.
    fn new(process_id: ProcessId, shard_id: ShardId) -> Self {
        // create shared clocks
        let clocks = SharedMap::new();
        // wrap them in an arc
        let clocks = Arc::new(clocks);

        Self {
            process_id,
            shard_id,
            clocks,
        }
    }

    fn init_clocks(&mut self, cmd: &Command) {
        cmd.keys(self.shard_id).for_each(|key| {
            // get initializes the key to the default value, and that's exactly
            // what we want
            let _ = self.clocks.get_or(key, || AtomicU64::default());
        });
    }

    fn proposal(&mut self, cmd: &Command, min_clock: u64) -> (u64, Votes) {
        // first round of votes:
        // - vote on each key and compute the highest clock seen
        // - this means that if we have more than one key, then we don't
        //   necessarily end up with all key clocks equal
        // OPTIMIZATION: keep track of the highest bumped-to value; if we have
        // to iterate in a way that the highest clock is iterated first, this
        // will be almost equivalent to `LockedKeyClocks`

        let key_count = cmd.key_count(self.shard_id);
        let mut clocks = HashSet::with_capacity(key_count);
        let mut votes = Votes::with_capacity(key_count);
        let mut up_to = min_clock;
        cmd.keys(self.shard_id).for_each(|key| {
            // bump the `key` clock
            let clock = self.clocks.get_or(key, || AtomicU64::default());
            let previous_value = Self::bump(&clock, up_to);

            // create vote range and save it
            up_to = cmp::max(up_to, previous_value + 1);
            let vr = VoteRange::new(self.process_id, previous_value + 1, up_to);
            votes.set(key.clone(), vec![vr]);

            // save final clock value
            clocks.insert(up_to);
        });

        // second round of votes:
        // - if not all clocks match (i.e. we didn't get a result equivalent to
        //   `LockedKeyClocks`), try to make them match
        if clocks.len() > 1 {
            cmd.keys(self.shard_id).for_each(|key| {
                let clock = self.clocks.get_or(key, || AtomicU64::default());
                if let Some(vr) =
                    Self::maybe_bump(self.process_id, &clock, up_to)
                {
                    votes.add(key, vr);
                }
            })
        }

        (up_to, votes)
    }

    fn detached(&mut self, cmd: &Command, up_to: u64, votes: &mut Votes) {
        for key in cmd.keys(self.shard_id) {
            let clock = self.clocks.get_or(key, || AtomicU64::default());
            if let Some(vr) = Self::maybe_bump(self.process_id, &clock, up_to) {
                votes.add(key, vr);
            }
        }
    }

    fn detached_all(&mut self, up_to: u64, votes: &mut Votes) {
        self.clocks.iter().for_each(|entry| {
            let key = entry.key();
            let clock = entry.value();
            if let Some(vr) = Self::maybe_bump(self.process_id, &clock, up_to) {
                votes.add(key, vr);
            }
        });
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
