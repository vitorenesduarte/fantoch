use super::VertexIndex;
use fantoch::config::Config;
use fantoch::id::{Dot, ProcessId, ShardId};
use fantoch::time::SysTime;
use fantoch::util;
use fantoch::HashSet;
use std::collections::VecDeque;
use threshold::AEClock;
use threshold::EventSet;

// epoch length is 1s
const EPOCH_MILLIS: u64 = 1000;
// executed clock is leveled every 10 seconds
const EPOCH_LEVEL_AGE: u64 = 10;

#[derive(Clone)]
pub struct LevelExecutedClock {
    process_id: ProcessId,
    // ids of processes in my shard
    shard_process_ids: HashSet<ProcessId>,
    // ids of processes *not* in my shard
    not_shard_process_ids: HashSet<ProcessId>,
    // mapping from epoch to what's been executed from my shard at that
    // epoch
    to_level: VecDeque<(u64, u64)>,
    current_epoch: Option<u64>,
}

impl LevelExecutedClock {
    pub fn new(
        process_id: ProcessId,
        shard_id: ShardId,
        config: &Config,
    ) -> Self {
        // compute shard process ids and not shard process ids
        let shard_process_ids =
            util::process_ids(shard_id, config.n()).collect();
        let not_shard_process_ids =
            util::all_process_ids(config.shards(), config.n())
                .filter_map(|(peer_id, peer_shard_id)| {
                    if peer_shard_id == shard_id {
                        None
                    } else {
                        Some(peer_id)
                    }
                })
                .collect();
        // create to level and current level epoch
        let to_level = Default::default();
        let current_epoch = Default::default();

        Self {
            process_id,
            shard_process_ids,
            not_shard_process_ids,
            to_level,
            current_epoch,
        }
    }

    pub fn maybe_level(
        &mut self,
        executed_clock: &mut AEClock<ProcessId>,
        vertex_index: &VertexIndex,
        time: &dyn SysTime,
    ) -> Vec<Dot> {
        let now = self.maybe_update_epoch(executed_clock, time);
        if let Some((epoch, _)) = self.to_level.get(0) {
            // compute age of this epoch
            let epoch_age = now - epoch;
            tracing::debug!(
                "p{}: LevelExecutedClock::maybe_level now {} | epoch {} | age {}",
                self.process_id,
                now,
                epoch,
                epoch_age
            );
            // if epoch is age is higher than the level age, then level
            if epoch_age >= EPOCH_LEVEL_AGE {
                // get what I executed from my shard on that epoch
                let (_, executed) = self
                    .to_level
                    .pop_front()
                    .expect("there should be a front to level");

                tracing::debug!(
                    "p{}: LevelExecutedClock::maybe_update_epoch before = {:?}",
                    self.process_id,
                    executed_clock
                );

                let mut maybe_executed = Vec::new();

                // level all the entries that are not from my shard to what I've
                // executed from my shard at that epoch (only if there are no
                // pending commands on up to that)
                self.not_shard_process_ids.iter().for_each(|peer_id| {
                    let level_from = executed_clock
                        .get(peer_id)
                        .expect("peer should be in executed clock")
                        .frontier() + 1;
                    // compute up to which value we can level; if there are no
                    // pending command up to `executed`, then level up to that;
                    // if there are, then level up to the command prior the
                    // oldest pending one
                    let mut level_up_to = executed;

                    // check if we can level all command up to `executed`
                    for event in level_from..=executed {
                        let dot = Dot::new(*peer_id, event);
                        if vertex_index.contains(&dot) {
                            // if this command is pending, then we can only level
                            // up to its prior command
                            level_up_to = event - 1;
                            break;
                        } else {
                            // update set of maybe executed dots (this is a maybe
                            // since this dot may already exist as an above
                            // exception in the executed clock)
                            maybe_executed.push(dot);
                        }
                    }

                    // level this peer's entry in the executed clock
                    executed_clock.add_range(peer_id, level_from, level_up_to);
                    tracing::debug!(
                        "p{}: LevelExecutedClock::maybe_update_epoch peer {} leveled up to {}",
                        self.process_id,
                        peer_id,
                        level_up_to
                    );
                });

                tracing::debug!(
                    "p{}: LevelExecutedClock::maybe_update_epoch after {} = {:?}",
                    self.process_id,
                    executed,
                    executed_clock
                );

                return maybe_executed;
            }
        }

        Vec::new()
    }

    fn maybe_update_epoch(
        &mut self,
        executed_clock: &AEClock<ProcessId>,
        time: &dyn SysTime,
    ) -> u64 {
        let now = time.millis() / EPOCH_MILLIS;
        match self.current_epoch {
            Some(current_epoch) => {
                // check if should update epoch
                if now > current_epoch {
                    // compute what I've executed from my shard
                    let executed = self
                        .shard_process_ids
                        .iter()
                        .map(|id| {
                            executed_clock
                                .get(id)
                                .expect("shard process id should be in executed clock")
                                .frontier()
                        })
                        .min()
                        .expect("min executed should exist");
                    tracing::debug!(
                        "p{}: LevelExecutedClock::maybe_update_epoch next epoch = {} | executed = {} | time = {}",
                        self.process_id,
                        now,
                        executed,
                        time.millis()
                    );
                    self.to_level.push_back((current_epoch, executed));
                    // update epoch
                    self.current_epoch = Some(now);
                }
            }
            None => {
                tracing::debug!(
                    "p{}: LevelExecutedClock::maybe_update_epoch first epoch: {}",
                    self.process_id,
                    now
                );
                self.current_epoch = Some(now);
            }
        }
        now
    }
}
