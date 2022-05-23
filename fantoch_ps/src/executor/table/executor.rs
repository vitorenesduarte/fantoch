use crate::executor::table::MultiVotesTable;
use crate::protocol::common::table::VoteRange;
use fantoch::config::Config;
use fantoch::executor::{
    ExecutionOrderMonitor, Executor, ExecutorMetrics, ExecutorResult,
    MessageKey,
};
use fantoch::id::{Dot, ProcessId, Rifl, ShardId};
use fantoch::kvs::{KVOp, KVStore, Key};
use fantoch::shared::SharedMap;
use fantoch::time::SysTime;
use fantoch::trace;
use fantoch::HashMap;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;

#[derive(Clone)]
pub struct TableExecutor {
    process_id: ProcessId,
    shard_id: ShardId,
    execute_at_commit: bool,
    table: MultiVotesTable,
    store: KVStore,
    metrics: ExecutorMetrics,
    to_clients: VecDeque<ExecutorResult>,
    to_executors: Vec<(ShardId, TableExecutionInfo)>,
    pending: HashMap<Key, PendingPerKey>,
    rifl_to_stable_count: Arc<SharedMap<Rifl, Mutex<u64>>>,
}

#[derive(Clone, Default)]
struct PendingPerKey {
    pending: VecDeque<Pending>,
    stable_shards_buffered: HashMap<Rifl, usize>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Pending {
    rifl: Rifl,
    shard_to_keys: Arc<HashMap<ShardId, Vec<Key>>>,
    // number of keys on being accessed on this shard
    shard_key_count: u64,
    // number of shards the key is not stable at yet
    missing_stable_shards: usize,
    ops: Arc<Vec<KVOp>>,
}

impl Pending {
    pub fn new(
        shard_id: ShardId,
        rifl: Rifl,
        shard_to_keys: Arc<HashMap<ShardId, Vec<Key>>>,
        ops: Arc<Vec<KVOp>>,
    ) -> Self {
        let shard_key_count = shard_to_keys
            .get(&shard_id)
            .expect("my shard should be accessed by this command")
            .len() as u64;
        let missing_stable_shards = shard_to_keys.len();
        Self {
            rifl,
            shard_to_keys,
            shard_key_count,
            missing_stable_shards,
            ops,
        }
    }

    pub fn single_key_command(&self) -> bool {
        // the command is single key if it accesses a single shard and the
        // number of keys accessed in that shard is one
        self.missing_stable_shards == 1 && self.shard_key_count == 1
    }
}

impl Executor for TableExecutor {
    type ExecutionInfo = TableExecutionInfo;

    fn new(process_id: ProcessId, shard_id: ShardId, config: Config) -> Self {
        let (_, _, stability_threshold) = config.tempo_quorum_sizes();
        let table = MultiVotesTable::new(
            process_id,
            shard_id,
            config.n(),
            stability_threshold,
        );
        let store = KVStore::new(config.executor_monitor_execution_order());
        let metrics = ExecutorMetrics::new();
        let to_clients = Default::default();
        let to_executors = Default::default();
        let pending = Default::default();
        let rifl_to_stable_count = Arc::new(SharedMap::new());

        Self {
            process_id,
            shard_id,
            execute_at_commit: config.execute_at_commit(),
            table,
            store,
            metrics,
            to_clients,
            to_executors,
            pending,
            rifl_to_stable_count,
        }
    }

    fn handle(&mut self, info: Self::ExecutionInfo, _time: &dyn SysTime) {
        // handle each new info by updating the votes table and execute ready
        // commands
        match info {
            TableExecutionInfo::AttachedVotes {
                dot,
                clock,
                key,
                rifl,
                shard_to_keys,
                ops,
                votes,
            } => {
                let pending =
                    Pending::new(self.shard_id, rifl, shard_to_keys, ops);
                if self.execute_at_commit {
                    self.execute(key, pending);
                } else {
                    let to_execute = self
                        .table
                        .add_attached_votes(dot, clock, &key, pending, votes);
                    self.send_stable_or_execute(key, to_execute);
                }
            }
            TableExecutionInfo::DetachedVotes { key, votes } => {
                if !self.execute_at_commit {
                    let to_execute = self.table.add_detached_votes(&key, votes);
                    self.send_stable_or_execute(key, to_execute);
                }
            }
            TableExecutionInfo::StableAtShard { key, rifl } => {
                self.handle_stable_msg(key, rifl)
            }
        }
    }

    fn to_clients(&mut self) -> Option<ExecutorResult> {
        self.to_clients.pop_front()
    }

    fn to_executors(&mut self) -> Option<(ShardId, TableExecutionInfo)> {
        self.to_executors.pop()
    }

    fn parallel() -> bool {
        true
    }

    fn metrics(&self) -> &ExecutorMetrics {
        &self.metrics
    }

    fn monitor(&self) -> Option<ExecutionOrderMonitor> {
        self.store.monitor().cloned()
    }
}

impl TableExecutor {
    fn handle_stable_msg(&mut self, key: Key, rifl: Rifl) {
        // get pending commands on this key
        let pending_per_key = self.pending.entry(key.clone()).or_default();

        trace!("p{}: key={} StableAtShard {:?}", self.process_id, key, rifl);
        if let Some(pending) = pending_per_key.pending.get_mut(0) {
            // check if it's a message about the first command pending
            if pending.rifl == rifl {
                // decrease number of missing stable shards
                pending.missing_stable_shards -= 1;
                trace!(
                    "p{}: key={} StableAtShard {:?} | missing shards {:?}",
                    self.process_id,
                    key,
                    rifl,
                    pending.missing_stable_shards
                );

                if pending.missing_stable_shards == 0 {
                    // if the command is stable at all shards, remove command
                    // from pending and execute it
                    let pending = pending_per_key.pending.pop_front().unwrap();
                    Self::do_execute(
                        key.clone(),
                        pending,
                        &mut self.store,
                        &mut self.to_clients,
                    );

                    // try to execute the remaining pending commands
                    while let Some(pending) =
                        pending_per_key.pending.pop_front()
                    {
                        let try_result =
                            Self::execute_single_or_mark_it_as_stable(
                                &key,
                                pending,
                                &mut self.store,
                                &mut self.to_clients,
                                &mut self.to_executors,
                                &mut pending_per_key.stable_shards_buffered,
                                &self.rifl_to_stable_count,
                            );
                        if let Some(pending) = try_result {
                            // if this command cannot be executed, buffer it and
                            // give up trying to execute more commands
                            pending_per_key.pending.push_front(pending);
                            return;
                        }
                    }
                }
            } else {
                // in this case, the command on this message is not yet
                // stable locally; in this case, we buffer this message
                *pending_per_key
                    .stable_shards_buffered
                    .entry(rifl)
                    .or_default() += 1;
            }
        } else {
            // in this case, the command on this message is not yet stable
            // locally; in this case, we buffer this message
            *pending_per_key
                .stable_shards_buffered
                .entry(rifl)
                .or_default() += 1;
        }
    }

    fn send_stable_or_execute<I>(&mut self, key: Key, mut to_execute: I)
    where
        I: Iterator<Item = Pending>,
    {
        let pending_per_key = self.pending.entry(key.clone()).or_default();
        if !pending_per_key.pending.is_empty() {
            // if there's already commmands pending at this key, then no
            // command can be executed, and thus we add them all as pending
            pending_per_key.pending.extend(to_execute);
            return;
        }

        // execute commands while no command is added as pending
        while let Some(pending) = to_execute.next() {
            trace!(
                "p{}: key={} try_execute_single {:?} | missing shards {:?}",
                self.process_id,
                key,
                pending.rifl,
                pending.missing_stable_shards
            );
            let try_result = Self::execute_single_or_mark_it_as_stable(
                &key,
                pending,
                &mut self.store,
                &mut self.to_clients,
                &mut self.to_executors,
                &mut pending_per_key.stable_shards_buffered,
                &self.rifl_to_stable_count,
            );
            if let Some(pending) = try_result {
                // if this command cannot be executed, then add it (and all the
                // remaining commands as pending) and give up trying to execute
                // more commands
                assert!(pending_per_key.pending.is_empty());
                pending_per_key.pending.push_back(pending);
                pending_per_key.pending.extend(to_execute);
                return;
            }
        }
    }

    #[must_use]
    fn execute_single_or_mark_it_as_stable(
        key: &Key,
        mut pending: Pending,
        store: &mut KVStore,
        to_clients: &mut VecDeque<ExecutorResult>,
        to_executors: &mut Vec<(ShardId, TableExecutionInfo)>,
        stable_shards_buffered: &mut HashMap<Rifl, usize>,
        rifl_to_stable_count: &Arc<SharedMap<Rifl, Mutex<u64>>>,
    ) -> Option<Pending> {
        let rifl = pending.rifl;
        if pending.single_key_command() {
            // if the command is single-key, execute immediately
            Self::do_execute(key.clone(), pending, store, to_clients);
            None
        } else {
            // closure that sends the stable message
            let mut send_stable_msg = || {
                for (shard_id, shard_keys) in pending.shard_to_keys.iter() {
                    for shard_key in shard_keys {
                        if shard_key != key {
                            let msg = TableExecutionInfo::stable_at_shard(
                                shard_key.clone(),
                                rifl,
                            );
                            to_executors.push((*shard_id, msg));
                        }
                    }
                }
                true
            };

            if pending.shard_key_count == 1 {
                // if this command access a single key on this shard, then send
                // the stable message right away
                assert!(send_stable_msg());
                // and update the number of shards the key is stable at
                pending.missing_stable_shards -= 1;
            } else {
                // otherwise, increase rifl count
                let count_ref =
                    rifl_to_stable_count.get_or(&rifl, || Mutex::new(0));
                let mut count = count_ref.lock();
                *count += 1;

                // if we're the last key at this shard increasing the rifl count
                // to the number of keys in this shard, then
                // notify all keys that the command is stable at
                // this shard
                if *count == pending.shard_key_count {
                    // the command is stable at this shard; so send stable
                    // messsage
                    assert!(send_stable_msg());
                    // and update the number of shards the key is stable at
                    pending.missing_stable_shards -= 1;

                    // cleanup
                    drop(count);
                    drop(count_ref);
                    rifl_to_stable_count
                        .remove(&rifl)
                        .expect("rifl must exist as a key");
                }
            }

            // check if there's any buffered stable messages
            if let Some(count) = stable_shards_buffered.remove(&rifl) {
                pending.missing_stable_shards -= count;
            }

            if pending.missing_stable_shards == 0 {
                // if the command is already stable at shards, then execute it
                Self::do_execute(key.clone(), pending, store, to_clients);
                None
            } else {
                // in this case, the command cannot be executed; so send it back
                // to be buffered
                Some(pending)
            }
        }
    }

    fn execute(&mut self, key: Key, stable: Pending) {
        Self::do_execute(key, stable, &mut self.store, &mut self.to_clients)
    }

    fn do_execute(
        key: Key,
        stable: Pending,
        store: &mut KVStore,
        to_clients: &mut VecDeque<ExecutorResult>,
    ) {
        // take the ops inside the arc if we're the last with a reference to it
        // (otherwise, clone them)
        let rifl = stable.rifl;
        let ops = stable.ops;
        let ops =
            Arc::try_unwrap(ops).unwrap_or_else(|ops| ops.as_ref().clone());
        // execute ops in the `KVStore`
        let partial_results = store.execute(&key, ops, rifl);
        to_clients.push_back(ExecutorResult::new(rifl, key, partial_results));
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TableExecutionInfo {
    AttachedVotes {
        dot: Dot,
        clock: u64,
        key: Key,
        rifl: Rifl,
        shard_to_keys: Arc<HashMap<ShardId, Vec<Key>>>,
        ops: Arc<Vec<KVOp>>,
        votes: Vec<VoteRange>,
    },
    DetachedVotes {
        key: Key,
        votes: Vec<VoteRange>,
    },
    StableAtShard {
        key: Key,
        rifl: Rifl,
    },
}

impl TableExecutionInfo {
    pub fn attached_votes(
        dot: Dot,
        clock: u64,
        key: Key,
        rifl: Rifl,
        shard_to_keys: Arc<HashMap<ShardId, Vec<Key>>>,
        ops: Arc<Vec<KVOp>>,
        votes: Vec<VoteRange>,
    ) -> Self {
        Self::AttachedVotes {
            dot,
            clock,
            key,
            rifl,
            shard_to_keys,
            ops,
            votes,
        }
    }

    pub fn detached_votes(key: Key, votes: Vec<VoteRange>) -> Self {
        Self::DetachedVotes { key, votes }
    }

    pub fn stable_at_shard(key: Key, rifl: Rifl) -> Self {
        Self::StableAtShard { key, rifl }
    }
}

impl MessageKey for TableExecutionInfo {
    fn key(&self) -> &Key {
        match self {
            Self::AttachedVotes { key, .. } => key,
            Self::DetachedVotes { key, .. } => key,
            Self::StableAtShard { key, .. } => key,
        }
    }
}
