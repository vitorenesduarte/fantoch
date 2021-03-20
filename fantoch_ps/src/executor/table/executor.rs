use crate::executor::table::MultiVotesTable;
use crate::protocol::common::table::VoteRange;
use fantoch::config::Config;
use fantoch::executor::{
    ExecutionOrderMonitor, Executor, ExecutorMetrics, ExecutorResult,
    MessageKey,
};
use fantoch::id::{Dot, ProcessId, Rifl, ShardId};
use fantoch::kvs::{KVOp, KVStore, Key};
use fantoch::time::SysTime;
use fantoch::trace;
use fantoch::HashMap;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;

#[derive(Clone)]
pub struct TableExecutor {
    process_id: ProcessId,
    execute_at_commit: bool,
    table: MultiVotesTable,
    store: KVStore,
    metrics: ExecutorMetrics,
    to_clients: VecDeque<ExecutorResult>,
    to_executors: Vec<(ShardId, TableExecutionInfo)>,
    pending: HashMap<Key, PendingPerKey>,
}

#[derive(Clone, Default)]
struct PendingPerKey {
    pending: VecDeque<Pending>,
    buffered: HashMap<Rifl, usize>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Pending {
    rifl: Rifl,
    elected_key: Key,
    elected_key_shard: ShardId,
    other_keys: Option<Vec<(ShardId, Key)>>,
    missing_stable_keys: Option<usize>,
    ops: Arc<Vec<KVOp>>,
}

impl Pending {
    pub fn new(
        rifl: Rifl,
        elected_key: Key,
        elected_key_shard: ShardId,
        other_keys: Option<Vec<(ShardId, Key)>>,
        ops: Arc<Vec<KVOp>>,
    ) -> Self {
        let missing_stable_keys =
            other_keys.as_ref().map(|other_keys| other_keys.len());
        Self {
            rifl,
            elected_key,
            elected_key_shard,
            other_keys,
            missing_stable_keys,
            ops,
        }
    }
}

impl Executor for TableExecutor {
    type ExecutionInfo = TableExecutionInfo;

    fn new(process_id: ProcessId, shard_id: ShardId, config: Config) -> Self {
        // TODO this is specific to newt
        let (_, _, stability_threshold) = config.newt_quorum_sizes();
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

        Self {
            process_id,
            execute_at_commit: config.execute_at_commit(),
            table,
            store,
            metrics,
            to_clients,
            to_executors,
            pending,
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
                elected_key,
                elected_key_shard,
                other_keys,
                ops,
                votes,
            } => {
                let pending = Pending::new(
                    rifl,
                    elected_key,
                    elected_key_shard,
                    other_keys,
                    ops,
                );
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
            TableExecutionInfo::Stable { key, rifl } => {
                self.handle_stable_msg(key, rifl)
            }
            TableExecutionInfo::StableAtAll { key, rifl } => {
                self.handle_stable_at_all_msg(key, rifl)
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

        trace!("p{}: key={} Stable {:?}", self.process_id, key, rifl);
        if let Some(pending) = pending_per_key.pending.get_mut(0) {
            // check if it's a message about the first command pending
            if pending.rifl == rifl {
                // decrease number of missing stable keys
                let missing_stable_keys =
                    pending.missing_stable_keys.as_mut().expect(
                        "missing stable keys should be set on the elected key",
                    );
                *missing_stable_keys -= 1;
                trace!(
                    "p{}: key={} Stable {:?} | missing {:?}",
                    self.process_id,
                    key,
                    rifl,
                    *missing_stable_keys
                );

                if *missing_stable_keys == 0 {
                    // if the command is now stable at all keys/partitions, then
                    // remove the command from pending, execute it and notify
                    // all the other keys/partitions
                    let pending = pending_per_key.pending.pop_front().unwrap();
                    Self::do_execute_and_notify_other_keys(
                        key.clone(),
                        pending,
                        &mut self.store,
                        &mut self.to_clients,
                        &mut self.to_executors,
                    );

                    // try to execute the remaining pending commands
                    Self::try_execute_next_pending(
                        key,
                        pending_per_key,
                        &mut self.store,
                        &mut self.to_clients,
                        &mut self.to_executors,
                    );
                }
            } else {
                // in this case, the command on this message is not yet
                // stable locally; in this case, we buffer this message
                *pending_per_key.buffered.entry(rifl).or_default() += 1;
            }
        } else {
            // in this case, the command on this message is not yet stable
            // locally; in this case, we buffer this message
            *pending_per_key.buffered.entry(rifl).or_default() += 1;
        }
    }

    fn handle_stable_at_all_msg(&mut self, key: Key, rifl: Rifl) {
        // get pending commands on this key
        let pending_per_key = self.pending.entry(key.clone()).or_default();

        trace!("p{}: key={} StableAtAll {:?}", self.process_id, key, rifl);

        // if must be a message about the first command pending
        let pending = pending_per_key.pending.pop_front().unwrap();
        assert_eq!(pending.rifl, rifl);

        // execute command
        Self::do_execute(
            key.clone(),
            pending,
            &mut self.store,
            &mut self.to_clients,
        );

        // try to execute the remaining pending commands
        Self::try_execute_next_pending(
            key,
            pending_per_key,
            &mut self.store,
            &mut self.to_clients,
            &mut self.to_executors,
        );
    }

    fn try_execute_next_pending(
        key: Key,
        pending_per_key: &mut PendingPerKey,
        store: &mut KVStore,
        to_clients: &mut VecDeque<ExecutorResult>,
        to_executors: &mut Vec<(ShardId, TableExecutionInfo)>,
    ) {
        while let Some(pending) = pending_per_key.pending.pop_front() {
            let try_result = Self::send_stable_or_execute_single(
                &key,
                pending,
                store,
                to_clients,
                to_executors,
                &mut pending_per_key.buffered,
            );
            if let Some(pending) = try_result {
                // if this command cannot be executed, buffer it and
                // give up trying to execute more commands
                pending_per_key.pending.push_front(pending);
                return;
            }
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

        // if there's no command pending on this key, then execute commands
        // while no command is added as pending
        while let Some(pending) = to_execute.next() {
            trace!(
                "p{}: key={} try_execute_single {:?} | missing {:?}",
                self.process_id,
                key,
                pending.rifl,
                pending.missing_stable_keys
            );
            let try_result = Self::send_stable_or_execute_single(
                &key,
                pending,
                &mut self.store,
                &mut self.to_clients,
                &mut self.to_executors,
                &mut pending_per_key.buffered,
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
    fn send_stable_or_execute_single(
        key: &Key,
        mut pending: Pending,
        store: &mut KVStore,
        to_clients: &mut VecDeque<ExecutorResult>,
        to_executors: &mut Vec<(ShardId, TableExecutionInfo)>,
        buffered: &mut HashMap<Rifl, usize>,
    ) -> Option<Pending> {
        // check if we're the elected key
        if &pending.elected_key == key {
            let missing_stable_keys = pending
                .missing_stable_keys
                .as_mut()
                .expect("missing stable keys should be set on the elected key");

            // if so, and the command is single-key, execute immediately
            if *missing_stable_keys == 0 {
                Self::do_execute(key.clone(), pending, store, to_clients);
                return None;
            } else {
                // if the command is not single key, then check if there's any
                // buffered stable messages
                if let Some(count) = buffered.remove(&pending.rifl) {
                    *missing_stable_keys -= count;

                    // if after processing the buffered stable messages, check
                    // if the command is ready to be executed
                    if *missing_stable_keys == 0 {
                        // if the command is now stable at all
                        // keys/partitions, then execute it and notify all the
                        // other keys/partitions
                        Self::do_execute_and_notify_other_keys(
                            key.clone(),
                            pending,
                            store,
                            to_clients,
                            to_executors,
                        );
                        return None;
                    }
                }
            }
        } else {
            // if I'm not the elected key, then simply notify the elected key
            // that the command is stable at my partition
            let msg = TableExecutionInfo::stable(
                pending.elected_key.clone(),
                pending.rifl,
            );
            to_executors.push((pending.elected_key_shard, msg));
        }

        // if we reach this, the command wasn't executed; so send it back to be
        // buffered
        Some(pending)
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

    fn do_execute_and_notify_other_keys(
        key: Key,
        mut pending: Pending,
        store: &mut KVStore,
        to_clients: &mut VecDeque<ExecutorResult>,
        to_executors: &mut Vec<(ShardId, TableExecutionInfo)>,
    ) {
        let other_keys = std::mem::take(&mut pending.other_keys)
            .expect("other keys should be set on the elected key");
        let msgs = other_keys.into_iter().map(|(shard_id, shard_key)| {
            let msg =
                TableExecutionInfo::stable_at_all(shard_key, pending.rifl);
            (shard_id, msg)
        });
        to_executors.extend(msgs);
        Self::do_execute(key, pending, store, to_clients)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TableExecutionInfo {
    AttachedVotes {
        dot: Dot,
        clock: u64,
        key: Key,
        rifl: Rifl,
        elected_key: Key,
        elected_key_shard: ShardId,
        other_keys: Option<Vec<(ShardId, Key)>>,
        ops: Arc<Vec<KVOp>>,
        votes: Vec<VoteRange>,
    },
    DetachedVotes {
        key: Key,
        votes: Vec<VoteRange>,
    },
    Stable {
        key: Key,
        rifl: Rifl,
    },
    StableAtAll {
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
        elected_key: Key,
        elected_key_shard: ShardId,
        other_keys: Option<Vec<(ShardId, Key)>>,
        ops: Arc<Vec<KVOp>>,
        votes: Vec<VoteRange>,
    ) -> Self {
        Self::AttachedVotes {
            dot,
            clock,
            key,
            rifl,
            elected_key,
            elected_key_shard,
            other_keys,
            ops,
            votes,
        }
    }

    pub fn detached_votes(key: Key, votes: Vec<VoteRange>) -> Self {
        Self::DetachedVotes { key, votes }
    }

    pub fn stable(key: Key, rifl: Rifl) -> Self {
        Self::Stable { key, rifl }
    }

    pub fn stable_at_all(key: Key, rifl: Rifl) -> Self {
        Self::StableAtAll { key, rifl }
    }
}

impl MessageKey for TableExecutionInfo {
    fn key(&self) -> &Key {
        match self {
            Self::AttachedVotes { key, .. } => key,
            Self::DetachedVotes { key, .. } => key,
            Self::Stable { key, .. } => key,
            Self::StableAtAll { key, .. } => key,
        }
    }
}
