use crate::executor::table::MultiVotesTable;
use crate::protocol::common::table::VoteRange;
use fantoch::config::Config;
use fantoch::executor::{
    ExecutionOrderMonitor, Executor, ExecutorMetrics, ExecutorResult,
    MessageKey,
};
use fantoch::hash_map::{Entry, HashMap};
use fantoch::id::{Dot, ProcessId, Rifl, ShardId};
use fantoch::kvs::{KVOp, KVStore, Key};
use fantoch::time::SysTime;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Pending {
    rifl: Rifl,
    remaining_keys: Vec<(ShardId, Key)>,
    ops: Arc<Vec<KVOp>>,
    missing_stable_keys: usize,
}

impl Pending {
    pub fn new(
        rifl: Rifl,
        remaining_keys: Vec<(ShardId, Key)>,
        ops: Arc<Vec<KVOp>>,
    ) -> Self {
        let missing_stable_keys = remaining_keys.len();
        Self {
            rifl,
            remaining_keys,
            ops,
            missing_stable_keys,
        }
    }
}

#[derive(Clone)]
pub struct TableExecutor {
    execute_at_commit: bool,
    table: MultiVotesTable,
    store: KVStore,
    monitor: Option<ExecutionOrderMonitor>,
    metrics: ExecutorMetrics,
    to_clients: VecDeque<ExecutorResult>,
    to_executors: Vec<(ShardId, TableExecutionInfo)>,
    pending: HashMap<Rifl, Pending>,
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
        let store = KVStore::new();
        let monitor = if config.executor_monitor_execution_order() {
            Some(ExecutionOrderMonitor::new())
        } else {
            None
        };
        let metrics = ExecutorMetrics::new();
        let to_clients = Default::default();
        let to_executors = Default::default();
        let pending = Default::default();

        Self {
            execute_at_commit: config.execute_at_commit(),
            table,
            store,
            monitor,
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
                remaining_keys,
                ops,
                votes,
            } => {
                let pending = Pending::new(rifl, remaining_keys, ops);
                if self.execute_at_commit {
                    self.try_execute(key, std::iter::once(pending));
                } else {
                    let to_execute = self
                        .table
                        .add_attached_votes(dot, clock, &key, pending, votes);
                    self.try_execute(key, to_execute);
                }
            }
            TableExecutionInfo::DetachedVotes { key, votes } => {
                if !self.execute_at_commit {
                    let to_execute = self.table.add_detached_votes(&key, votes);
                    self.try_execute(key, to_execute);
                }
            }
            TableExecutionInfo::Stable { rifl, key } => {
                if let Entry::Occupied(mut pending) = self.pending.entry(rifl) {
                    // decrease number of missing stable keys
                    pending.get_mut().missing_stable_keys -= 1;

                    if pending.get().missing_stable_keys == 0 {
                        // if all keys are stable, remove command from pending
                        // and execute it
                        let pending = pending.remove();
                        self.do_execute(&key, pending);
                    }
                } else {
                    panic!("every command in stable messages must be pending");
                }
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

    fn monitor(&self) -> Option<&ExecutionOrderMonitor> {
        self.monitor.as_ref()
    }
}

impl TableExecutor {
    fn try_execute<I>(&mut self, key: Key, to_execute: I)
    where
        I: Iterator<Item = Pending>,
    {
        to_execute.for_each(|mut stable| {
            if stable.missing_stable_keys == 0 {
                // if the command is single-key, execute immediately.
                self.do_execute(&key, stable);
            } else {
                // otherwise, send a `Stable` message to each of the other
                // keys/partitions accessed by the command

                // take `remaining_keys` as they're no longer needed
                let remaining_keys = std::mem::take(&mut stable.remaining_keys);
                let msgs = remaining_keys.into_iter().map(|(shard_id, key)| {
                    let msg = TableExecutionInfo::stable(key, stable.rifl);
                    (shard_id, msg)
                });
                self.to_executors.extend(msgs);

                // add (locally) stable command to `pending`
                self.pending.insert(stable.rifl, stable);
            }
        })
    }

    fn do_execute(&mut self, key: &Key, stable: Pending) {
        // take the ops inside the arc if we're the last with a
        // reference to it (otherwise, clone them)
        let rifl = stable.rifl;
        let ops = stable.ops;
        let ops =
            Arc::try_unwrap(ops).unwrap_or_else(|ops| ops.as_ref().clone());
        // execute ops in the `KVStore`
        let partial_results =
            self.store
                .execute_with_monitor(key, ops, rifl, &mut self.monitor);
        self.to_clients.push_back(ExecutorResult::new(
            rifl,
            key.clone(),
            partial_results,
        ));
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TableExecutionInfo {
    AttachedVotes {
        dot: Dot,
        clock: u64,
        key: Key,
        rifl: Rifl,
        remaining_keys: Vec<(ShardId, Key)>,
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
}

impl TableExecutionInfo {
    pub fn attached_votes(
        dot: Dot,
        clock: u64,
        key: Key,
        rifl: Rifl,
        remaining_keys: Vec<(ShardId, Key)>,
        ops: Arc<Vec<KVOp>>,
        votes: Vec<VoteRange>,
    ) -> Self {
        Self::AttachedVotes {
            dot,
            clock,
            key,
            rifl,
            remaining_keys,
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
}

impl MessageKey for TableExecutionInfo {
    fn key(&self) -> &Key {
        match self {
            Self::AttachedVotes { key, .. } => key,
            Self::DetachedVotes { key, .. } => key,
            Self::Stable { key, .. } => key,
        }
    }
}
