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
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;

#[derive(Clone)]
pub struct TableExecutor {
    execute_at_commit: bool,
    table: MultiVotesTable,
    store: KVStore,
    monitor: Option<ExecutionOrderMonitor>,
    metrics: ExecutorMetrics,
    to_clients: VecDeque<ExecutorResult>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Pending {
    rifl: Rifl,
    cmd_key_count: usize,
    ops: Arc<Vec<KVOp>>,
}

impl Pending {
    pub fn new(rifl: Rifl, cmd_key_count: usize, ops: Arc<Vec<KVOp>>) -> Self {
        Self {
            rifl,
            cmd_key_count,
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
        let store = KVStore::new();
        let monitor = if config.executor_monitor_execution_order() {
            Some(ExecutionOrderMonitor::new())
        } else {
            None
        };
        let metrics = ExecutorMetrics::new();
        let to_clients = Default::default();

        Self {
            execute_at_commit: config.execute_at_commit(),
            table,
            store,
            monitor,
            metrics,
            to_clients,
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
                cmd_key_count,
                ops,
                votes,
            } => {
                let pending = Pending::new(rifl, cmd_key_count, ops);
                if self.execute_at_commit {
                    self.execute(key, std::iter::once(pending));
                } else {
                    let to_execute = self
                        .table
                        .add_attached_votes(dot, clock, &key, pending, votes);
                    self.execute(key, to_execute);
                }
            }
            TableExecutionInfo::DetachedVotes { key, votes } => {
                if !self.execute_at_commit {
                    let to_execute = self.table.add_detached_votes(&key, votes);
                    self.execute(key, to_execute);
                }
            }
        }
    }

    fn to_clients(&mut self) -> Option<ExecutorResult> {
        self.to_clients.pop_front()
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
    fn execute<I>(&mut self, key: Key, to_execute: I)
    where
        I: Iterator<Item = Pending>,
    {
        to_execute.for_each(|stable| {
            self.execute_pending(&key, stable);
        })
    }

    fn execute_pending(&mut self, key: &Key, stable: Pending) {
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
        cmd_key_count: usize,
        ops: Arc<Vec<KVOp>>,
        votes: Vec<VoteRange>,
    },
    DetachedVotes {
        key: Key,
        votes: Vec<VoteRange>,
    },
}

impl TableExecutionInfo {
    pub fn attached_votes(
        dot: Dot,
        clock: u64,
        key: Key,
        rifl: Rifl,
        cmd_key_count: usize,
        ops: Arc<Vec<KVOp>>,
        votes: Vec<VoteRange>,
    ) -> Self {
        TableExecutionInfo::AttachedVotes {
            dot,
            clock,
            key,
            rifl,
            cmd_key_count,
            ops,
            votes,
        }
    }

    pub fn detached_votes(key: Key, votes: Vec<VoteRange>) -> Self {
        TableExecutionInfo::DetachedVotes { key, votes }
    }
}

impl MessageKey for TableExecutionInfo {
    fn key(&self) -> &Key {
        match self {
            TableExecutionInfo::AttachedVotes { key, .. } => key,
            TableExecutionInfo::DetachedVotes { key, .. } => key,
        }
    }
}
