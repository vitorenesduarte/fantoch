use crate::executor::table::MultiVotesTable;
use crate::protocol::common::table::VoteRange;
use fantoch::command::Command;
use fantoch::config::Config;
use fantoch::executor::{
    Executor, ExecutorMetrics, ExecutorResult, MessageKey, Pending,
};
use fantoch::id::{Dot, ProcessId, Rifl};
use fantoch::kvs::{KVOp, KVStore, Key};
use serde::{Deserialize, Serialize};
use tracing::instrument;

#[derive(Clone)]
pub struct TableExecutor {
    execute_at_commit: bool,
    table: MultiVotesTable,
    store: KVStore,
    pending: Pending,
    metrics: ExecutorMetrics,
}

impl Executor for TableExecutor {
    type ExecutionInfo = TableExecutionInfo;

    fn new(process_id: ProcessId, config: Config, executors: usize) -> Self {
        // TODO this is specific to newt
        let (_, _, stability_threshold) = config.newt_quorum_sizes();
        let table =
            MultiVotesTable::new(process_id, config.n(), stability_threshold);
        let store = KVStore::new();
        // aggregate results if the number of executors is 1
        let aggregate = executors == 1;
        let pending = Pending::new(aggregate);
        let metrics = ExecutorMetrics::new();

        Self {
            execute_at_commit: config.execute_at_commit(),
            table,
            store,
            pending,
            metrics,
        }
    }

    fn wait_for(&mut self, cmd: &Command) {
        // start command in pending
        assert!(self.pending.wait_for(cmd));
    }

    fn wait_for_rifl(&mut self, rifl: Rifl) {
        self.pending.wait_for_rifl(rifl);
    }

    fn handle(&mut self, info: Self::ExecutionInfo) -> Vec<ExecutorResult> {
        // handle each new info by updating the votes table and execute ready
        // commands
        match info {
            TableExecutionInfo::Votes {
                dot,
                clock,
                rifl,
                key,
                op,
                votes,
            } => {
                if self.execute_at_commit {
                    self.execute(key, std::iter::once((rifl, op)))
                } else {
                    let to_execute =
                        self.table.add_votes(dot, clock, rifl, &key, op, votes);
                    self.execute(key, to_execute)
                }
            }
            TableExecutionInfo::DetachedVotes { key, votes } => {
                if self.execute_at_commit {
                    vec![]
                } else {
                    let to_execute = self.table.add_detached_votes(&key, votes);
                    self.execute(key, to_execute)
                }
            }
        }
    }

    fn parallel() -> bool {
        true
    }

    fn metrics(&self) -> &ExecutorMetrics {
        &self.metrics
    }
}

impl TableExecutor {
    #[instrument(skip(self, key, to_execute))]
    fn execute<I>(&mut self, key: Key, to_execute: I) -> Vec<ExecutorResult>
    where
        I: Iterator<Item = (Rifl, KVOp)>,
    {
        to_execute
            .filter_map(|(rifl, op)| {
                // execute op in the `KVStore`
                let op_result = self.store.execute(&key, op);

                // add partial result to `Pending`
                self.pending.add_partial(rifl, || (key.clone(), op_result))
            })
            .collect()
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TableExecutionInfo {
    Votes {
        dot: Dot,
        clock: u64,
        rifl: Rifl,
        key: Key,
        op: KVOp,
        votes: Vec<VoteRange>,
    },
    DetachedVotes {
        key: Key,
        votes: Vec<VoteRange>,
    },
}

impl TableExecutionInfo {
    pub fn votes(
        dot: Dot,
        clock: u64,
        rifl: Rifl,
        key: Key,
        op: KVOp,
        votes: Vec<VoteRange>,
    ) -> Self {
        TableExecutionInfo::Votes {
            dot,
            clock,
            rifl,
            key,
            op,
            votes,
        }
    }

    pub fn detached_votes(key: Key, votes: Vec<VoteRange>) -> Self {
        TableExecutionInfo::DetachedVotes { key, votes }
    }
}

impl MessageKey for TableExecutionInfo {
    fn key(&self) -> Option<&Key> {
        let key = match self {
            TableExecutionInfo::Votes { key, .. } => key,
            TableExecutionInfo::DetachedVotes { key, .. } => key,
        };
        Some(&key)
    }
}
