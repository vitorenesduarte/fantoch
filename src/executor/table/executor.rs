use crate::command::Command;
use crate::config::Config;
use crate::executor::pending::Pending;
use crate::executor::table::MultiVotesTable;
use crate::executor::{Executor, ExecutorResult, MessageKey};
use crate::id::{Dot, Rifl};
use crate::kvs::{KVOp, KVStore, Key};
use crate::protocol::common::table::VoteRange;
use serde::{Deserialize, Serialize};

pub struct TableExecutor {
    execute_at_commit: bool,
    table: MultiVotesTable,
    store: KVStore,
    pending: Pending,
}

impl Executor for TableExecutor {
    type ExecutionInfo = TableExecutionInfo;

    fn new(config: Config) -> Self {
        // TODO this is specific to newt
        let (_, _, stability_threshold) = config.newt_quorum_sizes();
        let table = MultiVotesTable::new(config.n(), stability_threshold);
        let store = KVStore::new();
        // aggregate results if the number of executors is 1
        let aggregate = config.executors() == 1;
        let pending = Pending::new(aggregate);

        Self {
            execute_at_commit: config.execute_at_commit(),
            table,
            store,
            pending,
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
            TableExecutionInfo::PhantomVotes { key, votes } => {
                if self.execute_at_commit {
                    vec![]
                } else {
                    let to_execute = self.table.add_phantom_votes(&key, votes);
                    self.execute(key, to_execute)
                }
            }
        }
    }

    fn parallel() -> bool {
        true
    }
}

impl TableExecutor {
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TableExecutionInfo {
    Votes {
        dot: Dot,
        clock: u64,
        rifl: Rifl,
        key: Key,
        op: KVOp,
        votes: Vec<VoteRange>,
    },
    PhantomVotes {
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

    pub fn phantom_votes(key: Key, votes: Vec<VoteRange>) -> Self {
        TableExecutionInfo::PhantomVotes { key, votes }
    }
}

impl MessageKey for TableExecutionInfo {
    fn key(&self) -> Option<&Key> {
        let key = match self {
            TableExecutionInfo::Votes { key, .. } => key,
            TableExecutionInfo::PhantomVotes { key, .. } => key,
        };
        Some(&key)
    }
}
