use crate::command::Command;
use crate::config::Config;
use crate::executor::pending::Pending;
use crate::executor::table::MultiVotesTable;
use crate::executor::{Executor, ExecutorResult, MessageKey};
use crate::id::{Dot, Rifl};
use crate::kvs::{KVStore, Key};
use crate::protocol::common::table::{ProcessVotes, Votes};

pub struct TableExecutor {
    config: Config,
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
        // aggregate results if not parallel executor
        let aggregate = !config.parallel_executor();
        let pending = Pending::new(aggregate);

        Self {
            config,
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
        // handle each new info by updating the votes table
        let to_execute = match info {
            TableExecutionInfo::Votes {
                dot,
                cmd,
                clock,
                votes,
            } => self.table.add_votes(dot, cmd, clock, votes),
            TableExecutionInfo::PhantomVotes { process_votes } => {
                self.table.add_phantom_votes(process_votes)
            }
        };

        // get new commands that are ready to be executed
        let mut results = Vec::new();
        for (key, ops) in to_execute {
            for (rifl, op) in ops {
                // execute op in the `KVStore`
                let op_result = self.store.execute(&key, op);

                // add partial result to `Pending`
                if let Some(result) = self.pending.add_partial(rifl, || (key.clone(), op_result)) {
                    results.push(result);
                }
            }
        }
        results
    }

    fn parallel(&self) -> bool {
        self.config.parallel_executor()
    }

    fn show_metrics(&self) {
        self.table.show_metrics();
    }
}

#[derive(Debug, Clone)]
pub enum TableExecutionInfo {
    Votes {
        dot: Dot,
        cmd: Command,
        clock: u64,
        votes: Votes,
    },
    PhantomVotes {
        process_votes: ProcessVotes,
    },
}

impl TableExecutionInfo {
    pub fn votes(dot: Dot, cmd: Command, clock: u64, votes: Votes) -> Self {
        TableExecutionInfo::Votes {
            dot,
            cmd,
            clock,
            votes,
        }
    }

    pub fn phantom_votes(process_votes: ProcessVotes) -> Self {
        TableExecutionInfo::PhantomVotes { process_votes }
    }
}

impl MessageKey for TableExecutionInfo {
    fn key(&self) -> Option<&Key> {
        todo!()
    }
}
