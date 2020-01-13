use crate::command::{Command, CommandResult, Pending};
use crate::config::Config;
use crate::executor::table::MultiVotesTable;
use crate::executor::Executor;
use crate::id::Dot;
use crate::kvs::KVStore;
use crate::protocol::common::table::{ProcessVotes, Votes};

pub struct TableExecutor {
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
        let pending = Pending::new();

        TableExecutor {
            table,
            store,
            pending,
        }
    }

    fn register(&mut self, cmd: &Command) {
        // start command in `Pending`
        assert!(self.pending.start(&cmd));
    }

    fn handle(&mut self, infos: Vec<Self::ExecutionInfo>) -> Vec<CommandResult> {
        // borrow everything we'll need
        let table = &mut self.table;
        let store = &mut self.store;
        let pending = &mut self.pending;

        infos
            .into_iter()
            .flat_map(|info| {
                // handle each new info by updating the votes table
                let to_execute = match info {
                    TableExecutionInfo::Votes {
                        dot,
                        cmd,
                        clock,
                        votes,
                    } => table.add_votes(dot, cmd, clock, votes),
                    TableExecutionInfo::PhantomVotes { process_votes } => {
                        table.add_phantom_votes(process_votes)
                    }
                };

                // get new commands that are ready to be executed
                to_execute
                    .into_iter()
                    // flatten each pair with a key and list of ready partial operations
                    .flat_map(|(key, ops)| {
                        ops.into_iter()
                            .map(move |(rifl, op)| (key.clone(), rifl, op))
                    })
                    .filter_map(|(key, rifl, op)| {
                        // execute op in the `KVStore`
                        let op_result = store.execute(&key, op);

                        // add partial result to `Pending`
                        pending.add_partial(rifl, key, op_result)
                    })
                    // TODO can we avoid collecting here?
                    .collect::<Vec<_>>()
                    .into_iter()
            })
            .collect()
    }

    fn show_metrics(&self) {
        self.table.show_metrics();
    }
}

#[derive(Debug)]
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
