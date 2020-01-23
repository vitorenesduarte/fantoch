use crate::command::{Command, CommandResult};
use crate::config::Config;
use crate::executor::{ExecutionKey, Executor};
use crate::id::Rifl;
use crate::kvs::KVStore;
use std::collections::HashSet;

impl ExecutionKey for BasicExecutionInfo {}
pub type BasicExecutionInfo = Command;

pub struct BasicExecutor {
    store: KVStore,
    pending: HashSet<Rifl>,
}

impl Executor for BasicExecutor {
    type ExecutionInfo = BasicExecutionInfo;

    fn new(_config: Config) -> Self {
        let store = KVStore::new();
        let pending = HashSet::new();

        Self { store, pending }
    }

    fn register(&mut self, cmd: &Command) {
        // start command in pending
        assert!(self.pending.insert(cmd.rifl()));
    }

    fn handle(&mut self, cmd: Self::ExecutionInfo) -> Vec<CommandResult> {
        // borrow everything we'll need
        let store = &mut self.store;
        let pending = &mut self.pending;

        // get command rifl
        let rifl = cmd.rifl();
        // execute the command
        let result = store.execute_command(cmd);

        // if it was pending locally, then it's from a client of this process
        if pending.remove(&rifl) {
            vec![result]
        } else {
            Vec::new()
        }
    }
}
