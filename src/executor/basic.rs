use crate::command::Command;
use crate::config::Config;
use crate::executor::{ExecutionInfoKey, Executor, ExecutorResult};
use crate::id::Rifl;
use crate::kvs::KVStore;
use std::collections::HashSet;

impl ExecutionInfoKey for BasicExecutionInfo {}
pub type BasicExecutionInfo = Command;

pub struct BasicExecutor {
    config: Config,
    store: KVStore,
    pending: HashSet<Rifl>,
}

impl Executor for BasicExecutor {
    type ExecutionInfo = BasicExecutionInfo;

    fn new(config: Config) -> Self {
        let store = KVStore::new();
        let pending = HashSet::new();

        Self {
            config,
            store,
            pending,
        }
    }

    fn register(&mut self, rifl: Rifl, _key_count: usize) {
        // start command in pending
        assert!(self.pending.insert(rifl));
    }

    fn handle(&mut self, cmd: Self::ExecutionInfo) -> ExecutorResult {
        // get command rifl
        let rifl = cmd.rifl();
        // execute the command
        let result = cmd.execute(&mut self.store);

        // if it was pending locally, then it's from a client of this process
        let ready = if self.pending.remove(&rifl) {
            vec![result]
        } else {
            Vec::new()
        };
        ExecutorResult::Ready(ready)
    }

    fn parallel(&self) -> bool {
        self.config.parallel_executor()
    }
}
