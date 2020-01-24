use crate::config::Config;
use crate::executor::pending::Pending;
use crate::executor::{ExecutionInfoKey, Executor, ExecutorResult};
use crate::id::Rifl;
use crate::kvs::{KVOp, KVStore, Key};

pub struct BasicExecutor {
    config: Config,
    store: KVStore,
    pending: Pending,
}

impl Executor for BasicExecutor {
    type ExecutionInfo = BasicExecutionInfo;

    fn new(config: Config) -> Self {
        let store = KVStore::new();
        let pending = Pending::new(config.parallel_executor());

        Self {
            config,
            store,
            pending,
        }
    }

    fn register(&mut self, rifl: Rifl, key_count: usize) {
        // start command in pending
        assert!(self.pending.register(rifl, key_count));
    }

    fn handle(&mut self, info: Self::ExecutionInfo) -> Vec<ExecutorResult> {
        // execute op in the `KVStore`
        let op_result = self.store.execute(&info.key, info.op);

        // add partial result to `Pending`
        // TODO here we're passing a ref but we actually own `info.key`, so that's a waste for the
        // case where it ends up being cloned in `add_partial`
        if let Some(result) = self.pending.add_partial(info.rifl, &info.key, op_result) {
            vec![result]
        } else {
            Vec::new()
        }
    }

    fn parallel(&self) -> bool {
        self.config.parallel_executor()
    }
}

#[derive(Debug)]
pub struct BasicExecutionInfo {
    rifl: Rifl,
    key: Key,
    op: KVOp,
}

impl BasicExecutionInfo {
    pub fn new(rifl: Rifl, key: Key, op: KVOp) -> Self {
        Self { rifl, key, op }
    }
}

impl ExecutionInfoKey for BasicExecutionInfo {
    fn key(&self) -> Option<&Key> {
        Some(&self.key)
    }
}
