use crate::command::Command;
use crate::config::Config;
use crate::executor::pending::Pending;
use crate::executor::{Executor, ExecutorMetrics, ExecutorResult, MessageKey};
use crate::id::{ProcessId, Rifl};
use crate::kvs::{KVOp, KVStore, Key};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct BasicExecutor {
    store: KVStore,
    pending: Pending,
    metrics: ExecutorMetrics,
}

impl Executor for BasicExecutor {
    type ExecutionInfo = BasicExecutionInfo;

    fn new(_process_id: ProcessId, config: Config, executors: usize) -> Self {
        let store = KVStore::new();
        // aggregate results if the number of executors is 1
        let aggregate = executors == 1;
        let pending = Pending::new(aggregate);
        let metrics = ExecutorMetrics::new();

        Self {
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
        let BasicExecutionInfo { rifl, key, op } = info;
        // execute op in the `KVStore`
        let op_result = self.store.execute(&key, op);

        // add partial result to `Pending`
        if let Some(result) =
            self.pending.add_partial(rifl, || (key, op_result))
        {
            vec![result]
        } else {
            Vec::new()
        }
    }

    fn parallel() -> bool {
        true
    }

    fn metrics(&self) -> &ExecutorMetrics {
        &self.metrics
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

impl MessageKey for BasicExecutionInfo {
    fn key(&self) -> Option<&Key> {
        Some(&self.key)
    }
}
