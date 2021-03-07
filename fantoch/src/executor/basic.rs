use crate::config::Config;
use crate::executor::{
    ExecutionOrderMonitor, Executor, ExecutorMetrics, ExecutorResult,
    MessageKey,
};
use crate::id::{ProcessId, Rifl, ShardId};
use crate::kvs::{KVOp, KVStore, Key};
use crate::time::SysTime;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone)]
pub struct BasicExecutor {
    store: KVStore,
    metrics: ExecutorMetrics,
    to_clients: Vec<ExecutorResult>,
}

impl Executor for BasicExecutor {
    type ExecutionInfo = BasicExecutionInfo;

    fn new(
        _process_id: ProcessId,
        _shard_id: ShardId,
        _config: Config,
    ) -> Self {
        let monitor = false;
        let store = KVStore::new(monitor);
        let metrics = ExecutorMetrics::new();
        let to_clients = Vec::new();

        Self {
            store,
            metrics,
            to_clients,
        }
    }

    fn handle(&mut self, info: Self::ExecutionInfo, _time: &dyn SysTime) {
        let BasicExecutionInfo { rifl, key, ops } = info;
        // take the ops inside the arc if we're the last with a
        // reference to it (otherwise, clone them)
        let ops =
            Arc::try_unwrap(ops).unwrap_or_else(|ops| ops.as_ref().clone());
        // execute op in the `KVStore`
        let partial_results = self.store.execute(&key, ops, rifl);
        self.to_clients
            .push(ExecutorResult::new(rifl, key, partial_results));
    }

    fn to_clients(&mut self) -> Option<ExecutorResult> {
        self.to_clients.pop()
    }

    fn parallel() -> bool {
        true
    }

    fn metrics(&self) -> &ExecutorMetrics {
        &self.metrics
    }

    fn monitor(&self) -> Option<ExecutionOrderMonitor> {
        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BasicExecutionInfo {
    rifl: Rifl,
    key: Key,
    ops: Arc<Vec<KVOp>>,
}

impl BasicExecutionInfo {
    pub fn new(rifl: Rifl, key: Key, ops: Arc<Vec<KVOp>>) -> Self {
        Self { rifl, key, ops }
    }
}

impl MessageKey for BasicExecutionInfo {
    fn key(&self) -> &Key {
        &self.key
    }
}
