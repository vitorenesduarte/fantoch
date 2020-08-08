use crate::config::Config;
use crate::executor::{Executor, ExecutorMetrics, ExecutorResult, MessageKey};
use crate::id::{ProcessId, Rifl, ShardId};
use crate::kvs::{KVOp, KVStore, Key};
use serde::{Deserialize, Serialize};

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
        let store = KVStore::new();
        let metrics = ExecutorMetrics::new();
        let to_clients = Vec::new();

        Self {
            store,
            metrics,
            to_clients,
        }
    }

    fn handle(&mut self, info: Self::ExecutionInfo) {
        let BasicExecutionInfo { rifl, key, op } = info;
        // execute op in the `KVStore`
        let op_result = self.store.execute(&key, op);
        self.to_clients
            .push(ExecutorResult::new(rifl, key, op_result));
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
