use crate::command::Command;
use crate::config::Config;
use crate::executor::pending::Pending;
use crate::executor::{Executor, ExecutorMetrics, ExecutorResult, MessageKey};
use crate::id::{ProcessId, Rifl, ShardId};
use crate::kvs::{KVOp, KVStore, Key};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct BasicExecutor {
    store: KVStore,
    pending: Pending,
    metrics: ExecutorMetrics,
    to_clients: Vec<ExecutorResult>,
}

impl Executor for BasicExecutor {
    type ExecutionInfo = BasicExecutionInfo;

    fn new(
        process_id: ProcessId,
        shard_id: ShardId,
        _config: Config,
        executors: usize,
    ) -> Self {
        let store = KVStore::new();
        // aggregate results if the number of executors is 1
        let aggregate = executors == 1;
        let pending = Pending::new(aggregate, process_id, shard_id);
        let metrics = ExecutorMetrics::new();
        let to_clients = Vec::new();

        Self {
            store,
            pending,
            metrics,
            to_clients,
        }
    }

    fn wait_for(&mut self, cmd: &Command) {
        // start command in pending
        assert!(self.pending.wait_for(cmd));
    }

    fn wait_for_rifl(&mut self, rifl: Rifl) {
        self.pending.wait_for_rifl(rifl);
    }

    fn handle(&mut self, info: Self::ExecutionInfo) {
        let BasicExecutionInfo { rifl, key, op } = info;
        // execute op in the `KVStore`
        let op_result = self.store.execute(&key, op);

        // add partial result to `Pending`
        if let Some(result) =
            self.pending.add_partial(rifl, || (key, op_result))
        {
            self.to_clients.push(result);
        }
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
