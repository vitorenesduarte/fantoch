use crate::executor::graph::DependencyGraph;
use fantoch::command::Command;
use fantoch::config::Config;
use fantoch::executor::{
    Executor, ExecutorMetrics, ExecutorResult, MessageKey,
};
use fantoch::id::{Dot, ProcessId, ShardId};
use fantoch::kvs::KVStore;
use serde::{Deserialize, Serialize};
use threshold::VClock;

pub struct GraphExecutor {
    shard_id: ShardId,
    config: Config,
    graph: DependencyGraph,
    store: KVStore,
    metrics: ExecutorMetrics,
    to_clients: Vec<ExecutorResult>,
}

impl Executor for GraphExecutor {
    type ExecutionInfo = GraphExecutionInfo;

    fn new(process_id: ProcessId, shard_id: ShardId, config: Config) -> Self {
        let graph = DependencyGraph::new(process_id, shard_id, &config);
        let store = KVStore::new();
        let metrics = ExecutorMetrics::new();
        let to_clients = Vec::new();
        Self {
            shard_id,
            config,
            graph,
            store,
            metrics,
            to_clients,
        }
    }

    fn handle(&mut self, info: Self::ExecutionInfo) {
        if self.config.execute_at_commit() {
            self.execute(info.cmd);
        } else {
            // handle each new info
            self.graph.add(info.dot, info.cmd, info.clock);
            // get more commands that are ready to be executed
            while let Some(cmd) = self.graph.command_to_execute() {
                self.execute(cmd);
            }
        }
    }

    fn to_clients(&mut self) -> Option<ExecutorResult> {
        self.to_clients.pop()
    }

    fn parallel() -> bool {
        false
    }

    fn metrics(&self) -> &ExecutorMetrics {
        &self.metrics
    }
}

impl GraphExecutor {
    fn execute(&mut self, cmd: Command) {
        // execute the command
        let results = cmd.execute(self.shard_id, &mut self.store);
        self.to_clients.extend(results);
    }

    pub fn show_internal_status(&self) {
        println!("{:?}", self.graph);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphExecutionInfo {
    dot: Dot,
    cmd: Command,
    clock: VClock<ProcessId>,
}

impl GraphExecutionInfo {
    pub fn new(dot: Dot, cmd: Command, clock: VClock<ProcessId>) -> Self {
        Self { dot, cmd, clock }
    }
}

impl MessageKey for GraphExecutionInfo {}
