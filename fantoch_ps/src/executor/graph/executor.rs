use crate::executor::graph::DependencyGraph;
use fantoch::command::Command;
use fantoch::config::Config;
use fantoch::executor::{Executor, ExecutorMetrics, ExecutorResult};
use fantoch::id::{Dot, ProcessId, ShardId};
use fantoch::kvs::KVStore;
use fantoch::protocol::MessageIndex;
use fantoch::time::SysTime;
use serde::{Deserialize, Serialize};
use threshold::VClock;

#[derive(Clone)]
pub struct GraphExecutor {
    process_id: ProcessId,
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
            process_id,
            shard_id,
            config,
            graph,
            store,
            metrics,
            to_clients,
        }
    }

    fn set_executor_index(&mut self, index: usize) {
        self.graph.set_executor_index(index);
    }

    fn cleanup(&mut self, time: &dyn SysTime) {
        self.graph.cleanup(time);
        self.fetch_to_execute();
    }

    fn handle(&mut self, info: GraphExecutionInfo, time: &dyn SysTime) {
        match info {
            GraphExecutionInfo::Add {
                dot,
                cmd,
                clock,
                is_mine,
            } => {
                if self.config.execute_at_commit() {
                    self.execute(cmd);
                } else {
                    // handle new command
                    self.graph.add(dot, cmd, clock, is_mine, time);
                    self.fetch_to_execute();
                }
            }
        }
    }

    fn to_clients(&mut self) -> Option<ExecutorResult> {
        self.to_clients.pop()
    }

    fn max_executors() -> Option<usize> {
        Some(2)
    }

    fn metrics(&self) -> &ExecutorMetrics {
        &self.metrics
    }
}

impl GraphExecutor {
    fn fetch_to_execute(&mut self) {
        // get more commands that are ready to be executed
        while let Some(cmd) = self.graph.command_to_execute() {
            self.execute(cmd);
        }
    }

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
pub enum GraphExecutionInfo {
    Add {
        dot: Dot,
        cmd: Command,
        clock: VClock<ProcessId>,
        is_mine: bool,
    },
}

impl GraphExecutionInfo {
    pub fn add(
        dot: Dot,
        cmd: Command,
        clock: VClock<ProcessId>,
        shard_id: &ShardId,
    ) -> Self {
        let is_mine = cmd.replicated_by(shard_id);
        Self::Add {
            dot,
            cmd,
            clock,
            is_mine,
        }
    }
}

impl MessageIndex for GraphExecutionInfo {
    fn index(&self) -> Option<(usize, usize)> {
        use fantoch::run::worker_index_no_shift;
        match self {
            Self::Add { is_mine, .. } => {
                let index = if *is_mine { 0 } else { 1 };
                worker_index_no_shift(index)
            }
        }
    }
}
