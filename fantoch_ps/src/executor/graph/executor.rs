use crate::executor::graph::DependencyGraph;
use fantoch::command::Command;
use fantoch::config::Config;
use fantoch::executor::{Executor, ExecutorMetrics, ExecutorResult};
use fantoch::id::{Dot, ProcessId, ShardId};
use fantoch::kvs::KVStore;
use fantoch::protocol::MessageIndex;
use fantoch::HashSet;
use serde::{Deserialize, Serialize};
use threshold::VClock;

#[derive(Clone)]
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

    fn handle(&mut self, info: GraphExecutionInfo) {
        match info {
            GraphExecutionInfo::Add { dot, cmd, clock } => {
                if self.config.execute_at_commit() {
                    self.execute(cmd);
                } else {
                    // handle new command
                    self.graph.add(dot, cmd, clock);
                    // get more commands that are ready to be executed
                    while let Some(cmd) = self.graph.command_to_execute() {
                        self.execute(cmd);
                    }
                }
            }
            GraphExecutionInfo::Executed(executed) => {
                self.graph.executed_remotely(executed);
            }
        }
    }

    fn to_clients(&mut self) -> Option<ExecutorResult> {
        self.to_clients.pop()
    }

    fn to_executors(&mut self) -> Option<GraphExecutionInfo> {
        self.graph
            .executed_locally()
            .map(GraphExecutionInfo::Executed)
    }

    fn max_executors() -> Option<usize> {
        Some(2)
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
pub enum GraphExecutionInfo {
    Add {
        dot: Dot,
        cmd: Command,
        clock: VClock<ProcessId>,
    },
    Executed(Vec<(Dot, HashSet<ShardId>)>),
}

impl GraphExecutionInfo {
    pub fn add(dot: Dot, cmd: Command, clock: VClock<ProcessId>) -> Self {
        Self::Add { dot, cmd, clock }
    }
}

impl MessageIndex for GraphExecutionInfo {
    fn index(&self) -> Option<(usize, usize)> {
        use fantoch::run::worker_index_no_shift;
        match self {
            Self::Add { .. } => worker_index_no_shift(0),
            Self::Executed { .. } => worker_index_no_shift(1),
        }
    }
}
