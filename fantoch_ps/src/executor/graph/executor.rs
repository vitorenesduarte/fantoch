use crate::executor::graph::DependencyGraph;
use fantoch::command::Command;
use fantoch::config::Config;
use fantoch::executor::{
    Executor, ExecutorMetrics, ExecutorResult, MessageKey,
};
use fantoch::id::{Dot, ProcessId, Rifl, ShardId};
use fantoch::kvs::KVStore;
use fantoch::HashSet;
use serde::{Deserialize, Serialize};
use threshold::VClock;

#[derive(Clone)]
pub struct GraphExecutor {
    shard_id: ShardId,
    config: Config,
    graph: DependencyGraph,
    store: KVStore,
    pending: HashSet<Rifl>,
    metrics: ExecutorMetrics,
    to_clients: Vec<ExecutorResult>,
}

impl Executor for GraphExecutor {
    type ExecutionInfo = GraphExecutionInfo;

    fn new(
        process_id: ProcessId,
        shard_id: ShardId,
        config: Config,
        executors: usize,
    ) -> Self {
        assert_eq!(executors, 1);

        let graph = DependencyGraph::new(process_id, shard_id, &config);
        let store = KVStore::new();
        let pending = HashSet::new();
        let metrics = ExecutorMetrics::new();
        let to_clients = Vec::new();
        Self {
            shard_id,
            config,
            graph,
            store,
            pending,
            metrics,
            to_clients,
        }
    }

    fn wait_for(&mut self, cmd: &Command) {
        self.wait_for_rifl(cmd.rifl());
    }

    fn wait_for_rifl(&mut self, rifl: Rifl) {
        // start command in pending
        assert!(self.pending.insert(rifl));
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
        // get command rifl
        let rifl = cmd.rifl();
        // execute the command
        let result = cmd.execute(self.shard_id, &mut self.store);

        // if it was pending locally, then it's from a client of this
        // process
        if self.pending.remove(&rifl) {
            self.to_clients.push(ExecutorResult::Ready(result));
        }
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
