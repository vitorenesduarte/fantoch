use crate::executor::pred::PredecessorsGraph;
use crate::protocol::common::pred::{CaesarDeps, Clock};
use fantoch::command::Command;
use fantoch::config::Config;
use fantoch::executor::{
    ExecutionOrderMonitor, Executor, ExecutorMetrics, ExecutorResult,
};
use fantoch::id::{Dot, ProcessId, ShardId};
use fantoch::kvs::KVStore;
use fantoch::protocol::{CommittedAndExecuted, MessageIndex};
use fantoch::time::SysTime;
use fantoch::trace;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;

pub struct PredecessorsExecutor {
    process_id: ProcessId,
    shard_id: ShardId,
    graph: PredecessorsGraph,
    store: KVStore,
    to_clients: VecDeque<ExecutorResult>,
}

impl Executor for PredecessorsExecutor {
    type ExecutionInfo = PredecessorsExecutionInfo;

    fn new(process_id: ProcessId, shard_id: ShardId, config: Config) -> Self {
        let graph = PredecessorsGraph::new(process_id, &config);
        let store = KVStore::new(config.executor_monitor_execution_order());
        let to_clients = Default::default();
        Self {
            process_id,
            shard_id,
            graph,
            store,
            to_clients,
        }
    }

    fn handle(&mut self, info: PredecessorsExecutionInfo, time: &dyn SysTime) {
        // handle new command
        self.graph
            .add(info.dot, info.cmd, info.clock, info.deps, time);

        // get more commands that are ready to be executed
        while let Some(cmd) = self.graph.command_to_execute() {
            trace!(
                "p{}: PredecessorsExecutor::comands_to_execute {:?} | time = {}",
                self.process_id,
                cmd.rifl(),
                time.millis()
            );
            self.execute(cmd);
        }
    }

    fn to_clients(&mut self) -> Option<ExecutorResult> {
        self.to_clients.pop_front()
    }

    fn executed(
        &mut self,
        _time: &dyn SysTime,
    ) -> Option<CommittedAndExecuted> {
        let committed_and_executed = self.graph.committed_and_executed();
        trace!(
            "p{}: PredecessorsExecutor::executed {:?} | time = {}",
            self.process_id,
            committed_and_executed,
            _time.millis()
        );
        Some(committed_and_executed)
    }

    fn parallel() -> bool {
        false
    }

    fn metrics(&self) -> &ExecutorMetrics {
        &self.graph.metrics()
    }

    fn monitor(&self) -> Option<ExecutionOrderMonitor> {
        self.store.monitor().cloned()
    }
}

impl PredecessorsExecutor {
    fn execute(&mut self, cmd: Command) {
        // execute the command
        let results = cmd.execute(self.shard_id, &mut self.store);
        self.to_clients.extend(results);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredecessorsExecutionInfo {
    dot: Dot,
    cmd: Command,
    clock: Clock,
    deps: Arc<CaesarDeps>,
}

impl PredecessorsExecutionInfo {
    pub fn new(
        dot: Dot,
        cmd: Command,
        clock: Clock,
        deps: Arc<CaesarDeps>,
    ) -> Self {
        Self {
            dot,
            cmd,
            clock,
            deps,
        }
    }
}

impl MessageIndex for PredecessorsExecutionInfo {
    fn index(&self) -> Option<(usize, usize)> {
        None
    }
}
