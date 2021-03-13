use crate::executor::pred::PredecessorsGraph;
use crate::protocol::common::pred::{CaesarDots, Clock};
use fantoch::command::Command;
use fantoch::config::Config;
use fantoch::executor::{
    ExecutionOrderMonitor, Executor, ExecutorMetrics, ExecutorResult,
};
use fantoch::id::{Dot, ProcessId, ShardId};
use fantoch::protocol::{Committed, Executed, MessageIndex};
use fantoch::time::SysTime;
use fantoch::trace;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone)]
pub struct PredecessorsExecutor {
    executor_index: usize,
    process_id: ProcessId,
    config: Config,
    graph: PredecessorsGraph,
}

impl Executor for PredecessorsExecutor {
    type ExecutionInfo = PredecessorsExecutionInfo;

    fn new(process_id: ProcessId, shard_id: ShardId, config: Config) -> Self {
        // this value will be overwritten
        let executor_index = 0;
        let graph = PredecessorsGraph::new(process_id, shard_id, &config);
        Self {
            executor_index,
            process_id,
            config,
            graph,
        }
    }

    fn set_executor_index(&mut self, index: usize) {
        self.executor_index = index;
    }

    fn handle(&mut self, info: PredecessorsExecutionInfo, time: &dyn SysTime) {
        // handle new command
        self.graph
            .add(info.dot, info.cmd, info.clock, info.deps, time);
    }

    fn to_clients(&mut self) -> Option<ExecutorResult> {
        self.graph.to_clients()
    }

    fn committed_and_executed(
        &mut self,
        _time: &dyn SysTime,
    ) -> Option<(Committed, Executed)> {
        if self.executor_index == 0 {
            // only generate this notification on the first executor
            let committed_and_executed = self.graph.committed_and_executed_frontiers();
            trace!(
                "p{}: PredecessorsExecutor::committed_and_executed {:?} | time = {}",
                self.process_id,
                committed_and_executed,
                _time.millis()
            );
            Some(committed_and_executed)
        } else {
            None
        }
    }

    fn parallel() -> bool {
        true
    }

    fn metrics(&self) -> &ExecutorMetrics {
        &self.graph.metrics()
    }

    fn monitor(&self) -> Option<ExecutionOrderMonitor> {
        self.graph.monitor()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredecessorsExecutionInfo {
    dot: Dot,
    cmd: Command,
    clock: Clock,
    deps: Arc<CaesarDots>,
}

impl PredecessorsExecutionInfo {
    pub fn new(
        dot: Dot,
        cmd: Command,
        clock: Clock,
        deps: Arc<CaesarDots>,
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
        Some((0, self.dot.sequence() as usize))
    }
}
