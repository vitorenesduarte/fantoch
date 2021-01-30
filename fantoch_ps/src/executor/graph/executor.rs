use crate::executor::graph::DependencyGraph;
use crate::protocol::common::graph::Dependency;
use fantoch::command::Command;
use fantoch::config::Config;
use fantoch::executor::{
    ExecutionOrderMonitor, Executor, ExecutorMetrics, ExecutorResult,
};
use fantoch::id::{Dot, ProcessId, ShardId};
use fantoch::kvs::KVStore;
use fantoch::protocol::MessageIndex;
use fantoch::time::SysTime;
use fantoch::HashSet;
use fantoch::{debug, trace};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fmt;
use std::iter::FromIterator;
use std::sync::Arc;

#[derive(Clone)]
pub struct GraphExecutor {
    executor_index: usize,
    process_id: ProcessId,
    shard_id: ShardId,
    config: Config,
    graph: DependencyGraph,
    store: KVStore,
    monitor: Option<ExecutionOrderMonitor>,
    to_clients: VecDeque<ExecutorResult>,
    to_executors: Vec<(ShardId, GraphExecutionInfo)>,
}

impl Executor for GraphExecutor {
    type ExecutionInfo = GraphExecutionInfo;

    fn new(process_id: ProcessId, shard_id: ShardId, config: Config) -> Self {
        // this value will be overwritten
        let executor_index = 0;
        let graph = DependencyGraph::new(process_id, shard_id, &config);
        let store = KVStore::new();
        let monitor = if config.executor_monitor_execution_order() {
            Some(ExecutionOrderMonitor::new())
        } else {
            None
        };
        let to_clients = Default::default();
        let to_executors = Default::default();
        Self {
            executor_index,
            process_id,
            shard_id,
            config,
            graph,
            store,
            monitor,
            to_clients,
            to_executors,
        }
    }

    fn set_executor_index(&mut self, index: usize) {
        self.executor_index = index;
        self.graph.set_executor_index(index);
    }

    fn cleanup(&mut self, time: &dyn SysTime) {
        if self.config.shard_count() > 1 {
            self.graph.cleanup(time);
            self.fetch_actions(time);
        }
    }

    fn monitor_pending(&mut self, time: &dyn SysTime) {
        self.graph.monitor_pending(time);
    }

    fn handle(&mut self, info: GraphExecutionInfo, time: &dyn SysTime) {
        match info {
            GraphExecutionInfo::Add { dot, cmd, deps } => {
                if self.config.execute_at_commit() {
                    self.execute(cmd);
                } else {
                    // handle new command
                    let deps = Vec::from_iter(deps);
                    self.graph.handle_add(dot, cmd, deps, time);
                    self.fetch_actions(time);
                }
            }
            GraphExecutionInfo::Request { from, dots } => {
                self.graph.handle_request(from, dots, time);
                self.fetch_actions(time);
            }
            GraphExecutionInfo::RequestReply { infos } => {
                self.graph.handle_request_reply(infos, time);
                self.fetch_actions(time);
            }
            GraphExecutionInfo::Executed { dots } => {
                self.graph.handle_executed(dots, time);
            }
        }
    }

    fn to_clients(&mut self) -> Option<ExecutorResult> {
        self.to_clients.pop_front()
    }

    fn to_executors(&mut self) -> Option<(ShardId, GraphExecutionInfo)> {
        self.to_executors.pop()
    }

    fn parallel() -> bool {
        true
    }

    fn metrics(&self) -> &ExecutorMetrics {
        &self.graph.metrics()
    }

    fn monitor(&self) -> Option<&ExecutionOrderMonitor> {
        self.monitor.as_ref()
    }
}

impl GraphExecutor {
    fn fetch_actions(&mut self, time: &dyn SysTime) {
        self.fetch_commands_to_execute(time);
        if self.config.shard_count() > 1 {
            self.fetch_to_executors(time);
            self.fetch_requests(time);
            self.fetch_request_replies(time);
        }
    }

    fn fetch_commands_to_execute(&mut self, _time: &dyn SysTime) {
        // get more commands that are ready to be executed
        while let Some(cmd) = self.graph.command_to_execute() {
            trace!(
                "p{}: @{} GraphExecutor::comands_to_execute {:?} | time = {}",
                self.process_id,
                self.executor_index,
                cmd.rifl(),
                _time.millis()
            );
            self.execute(cmd);
        }
    }

    fn fetch_to_executors(&mut self, _time: &dyn SysTime) {
        if let Some(added) = self.graph.to_executors() {
            debug!(
                "p{}: @{} GraphExecutor::to_executors {:?} | time = {}",
                self.process_id,
                self.executor_index,
                added,
                _time.millis()
            );
            let executed = GraphExecutionInfo::executed(added);
            self.to_executors.push((self.shard_id, executed));
        }
    }

    fn fetch_requests(&mut self, _time: &dyn SysTime) {
        for (to, dots) in self.graph.requests() {
            trace!(
                "p{}: @{} GraphExecutor::fetch_requests {:?} {:?} | time = {}",
                self.process_id,
                self.executor_index,
                to,
                dots,
                _time.millis()
            );
            let request = GraphExecutionInfo::request(self.shard_id, dots);
            self.to_executors.push((to, request));
        }
    }

    fn fetch_request_replies(&mut self, _time: &dyn SysTime) {
        for (to, infos) in self.graph.request_replies() {
            trace!(
                "p{}: @{} Graph::fetch_request_replies {:?} {:?} | time = {}",
                self.process_id,
                self.executor_index,
                to,
                infos,
                _time.millis()
            );
            let reply = GraphExecutionInfo::request_reply(infos);
            self.to_executors.push((to, reply));
        }
    }

    fn execute(&mut self, cmd: Arc<Command>) {
        // take the command inside the arc if we're the last with a
        // reference to it (otherwise, clone the command)
        let cmd =
            Arc::try_unwrap(cmd).unwrap_or_else(|cmd| cmd.as_ref().clone());
        // execute the command
        let results =
            cmd.execute(self.shard_id, &mut self.store, &mut self.monitor);
        self.to_clients.extend(results);
    }
}

impl fmt::Debug for GraphExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:#?}", self.graph)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GraphExecutionInfo {
    Add {
        dot: Dot,
        cmd: Arc<Command>,
        deps: HashSet<Dependency>,
    },
    Request {
        from: ShardId,
        dots: HashSet<Dot>,
    },
    RequestReply {
        infos: Vec<super::RequestReply>,
    },
    Executed {
        dots: HashSet<Dot>,
    },
}

impl GraphExecutionInfo {
    pub fn add(dot: Dot, cmd: Arc<Command>, deps: HashSet<Dependency>) -> Self {
        Self::Add { dot, cmd, deps }
    }

    fn request(from: ShardId, dots: HashSet<Dot>) -> Self {
        Self::Request { from, dots }
    }

    fn request_reply(infos: Vec<super::RequestReply>) -> Self {
        Self::RequestReply { infos }
    }

    fn executed(dots: HashSet<Dot>) -> Self {
        Self::Executed { dots }
    }
}

impl MessageIndex for GraphExecutionInfo {
    fn index(&self) -> Option<(usize, usize)> {
        const MAIN_INDEX: usize = 0;
        const SECONDARY_INDEX: usize = 1;

        const fn main_executor() -> Option<(usize, usize)> {
            Some((0, MAIN_INDEX))
        }

        const fn secondary_executor() -> Option<(usize, usize)> {
            Some((0, SECONDARY_INDEX))
        }

        match self {
            Self::Add { .. } => main_executor(),
            Self::Request { .. } => secondary_executor(),
            Self::RequestReply { .. } => main_executor(),
            Self::Executed { .. } => secondary_executor(),
        }
    }
}
