use crate::executor::graph::DependencyGraph;
use fantoch::command::Command;
use fantoch::config::Config;
use fantoch::executor::{Executor, ExecutorMetrics, ExecutorResult};
use fantoch::id::{Dot, ProcessId, ShardId};
use fantoch::kvs::KVStore;
use fantoch::log;
use fantoch::protocol::MessageIndex;
use fantoch::time::SysTime;
use fantoch::HashSet;
use serde::{Deserialize, Serialize};
use threshold::{AEClock, VClock};

#[derive(Clone)]
pub struct GraphExecutor {
    executor_index: usize,
    process_id: ProcessId,
    shard_id: ShardId,
    config: Config,
    graph: DependencyGraph,
    store: KVStore,
    to_clients: Vec<ExecutorResult>,
    to_executors: Vec<(ShardId, GraphExecutionInfo)>,
}

impl Executor for GraphExecutor {
    type ExecutionInfo = GraphExecutionInfo;

    fn new(process_id: ProcessId, shard_id: ShardId, config: Config) -> Self {
        // this value will be overwritten
        let executor_index = 0;
        let graph = DependencyGraph::new(process_id, shard_id, &config);
        let store = KVStore::new();
        let to_clients = Vec::new();
        let to_executors = Vec::new();
        Self {
            executor_index,
            process_id,
            shard_id,
            config,
            graph,
            store,
            to_clients,
            to_executors,
        }
    }

    fn set_executor_index(&mut self, index: usize) {
        self.executor_index = index;
        self.graph.set_executor_index(index);
    }

    fn cleanup(&mut self, time: &dyn SysTime) {
        if self.config.shards() > 1 {
            if let Some(self_execution_info) = self.graph.cleanup(time) {
                self.to_executors.push((self.shard_id, self_execution_info))
            }
            self.fetch_actions(time);
        }
    }

    fn handle(&mut self, info: GraphExecutionInfo, time: &dyn SysTime) {
        match info {
            GraphExecutionInfo::Add { dot, cmd, clock } => {
                if self.config.execute_at_commit() {
                    self.execute(cmd);
                } else {
                    // handle new command
                    self.graph.handle_add(dot, cmd, clock, time);
                    self.fetch_actions(time);
                }
            }
            GraphExecutionInfo::AddMine { dot } => {
                self.graph.handle_add_mine(dot, time);
            }
            GraphExecutionInfo::Request { from, dots } => {
                self.graph.handle_request(from, dots, time);
                self.fetch_actions(time);
            }
            GraphExecutionInfo::RequestReply { infos } => {
                self.graph.handle_request_reply(infos, time);
                self.fetch_actions(time);
            }
            GraphExecutionInfo::ExecutedClock { clock } => {
                self.graph.handle_executed_clock(clock, time);
            }
        }
    }

    fn to_clients(&mut self) -> Option<ExecutorResult> {
        self.to_clients.pop()
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
}

impl GraphExecutor {
    fn fetch_actions(&mut self, time: &dyn SysTime) {
        self.fetch_commands_to_execute(time);
        if self.config.shards() > 1 {
            self.fetch_requests(time);
            self.fetch_request_replies(time);
        }
    }

    fn fetch_commands_to_execute(&mut self, _time: &dyn SysTime) {
        // get more commands that are ready to be executed
        while let Some(cmd) = self.graph.command_to_execute() {
            log!(
                "p{}: @{} GraphExecutor::fetch_comands_to_execute {:?} | time = {}",
                self.process_id,
                self.executor_index,
                cmd.rifl(),
                _time.millis()
            );
            self.execute(cmd);
        }
    }

    fn fetch_requests(&mut self, _time: &dyn SysTime) {
        for (to, dots) in self.graph.requests() {
            log!(
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
            log!(
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
    AddMine {
        dot: Dot,
    },
    Request {
        from: ShardId,
        dots: HashSet<Dot>,
    },
    RequestReply {
        infos: Vec<super::RequestReply>,
    },
    ExecutedClock {
        clock: AEClock<ProcessId>,
    },
}

impl GraphExecutionInfo {
    pub fn add(dot: Dot, cmd: Command, clock: VClock<ProcessId>) -> Self {
        Self::Add { dot, cmd, clock }
    }

    pub fn add_mine(dot: Dot) -> Self {
        Self::AddMine { dot }
    }

    fn request(from: ShardId, dots: HashSet<Dot>) -> Self {
        Self::Request { from, dots }
    }

    fn request_reply(infos: Vec<super::RequestReply>) -> Self {
        Self::RequestReply { infos }
    }
}

impl MessageIndex for GraphExecutionInfo {
    fn index(&self) -> Option<(usize, usize)> {
        const MAX_EXECUTORS: usize = 100;
        const fn executor_index_no_shift() -> Option<(usize, usize)> {
            // when there's no shift, the index must be 0
            let shift = 0;
            let index = 0;
            Some((shift, index))
        }

        fn executor_random_index_shift() -> Option<(usize, usize)> {
            use rand::Rng;
            // if there's a shift, we select a random worker
            let shift = 1;
            let index = rand::thread_rng().gen_range(0, MAX_EXECUTORS);
            Some((shift, index))
        }

        match self {
            Self::Add { .. } => executor_index_no_shift(),
            Self::AddMine { .. } => executor_index_no_shift(),
            Self::Request { .. } => executor_random_index_shift(),
            Self::RequestReply { .. } => executor_index_no_shift(),
            Self::ExecutedClock { .. } => None, // send to all workers
        }
    }
}
