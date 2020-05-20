use crate::executor::graph::DependencyGraph;
use fantoch::command::Command;
use fantoch::config::Config;
use fantoch::executor::{Executor, ExecutorResult, MessageKey};
use fantoch::id::{Dot, ProcessId, Rifl};
use fantoch::kvs::KVStore;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use threshold::VClock;

pub struct GraphExecutor {
    execute_at_commit: bool,
    graph: DependencyGraph,
    store: KVStore,
    pending: HashSet<Rifl>,
}

impl Executor for GraphExecutor {
    type ExecutionInfo = GraphExecutionInfo;

    fn new(config: Config) -> Self {
        let graph = DependencyGraph::new(&config);
        let store = KVStore::new();
        let pending = HashSet::new();
        Self {
            execute_at_commit: config.execute_at_commit(),
            graph,
            store,
            pending,
        }
    }

    fn wait_for(&mut self, cmd: &Command) {
        self.wait_for_rifl(cmd.rifl());
    }

    fn wait_for_rifl(&mut self, rifl: Rifl) {
        // start command in pending
        assert!(self.pending.insert(rifl));
    }

    fn handle(&mut self, info: Self::ExecutionInfo) -> Vec<ExecutorResult> {
        let to_execute = if self.execute_at_commit {
            vec![info.cmd]
        } else {
            // handle each new info
            self.graph.add(info.dot, info.cmd, info.clock);
            // get more commands that are ready to be executed
            self.graph.commands_to_execute()
        };

        // execute them all
        to_execute
            .into_iter()
            .filter_map(|cmd| self.execute(cmd))
            .collect()
    }

    fn parallel() -> bool {
        false
    }
}

impl GraphExecutor {
    fn execute(&mut self, cmd: Command) -> Option<ExecutorResult> {
        // get command rifl
        let rifl = cmd.rifl();
        // execute the command
        let result = cmd.execute(&mut self.store);

        // if it was pending locally, then it's from a client of this
        // process
        if self.pending.remove(&rifl) {
            Some(ExecutorResult::Ready(result))
        } else {
            None
        }
    }

    pub fn show_internal_status(&self) {
        println!("{:?}", self.graph);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
