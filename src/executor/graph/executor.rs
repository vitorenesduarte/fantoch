use crate::command::Command;
use crate::config::Config;
use crate::executor::graph::DependencyGraph;
use crate::executor::{Executor, ExecutorResult, MessageKey};
use crate::id::{Dot, ProcessId, Rifl};
use crate::kvs::KVStore;
use std::collections::HashSet;
use threshold::VClock;

pub struct GraphExecutor {
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
        // handle each new info
        self.graph.add(info.dot, info.cmd, info.clock);

        // get more commands that are ready to be executed
        let to_execute = self.graph.commands_to_execute();

        // execute them all
        to_execute
            .into_iter()
            .filter_map(|cmd| {
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
            })
            .collect()
    }

    fn show_metrics(&self) {
        self.graph.show_metrics();
    }
}

#[derive(Debug, Clone)]
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
