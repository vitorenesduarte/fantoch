use crate::command::{Command, CommandResult};
use crate::config::Config;
use crate::executor::graph::DependencyGraph;
use crate::executor::Executor;
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

    fn register(&mut self, cmd: &Command) {
        // start command in pending
        assert!(self.pending.insert(cmd.rifl()));
    }

    fn handle(&mut self, infos: Vec<Self::ExecutionInfo>) -> Vec<CommandResult> {
        // borrow everything we'll need
        let graph = &mut self.graph;
        let store = &mut self.store;
        let pending = &mut self.pending;

        infos
            .into_iter()
            .flat_map(|info| {
                // handle each new info
                graph.add(info.dot, info.cmd, info.clock);

                // get more commands that are ready to be executed
                let to_execute = graph.commands_to_execute();

                // execute them all
                to_execute
                    .into_iter()
                    .filter_map(|cmd| {
                        // get command rifl
                        let rifl = cmd.rifl();
                        // execute the command
                        let result = store.execute_command(cmd);

                        // if it was pending locally, then it's from a client of this process
                        if pending.remove(&rifl) {
                            Some(result)
                        } else {
                            None
                        }
                    })
                    // TODO can we avoid collecting here?
                    .collect::<Vec<_>>()
                    .into_iter()
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
