// This module contains the implementation of a dependency graph.
mod graph;

// This module contains the implementation of a votes table.
mod table;

// Re-exports.
pub use graph::{GraphExecutionInfo, GraphExecutor};
pub use table::{TableExecutionInfo, TableExecutor};

use crate::command::{Command, CommandResult};
use crate::config::Config;
use std::fmt::Debug;

pub trait Executor {
    type ExecutionInfo: Debug;

    fn new(config: &Config) -> Self;

    fn register(&mut self, cmd: &Command);

    fn handle(&mut self, infos: Vec<Self::ExecutionInfo>) -> Vec<CommandResult>;

    fn show_metrics(&self) {
        // by default, nothing to show
    }
}
