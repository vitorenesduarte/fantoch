// This module contains the implementation of a basic executor that executes operations as soon as
// it receives them.
mod basic;

// This module contains the implementation of a dependency graph executor.
mod graph;

// This module contains the implementation of a votes table executor.
mod table;

// Re-exports.
pub use basic::{BasicExecutionInfo, BasicExecutor};
pub use graph::{GraphExecutionInfo, GraphExecutor};
pub use table::{TableExecutionInfo, TableExecutor};

use crate::command::{Command, CommandResult};
use crate::config::Config;
use crate::kvs::Key;
use std::fmt::Debug;

pub trait Executor {
    type ExecutionInfo: Debug + Send + ExecutionKey;

    fn new(config: Config) -> Self;

    fn register(&mut self, cmd: &Command);

    fn handle(&mut self, infos: Self::ExecutionInfo) -> Vec<CommandResult>;

    fn show_metrics(&self) {
        // by default, nothing to show
    }

    fn is_parallel(&self) -> bool {
        false
    }
}

pub trait ExecutionKey {
    /// Parallel executors should implement this, otherwise a single executor will be assumed
    fn key(&self) -> Option<Key> {
        None
    }
}
