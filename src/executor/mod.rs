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

use crate::command::CommandResult;
use crate::config::Config;
use crate::id::{ClientId, Rifl};
use crate::kvs::{KVOpResult, Key};
use std::fmt::Debug;

pub trait Executor {
    type ExecutionInfo: Debug + Send + ExecutionInfoKey;

    fn new(config: Config) -> Self;

    fn register(&mut self, rifl: Rifl, key_count: usize);

    fn handle(&mut self, infos: Self::ExecutionInfo) -> Vec<ExecutorResult>;

    fn parallel(&self) -> bool;

    fn show_metrics(&self) {
        // by default, nothing to show
    }
}

pub trait ExecutionInfoKey {
    /// If `None` is returned, then the execution info is sent to all executor processes.
    /// In particular, if the executor is not parallel, the execution info is sent to the single
    /// executor process.
    fn key(&self) -> Option<Key> {
        None
    }
}

pub enum ExecutorResult {
    /// this contains a command result that is
    Ready(CommandResult),
    Partial(Rifl, Key, KVOpResult),
}

impl ExecutorResult {
    /// Check which client should receive this result.
    pub fn client(&self) -> ClientId {
        match self {
            ExecutorResult::Ready(cmd_result) => cmd_result.rifl().source(),
            ExecutorResult::Partial(rifl, _, _) => rifl.source(),
        }
    }

    /// Extracts a ready `CommandResult` from self. Panics if not ready.
    pub fn unwrap_ready(self) -> CommandResult {
        match self {
            ExecutorResult::Ready(cmd_result) => cmd_result,
            ExecutorResult::Partial(_, _, _) => panic!(
                "called `ExecutorResult::unwrap_ready()` on a `ExecutorResult::Partial` value"
            ),
        }
    }
}
