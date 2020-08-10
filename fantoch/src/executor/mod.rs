// This module contains the definition of `Pending`.
mod aggregate;

// This module contains the implementation of a basic executor that executes
// operations as soon as it receives them.
mod basic;

// Re-exports.
pub use aggregate::AggregatePending;
pub use basic::{BasicExecutionInfo, BasicExecutor};

use crate::config::Config;
use crate::id::{ProcessId, Rifl, ShardId};
use crate::kvs::{KVOpResult, Key};
use fantoch_prof::metrics::Metrics;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug};

pub trait Executor: Sized {
    // TODO why is Send needed?
    type ExecutionInfo: Debug
        + Clone
        + PartialEq
        + Eq
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + MessageKey; // TODO why is Sync needed??

    fn new(process_id: ProcessId, shard_id: ShardId, config: Config) -> Self;

    fn handle(&mut self, infos: Self::ExecutionInfo);

    #[must_use]
    fn to_clients(&mut self) -> Option<ExecutorResult>;

    #[must_use]
    fn to_clients_iter(&mut self) -> ToClientsIter<'_, Self> {
        ToClientsIter { executor: self }
    }

    #[must_use]
    fn to_executors(&mut self) -> Option<Self::ExecutionInfo> {
        // non-genuine protocols should overwrite this
        None
    }

    fn parallel() -> bool;

    fn metrics(&self) -> &ExecutorMetrics;
}

pub struct ToClientsIter<'a, E> {
    executor: &'a mut E,
}

impl<'a, E> Iterator for ToClientsIter<'a, E>
where
    E: Executor,
{
    type Item = ExecutorResult;

    fn next(&mut self) -> Option<Self::Item> {
        self.executor.to_clients()
    }
}

pub type ExecutorMetrics = Metrics<ExecutorMetricsKind, u64>;

#[derive(Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutorMetricsKind {
    ChainSize,
    ExecutionDelay,
}

impl Debug for ExecutorMetricsKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecutorMetricsKind::ChainSize => write!(f, "chain_size"),
            ExecutorMetricsKind::ExecutionDelay => write!(f, "execution_delay"),
        }
    }
}

pub trait MessageKey {
    /// If `None` is returned, then the message is sent the *single* executor
    /// process. If there's more than one executor, and this function
    /// returns `None`, the runtime will panic.
    fn key(&self) -> Option<&Key> {
        None
    }
}

#[derive(Debug, Clone)]
pub struct ExecutorResult {
    pub rifl: Rifl,
    pub key: Key,
    pub op_result: KVOpResult,
}

impl ExecutorResult {
    pub fn new(rifl: Rifl, key: Key, op_result: KVOpResult) -> Self {
        ExecutorResult {
            rifl,
            key,
            op_result,
        }
    }
}
