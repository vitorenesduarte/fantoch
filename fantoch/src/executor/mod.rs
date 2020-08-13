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
use crate::protocol::MessageIndex;
use crate::time::SysTime;
use crate::util;
use fantoch_prof::metrics::Metrics;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug};

pub trait Executor: Clone {
    // TODO why is Send needed?
    type ExecutionInfo: Debug
        + Clone
        + PartialEq
        + Eq
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + MessageIndex; // TODO why is Sync needed??

    fn new(process_id: ProcessId, shard_id: ShardId, config: Config) -> Self;

    fn set_executor_index(&mut self, _index: usize) {
        // executors interested in the index should overwrite this
    }

    fn cleanup(&mut self, _time: &dyn SysTime) {
        // executors interested in a periodic cleanup should overwrite this
    }

    fn handle(&mut self, infos: Self::ExecutionInfo, time: &dyn SysTime);

    #[must_use]
    fn to_clients(&mut self) -> Option<ExecutorResult>;

    #[must_use]
    fn to_clients_iter(&mut self) -> ToClientsIter<'_, Self> {
        ToClientsIter { executor: self }
    }

    #[must_use]
    fn to_executors(&mut self) -> Option<(ShardId, Self::ExecutionInfo)> {
        // non-genuine protocols should overwrite this
        None
    }

    #[must_use]
    fn to_executors_iter(&mut self) -> ToExecutorsIter<'_, Self> {
        ToExecutorsIter { executor: self }
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

pub struct ToExecutorsIter<'a, E> {
    executor: &'a mut E,
}

impl<'a, E> Iterator for ToExecutorsIter<'a, E>
where
    E: Executor,
{
    type Item = (ShardId, E::ExecutionInfo);

    fn next(&mut self) -> Option<Self::Item> {
        self.executor.to_executors()
    }
}

pub type ExecutorMetrics = Metrics<ExecutorMetricsKind>;

#[derive(Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutorMetricsKind {
    ExecutionDelay,
    ChainSize,
    OutRequests,
    InRequestReplies,
}

impl Debug for ExecutorMetricsKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // general metric
            ExecutorMetricsKind::ExecutionDelay => write!(f, "execution_delay"),
            // graph executor specific
            ExecutorMetricsKind::ChainSize => write!(f, "chain_size"),
            ExecutorMetricsKind::OutRequests => write!(f, "out_requests"),
            ExecutorMetricsKind::InRequestReplies => {
                write!(f, "in_request_replies")
            }
        }
    }
}

pub trait MessageKey {
    /// Returns which `key` the execution info is about.
    fn key(&self) -> &Key;
}

impl<A> MessageIndex for A
where
    A: MessageKey,
{
    fn index(&self) -> Option<(usize, usize)> {
        Some(key_index(self.key()))
    }
}

// The index of a key is its hash
#[allow(clippy::ptr_arg)]
fn key_index(key: &Key) -> (usize, usize) {
    let index = util::key_hash(key) as usize;
    (0, index)
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
