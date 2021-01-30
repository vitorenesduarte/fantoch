// This module contains the implementation of data structured used to hold info
// about commands.
mod info;

// This module contains the definition of `BaseProcess`.
mod base;

// This module contains the definition of a basic replication protocol that
// waits for f + 1 acks before committing a command. It's for sure inconsistent
// and most likely non-fault-tolerant until we base it on the synod module.
// TODO evolve the synod module so that is allows patterns like Coordinated
// Paxos and Simple Paxos from Mencius. With such patterns we can make this
// protocol fault-tolerant (but still inconsistent).
mod basic;

// This module contains common functionality from tracking when it's safe to
// garbage-collect a command, i.e., when it's been committed at all processes.
mod gc;

// Re-exports.
pub use base::BaseProcess;
pub use basic::Basic;
pub use gc::GCTrack;
pub use info::{Info, LockedCommandsInfo, SequentialCommandsInfo};

use crate::command::Command;
use crate::config::Config;
use crate::executor::Executor;
use crate::id::{Dot, ProcessId, ShardId};
use crate::time::SysTime;
use crate::{HashMap, HashSet};
use fantoch_prof::metrics::Metrics;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug};
use std::time::Duration;
use threshold::AEClock;

// Compact representation of which `Dot`s have been executed.
pub type Executed = AEClock<ProcessId>;

pub trait Protocol: Debug + Clone {
    type Message: Debug
        + Clone
        + PartialEq
        + Eq
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + MessageIndex; // TODO why is Sync needed??
    type PeriodicEvent: Debug + Clone + Send + Sync + MessageIndex + Eq;
    type Executor: Executor + Send;

    /// Returns a new instance of the protocol and a list of periodic events.
    fn new(
        process_id: ProcessId,
        shard_id: ShardId,
        config: Config,
    ) -> (Self, Vec<(Self::PeriodicEvent, Duration)>);

    fn id(&self) -> ProcessId;

    fn shard_id(&self) -> ShardId;

    fn discover(
        &mut self,
        processes: Vec<(ProcessId, ShardId)>,
    ) -> (bool, HashMap<ShardId, ProcessId>);

    fn submit(&mut self, dot: Option<Dot>, cmd: Command, time: &dyn SysTime);

    fn handle(
        &mut self,
        from: ProcessId,
        from_shard_id: ShardId,
        msg: Self::Message,
        time: &dyn SysTime,
    );

    fn handle_event(&mut self, event: Self::PeriodicEvent, time: &dyn SysTime);

    fn handle_executed(&mut self, _executed: Executed, _time: &dyn SysTime) {
        // protocols interested in handling this type of notifications at the
        // worker `GC_WORKER_INDEX` (see fantoch::run::prelude) should overwrite
        // this
    }

    #[must_use]
    fn to_processes(&mut self) -> Option<Action<Self>>;

    #[must_use]
    fn to_processes_iter(&mut self) -> ToProcessesIter<'_, Self> {
        ToProcessesIter { process: self }
    }

    #[must_use]
    fn to_executors(
        &mut self,
    ) -> Option<<Self::Executor as Executor>::ExecutionInfo>;

    #[must_use]
    fn to_executors_iter(&mut self) -> ToExecutorsIter<'_, Self> {
        ToExecutorsIter { process: self }
    }

    fn parallel() -> bool;

    fn leaderless() -> bool;

    fn metrics(&self) -> &ProtocolMetrics;
}

pub struct ToProcessesIter<'a, P> {
    process: &'a mut P,
}

impl<'a, P> Iterator for ToProcessesIter<'a, P>
where
    P: Protocol,
{
    type Item = Action<P>;

    fn next(&mut self) -> Option<Self::Item> {
        self.process.to_processes()
    }
}

pub struct ToExecutorsIter<'a, P> {
    process: &'a mut P,
}

impl<'a, P> Iterator for ToExecutorsIter<'a, P>
where
    P: Protocol,
{
    type Item = <P::Executor as Executor>::ExecutionInfo;

    fn next(&mut self) -> Option<Self::Item> {
        self.process.to_executors()
    }
}

pub type ProtocolMetrics = Metrics<ProtocolMetricsKind>;

#[derive(Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProtocolMetricsKind {
    FastPath,
    SlowPath,
    Stable,
    CommitLatency,
    WaitConditionDelay,
    CommittedDepsLen,
}

impl Debug for ProtocolMetricsKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolMetricsKind::FastPath => write!(f, "fast_path"),
            ProtocolMetricsKind::SlowPath => write!(f, "slow_path"),
            ProtocolMetricsKind::Stable => write!(f, "stable"),
            ProtocolMetricsKind::CommitLatency => {
                write!(f, "commit_latency")
            }
            ProtocolMetricsKind::WaitConditionDelay => {
                write!(f, "wait_condition_delay")
            }
            ProtocolMetricsKind::CommittedDepsLen => {
                write!(f, "committed_deps_len")
            }
        }
    }
}

pub trait MessageIndex {
    /// This trait is used to decide to which worker some messages should be
    /// forwarded to, ensuring that messages with the same index are forwarded
    /// to the same process. If `None` is returned, then the message is sent to
    /// all workers. In particular, if the protocol is not parallel, the
    /// message is sent to the single protocol worker.
    ///
    /// There only 2 types of indexes are supported:
    /// - Some((reserved, index)): `index` will be used to compute working index
    ///   making sure that index is higher than `reserved`
    /// - None: no indexing; message will be sent to all workers
    fn index(&self) -> Option<(usize, usize)>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action<P: Protocol> {
    ToSend {
        target: HashSet<ProcessId>,
        msg: <P as Protocol>::Message,
    },
    ToForward {
        msg: <P as Protocol>::Message,
    },
}
