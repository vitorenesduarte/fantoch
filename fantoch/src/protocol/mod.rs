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
pub use info::{CommandsInfo, Info};

use crate::command::Command;
use crate::config::Config;
use crate::executor::Executor;
use crate::id::{Dot, ProcessId};
use crate::metrics::Metrics;
use crate::time::SysTime;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashSet;
use std::fmt::{self, Debug};

pub trait Protocol: Clone {
    type Message: Debug
        + Clone
        + PartialEq
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + MessageIndex; // TODO why is Sync needed??
    type PeriodicEvent: Debug + Clone + Send + Sync + PeriodicEventIndex;
    type Executor: Executor + Send;

    /// Returns a new instance of the protocol and a list of periodic events
    /// (intervals in milliseconds).
    fn new(
        process_id: ProcessId,
        config: Config,
    ) -> (Self, Vec<(Self::PeriodicEvent, u64)>);

    fn id(&self) -> ProcessId;

    fn discover(&mut self, processes: Vec<ProcessId>) -> bool;

    #[must_use]
    fn submit(
        &mut self,
        dot: Option<Dot>,
        cmd: Command,
        time: &dyn SysTime,
    ) -> Vec<Action<Self::Message>>;

    #[must_use]
    fn handle(
        &mut self,
        from: ProcessId,
        msg: Self::Message,
        time: &dyn SysTime,
    ) -> Vec<Action<Self::Message>>;

    #[must_use]
    fn handle_event(
        &mut self,
        event: Self::PeriodicEvent,
        time: &dyn SysTime,
    ) -> Vec<Action<Self::Message>>;

    #[must_use]
    fn to_executor(
        &mut self,
    ) -> Vec<<Self::Executor as Executor>::ExecutionInfo>;

    fn parallel() -> bool;

    fn leaderless() -> bool;

    fn metrics(&self) -> &ProtocolMetrics;
}

pub type ProtocolMetrics = Metrics<ProtocolMetricsKind, u64>;

#[derive(Clone, Hash, PartialEq, Eq)]
pub enum ProtocolMetricsKind {
    FastPath,
    SlowPath,
    Stable,
}

impl Debug for ProtocolMetricsKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ProtocolMetricsKind::FastPath => write!(f, "fast_path"),
            ProtocolMetricsKind::SlowPath => write!(f, "slow_path"),
            ProtocolMetricsKind::Stable => write!(f, "stable"),
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

pub trait PeriodicEventIndex {
    /// Same as `MessageIndex`.
    fn index(&self) -> Option<(usize, usize)>;
}

#[derive(Clone, PartialEq, Debug)]
pub enum Action<M> {
    ToSend { target: HashSet<ProcessId>, msg: M },
    ToForward { msg: M },
}
