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

// Re-exports.
pub use base::BaseProcess;
pub use basic::Basic;
pub use info::{CommandsInfo, Info};

use crate::command::Command;
use crate::config::Config;
use crate::executor::Executor;
use crate::id::{Dot, ProcessId};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashSet;
use std::fmt::Debug;

pub trait Protocol: Clone {
    type Message: Debug
        + Clone
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + MessageIndex; // TODO why is Sync needed??
    type PeriodicEvent: Clone;
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
    ) -> ToSend<Self::Message>;

    #[must_use]
    fn handle(
        &mut self,
        from: ProcessId,
        msg: Self::Message,
    ) -> Option<ToSend<Self::Message>>;

    #[must_use]
    fn handle_event(
        &mut self,
        event: Self::PeriodicEvent,
    ) -> Option<ToSend<Self::Message>>;

    #[must_use]
    fn to_executor(
        &mut self,
    ) -> Vec<<Self::Executor as Executor>::ExecutionInfo>;

    fn parallel() -> bool;

    fn leaderless() -> bool {
        true
    }

    fn show_metrics(&self) {
        // by default, nothing to show
    }
}

pub trait MessageIndex {
    /// This trait is used to decide to which worker some messages should be
    /// forwarded to, ensuring that messages with the same index are forwarded
    /// to the same process. If `None` is returned, then the message is sent to
    /// all workers. In particular, if the protocol is not parallel, the
    /// message is sent to the single protocol worker.
    /// Two types of indexes are supported:
    /// - Index: simple sequence number
    /// - DotIndex: dot index in which the dot sequence will be used as index
    fn index(&self) -> MessageIndexes {
        MessageIndexes::None
    }
}

pub enum MessageIndexes<'a> {
    Index(usize),
    DotIndex(&'a Dot),
    None,
}

#[derive(Clone, PartialEq, Debug)]
pub struct ToSend<M> {
    pub from: ProcessId,
    pub target: HashSet<ProcessId>,
    pub msg: M,
}
