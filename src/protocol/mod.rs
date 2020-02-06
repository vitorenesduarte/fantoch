// This module contains the definition of `BaseProcess`.
mod base;

// This module contains common data-structures between protocols.
pub mod common;

// This module contains the definition of a basic replication protocol that
// waits for f + 1 acks before committing a command. It's for sure inconsistent
// and most likely non-fault-tolerant until we base it on the synod module.
// TODO evolve the synod module so that is allows patterns like Coordinated
// Paxos and Simple Paxos from Mencius. With such patterns we can make this
// protocol fault-tolerant (but still inconsistent).
mod basic;

// This module contains the definition of `Atlas`.
mod atlas;

// This module contains the definition of `EPaxos`.
mod epaxos;

// This module contains the definition of `Newt`.
mod newt;

// Re-exports.
pub use atlas::{AtlasLocked, AtlasSequential};
pub use basic::Basic;
pub use epaxos::{EPaxosLocked, EPaxosSequential};
pub use newt::{NewtAtomic, NewtSequential};

pub use base::BaseProcess;

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
        + MessageDot; // TODO why is Sync needed??
    type Executor: Executor + Send;

    fn new(process_id: ProcessId, config: Config) -> Self;

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
    fn to_executor(
        &mut self,
    ) -> Vec<<Self::Executor as Executor>::ExecutionInfo>;

    fn parallel() -> bool;

    fn show_metrics(&self) {
        // by default, nothing to show
    }
}

pub trait MessageDot {
    /// If `None` is returned, then the message is sent to all protocol
    /// processes. In particular, if the protocol is not parallel, the
    /// message is sent to the single protocol process.
    fn dot(&self) -> Option<&Dot> {
        None
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct ToSend<M> {
    pub from: ProcessId,
    pub target: HashSet<ProcessId>,
    pub msg: M,
}
