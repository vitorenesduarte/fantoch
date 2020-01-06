// This module contains the definition of `BaseProcess`.
mod base;

// This module contains common data-structures between protocols.
mod common;

// This module contains the definition of `Atlas`.
mod atlas;

/// This module contains the definition of `EPaxos`.
mod epaxos;

// // This module contains the definition of `Newt`.
// mod newt;

// Re-exports.
pub use atlas::Atlas;
pub use epaxos::EPaxos;
// pub use newt::Newt;

pub use base::BaseProcess;

use crate::command::Command;
use crate::config::Config;
use crate::executor::Executor;
use crate::id::ProcessId;
use crate::planet::{Planet, Region};

pub trait Process {
    type Message: Clone;
    type Executor: Executor;

    fn new(process_id: ProcessId, region: Region, planet: Planet, config: Config) -> Self;

    fn id(&self) -> ProcessId;

    fn discover(&mut self, processes: Vec<(ProcessId, Region)>) -> bool;

    #[must_use]
    fn submit(&mut self, cmd: Command) -> ToSend<Self::Message>;

    #[must_use]
    fn handle(&mut self, from: ProcessId, msg: Self::Message) -> Option<ToSend<Self::Message>>;

    #[must_use]
    fn to_executor(&mut self) -> Vec<<Self::Executor as Executor>::ExecutionInfo>;

    fn show_metrics(&self) {
        // by default, nothing to show
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct ToSend<M> {
    pub from: ProcessId,
    pub target: Vec<ProcessId>,
    pub msg: M,
}

// #[cfg(test)]
// fn run()
//     fn run

// }
