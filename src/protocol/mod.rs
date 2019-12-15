// This module contains the definition of `BaseProcess`.
mod base;

// This module contains the definition of `Newt`.
mod newt;

// Re-exports.
pub use base::BaseProcess;
pub use newt::Newt;

use crate::command::{Command, CommandResult};
use crate::config::Config;
use crate::id::ProcessId;
use crate::planet::{Planet, Region};

pub trait Process {
    type Message: Clone;

    fn new(process_id: ProcessId, region: Region, planet: Planet, config: Config) -> Self;

    fn id(&self) -> ProcessId;

    fn discover(&mut self, processes: Vec<(ProcessId, Region)>) -> bool;

    fn submit(&mut self, cmd: Command) -> ToSend<Self::Message>;

    fn handle(&mut self, from: ProcessId, msg: Self::Message) -> ToSend<Self::Message>;

    fn commands_ready(&mut self) -> Vec<CommandResult>;
}

#[derive(Clone, PartialEq, Debug)]
pub enum ToSend<M> {
    // nothing to send
    Nothing,
    // new command to be sent to a coordinator
    ToCoordinator(ProcessId, Command),
    // a protocol message to be sent to a list of processes
    ToProcesses(ProcessId, Vec<ProcessId>, M),
}

impl<M> ToSend<M> {
    /// Check if there's nothing to be sent.
    pub fn is_nothing(&self) -> bool {
        match *self {
            ToSend::Nothing => true,
            _ => false,
        }
    }

    /// Check if it's something to be sent to a coordinator.
    pub fn to_coordinator(&self) -> bool {
        match *self {
            ToSend::ToCoordinator(_, _) => true,
            _ => false,
        }
    }

    /// Check if it' ssomething to be sent to processes.
    pub fn to_processes(&self) -> bool {
        match *self {
            ToSend::ToProcesses(_, _, _) => true,
            _ => false,
        }
    }
}
