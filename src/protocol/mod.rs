// This module contains the definition of `ToSend`.
mod tosend;

// This module contains the definition of `BaseProcess`.
mod base;

// This module contains the definition of `Newt`.
mod newt;

// Re-exports.
pub use base::BaseProcess;
pub use newt::Newt;
pub use tosend::ToSend;

use crate::command::Command;
use crate::config::Config;
use crate::id::ProcessId;
use crate::planet::{Planet, Region};

pub trait Process {
    type Message: Clone;

    fn new(process_id: ProcessId, region: Region, planet: Planet, config: Config) -> Self;

    fn id(&self) -> ProcessId;

    fn discover(&mut self, processes: Vec<(ProcessId, Region)>) -> bool;

    fn submit(&mut self, cmd: Command) -> ToSend<Self::Message>;

    fn handle(&mut self, msg: Self::Message) -> ToSend<Self::Message>;
}
