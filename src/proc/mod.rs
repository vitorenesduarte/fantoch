// This module contains the definition of `ToSend`.
mod tosend;

// This module contains the definition of `BaseProc`.
mod base;

// Re-exports.
pub use base::BaseProc;
pub use tosend::ToSend;

use crate::command::Command;
use crate::config::Config;
use crate::id::ProcId;
use crate::planet::{Planet, Region};

pub trait Proc {
    type Message: Clone;

    fn new(id: ProcId, region: Region, planet: Planet, config: Config) -> Self;

    fn id(&self) -> ProcId;

    fn discover(&mut self, procs: Vec<(ProcId, Region)>) -> bool;

    fn submit(&mut self, cmd: Command) -> ToSend<Self::Message>;

    fn handle(&mut self, msg: Self::Message) -> ToSend<Self::Message>;
}
