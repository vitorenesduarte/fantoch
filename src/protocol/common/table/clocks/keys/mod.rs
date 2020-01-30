pub mod sequential;

use crate::command::Command;
use crate::id::ProcessId;
use crate::protocol::common::table::ProcessVotes;

pub trait KeyClocks: Clone {
    /// Create a new `SequentialKC` instance.
    fn new(id: ProcessId) -> Self;

    /// Retrieves the current clock for some command.
    /// If the command touches multiple keys, returns the maximum between the
    /// clocks associated with each key.
    fn clock(&self, cmd: &Command) -> u64;

    /// Vote up-to `clock`.
    fn process_votes(&mut self, cmd: &Command, clock: u64) -> ProcessVotes;
}
