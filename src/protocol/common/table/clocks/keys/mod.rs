pub mod sequential;

use crate::command::Command;
use crate::id::ProcessId;
use crate::protocol::common::table::ProcessVotes;

pub trait KeyClocks: Clone {
    /// Create a new `KeyClocks` instance.
    fn new(id: ProcessId) -> Self;

    /// Bump clocks to at least `min_clock` and return the new clock (that might
    /// be `min_clock` in case it was higher than any of the local clocks). Also
    /// returns the consumed votes.
    fn bump_and_vote(
        &mut self,
        cmd: &Command,
        min_clock: u64,
    ) -> (u64, ProcessVotes);

    /// Votes up to `clock` and returns the consumed votes.
    fn vote(&mut self, cmd: &Command, clock: u64) -> ProcessVotes;
}
