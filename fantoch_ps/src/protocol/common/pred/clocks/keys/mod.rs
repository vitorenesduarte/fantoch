// This module contains the definition of `LockedKeyClocks`.
mod locked;

// Re-exports.
pub use locked::LockedKeyClocks;

use super::Clock;
use crate::protocol::common::pred::CompressedDots;
use fantoch::command::Command;
use fantoch::id::{Dot, ProcessId, ShardId};
use fantoch::HashSet;
use std::fmt::Debug;

pub trait KeyClocks: Debug + Clone {
    /// Create a new `KeyClocks` instance.
    fn new(process_id: ProcessId, shard_id: ShardId) -> Self;

    // Generate the next clock.
    fn clock_next(&mut self) -> Clock;

    // Joins with remote clock.
    fn clock_join(&mut self, other: &Clock);

    // Adds a new command with some tentative timestamp.
    // After this, it starts being reported as a predecessor of other commands
    // with tentative higher timestamps.
    fn add(&mut self, dot: Dot, cmd: &Command, clock: Clock);

    // Removes a previously added command with some tentative timestamp.
    // After this, it stops being reported as a predecessor of other commands.
    fn remove(&mut self, cmd: &Command, clock: Clock);

    /// Computes all conflicting commands with a timestamp lower than `clock`.
    /// If `higher` is set, it fills it with all the conflicting commands with a
    /// timestamp higher than `clock`.
    fn predecessors(
        &self,
        dot: Dot,
        cmd: &Command,
        clock: Clock,
        higher: Option<&mut HashSet<Dot>>,
    ) -> CompressedDots;

    fn parallel() -> bool;
}
