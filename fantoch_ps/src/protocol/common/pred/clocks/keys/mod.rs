// This module contains the definition of `SequentialKeyClocks`.
mod sequential;

// Re-exports.
pub use sequential::SequentialKeyClocks;

use super::Timestamp;
use fantoch::command::Command;
use fantoch::id::{Dot, ShardId};
use fantoch::HashSet;

pub trait KeyClocks: Clone {
    /// Create a new `KeyClocks` instance.
    fn new(shard_id: ShardId) -> Self;

    /// Computes this command's set of predecessors. From this moment on, this
    /// command will be reported as a predecessor of commands with a higher
    /// timestamp. It also removes the prior instance of this command associated
    /// with its previous clock.
    fn predecessors(
        &mut self,
        dot: Dot,
        cmd: &Command,
        clock: Timestamp,
        previous_clock: Option<Timestamp>,
    ) -> HashSet<Dot>;

    fn remove(&mut self, cmd: &Command, clock: Timestamp);

    fn parallel() -> bool;
}
