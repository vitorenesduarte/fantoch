// This module contains the definition of `SequentialKeyClocks`.
mod sequential;

// Re-exports.
pub use sequential::SequentialKeyClocks;

use fantoch::command::Command;
use fantoch::id::Dot;
use fantoch::HashSet;

pub trait KeyClocks: Clone {
    /// Create a new `KeyClocks` instance.
    fn new() -> Self;

    /// Computes this command's set of predecessors. From this moment on, this
    /// command will be reported as a predecessor of commands with a higher
    /// timestamp.
    fn predecessors(
        &mut self,
        dot: Dot,
        cmd: &Command,
        clock: u64,
    ) -> HashSet<Dot>;

    fn parallel() -> bool;
}
