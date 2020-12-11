// This module contains the implementation of `SequentialCommandsInfo`.
mod sequential;

// Re-exports.
pub use sequential::SequentialCommandsInfo;

use crate::id::{Dot, ProcessId, ShardId};

pub trait Info {
    fn new(
        process_id: ProcessId,
        shard_id: ShardId,
        n: usize,
        f: usize,
        fast_quorum_size: usize,
        write_quorum_size: usize,
    ) -> Self;
}

pub trait CommandsInfo<I>
where
    I: Info,
{
    fn new(
        process_id: ProcessId,
        shard_id: ShardId,
        n: usize,
        f: usize,
        fast_quorum_size: usize,
        write_quorum_size: usize,
    ) -> Self;

    /// Returns the `Info` associated with `Dot`.
    /// If no `Info` is associated, an empty `Info` is returned.
    fn get(&mut self, dot: Dot) -> &mut I;

    /// Performs garbage collection of stable dots.
    /// Returns how many stable does were removed.
    fn gc(&mut self, stable: Vec<(ProcessId, u64, u64)>) -> usize;

    /// Removes a command has been committed.
    fn gc_single(&mut self, dot: Dot);
}
