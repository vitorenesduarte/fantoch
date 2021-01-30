// This module contains the implementation of `SequentialCommandsInfo`.
mod sequential;

// This module contains the implementation of `LockedCommandsInfo`.
mod locked;

// Re-exports.
pub use locked::LockedCommandsInfo;
pub use sequential::SequentialCommandsInfo;

use crate::id::{ProcessId, ShardId};

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
