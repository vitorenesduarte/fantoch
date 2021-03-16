use super::Info;
use crate::id::{Dot, ProcessId, ShardId};
use crate::shared::{SharedMap, SharedMapRef};
use parking_lot::Mutex;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct LockedCommandsInfo<I: Info> {
    process_id: ProcessId,
    shard_id: ShardId,
    n: usize,
    f: usize,
    fast_quorum_size: usize,
    write_quorum_size: usize,
    dot_to_info: Arc<SharedMap<Dot, Mutex<I>>>,
}

impl<I> LockedCommandsInfo<I>
where
    I: Info,
{
    pub fn new(
        process_id: ProcessId,
        shard_id: ShardId,
        n: usize,
        f: usize,
        fast_quorum_size: usize,
        write_quorum_size: usize,
    ) -> Self {
        Self {
            process_id,
            shard_id,
            n,
            f,
            fast_quorum_size,
            write_quorum_size,
            dot_to_info: Arc::new(SharedMap::new()),
        }
    }

    /// Returns the `Info` associated with `Dot` in case it exists.
    pub fn get(&mut self, dot: Dot) -> Option<SharedMapRef<'_, Dot, Mutex<I>>> {
        self.dot_to_info.get(&dot)
    }

    /// Returns the `Info` associated with `Dot`.
    /// If no `Info` is associated, an empty `Info` is returned.
    pub fn get_or_default(
        &mut self,
        dot: Dot,
    ) -> SharedMapRef<'_, Dot, Mutex<I>> {
        // borrow everything we need so that the borrow checker does not
        // complain
        let process_id = self.process_id;
        let shard_id = self.shard_id;
        let n = self.n;
        let f = self.f;
        let fast_quorum_size = self.fast_quorum_size;
        let write_quorum_size = self.write_quorum_size;
        self.dot_to_info.get_or(&dot, || {
            let info = I::new(
                process_id,
                shard_id,
                n,
                f,
                fast_quorum_size,
                write_quorum_size,
            );
            Mutex::new(info)
        })
    }

    /// Removes a command.
    #[must_use]
    pub fn gc_single(&mut self, dot: &Dot) -> Option<I> {
        self.dot_to_info
            .remove(dot)
            .map(|(_dot, lock)| lock.into_inner())
    }

    pub fn len(&self) -> usize {
        self.dot_to_info.len()
    }
}
