use super::Info;
use crate::id::{Dot, ProcessId, ShardId};
use crate::shared::{SharedMap, SharedMapRef};
use crate::util;
use parking_lot::RwLock;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct LockedCommandsInfo<I: Info> {
    process_id: ProcessId,
    shard_id: ShardId,
    n: usize,
    f: usize,
    fast_quorum_size: usize,
    write_quorum_size: usize,
    dot_to_info: Arc<SharedMap<Dot, RwLock<I>>>,
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

    /// Returns the `Info` associated with `Dot`.
    /// If no `Info` is associated, an empty `Info` is returned.
    pub fn get(&mut self, dot: Dot) -> SharedMapRef<'_, Dot, RwLock<I>> {
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
            RwLock::new(info)
        })
    }

    /// Performs garbage collection of stable dots.
    /// Returns how many stable does were removed.
    pub fn gc(&mut self, stable: Vec<(ProcessId, u64, u64)>) -> usize {
        util::dots(stable)
            .filter(|dot| {
                // remove dot:
                // - the dot may not exist locally if there are multiple workers
                //   and this worker is not responsible for such dot
                self.dot_to_info.remove(&dot).is_some()
            })
            .count()
    }

    /// Removes a command has been committed.
    pub fn gc_single(&mut self, dot: Dot) {
        assert!(self.dot_to_info.remove(&dot).is_some());
    }
}
