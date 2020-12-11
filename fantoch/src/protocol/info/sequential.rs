
use crate::util;
use crate::HashMap;
use crate::id::{Dot, ProcessId, ShardId};
use super::{Info, CommandsInfo};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequentialCommandsInfo<I> {
    process_id: ProcessId,
    shard_id: ShardId,
    n: usize,
    f: usize,
    fast_quorum_size: usize,
    write_quorum_size: usize,
    dot_to_info: HashMap<Dot, I>,
}

impl<I> CommandsInfo<I> for SequentialCommandsInfo<I>
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
    ) -> Self {
        Self {
            process_id,
            shard_id,
            n,
            f,
            fast_quorum_size,
            write_quorum_size,
            dot_to_info: HashMap::new(),
        }
    }

    /// Returns the `Info` associated with `Dot`.
    /// If no `Info` is associated, an empty `Info` is returned.
    fn get(&mut self, dot: Dot) -> &mut I {
        // borrow everything we need so that the borrow checker does not
        // complain
        let process_id = self.process_id;
        let shard_id = self.shard_id;
        let n = self.n;
        let f = self.f;
        let fast_quorum_size = self.fast_quorum_size;
        let write_quorum_size = self.write_quorum_size;
        self.dot_to_info.entry(dot).or_insert_with(|| {
            I::new(
                process_id,
                shard_id,
                n,
                f,
                fast_quorum_size,
                write_quorum_size,
            )
        })
    }

    /// Performs garbage collection of stable dots.
    /// Returns how many stable does were removed.
    fn gc(&mut self, stable: Vec<(ProcessId, u64, u64)>) -> usize {
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
    fn gc_single(&mut self, dot: Dot) {
        assert!(self.dot_to_info.remove(&dot).is_some());
    }
}