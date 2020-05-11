use crate::id::{Dot, ProcessId};
use crate::protocol::gc::GCTrack;
use crate::util;
use std::collections::HashMap;
use threshold::{AEClock, VClock};

pub trait Info {
    fn new(
        process_id: ProcessId,
        n: usize,
        f: usize,
        fast_quorum_size: usize,
    ) -> Self;
}

// `CommandsInfo` contains `CommandInfo` for each `Dot`.
#[derive(Clone)]
pub struct CommandsInfo<I> {
    process_id: ProcessId,
    n: usize,
    f: usize,
    fast_quorum_size: usize,
    dot_to_info: HashMap<Dot, I>,
    gc_track: GCTrack,
}

impl<I> CommandsInfo<I>
where
    I: Info,
{
    pub fn new(
        process_id: ProcessId,
        n: usize,
        f: usize,
        fast_quorum_size: usize,
    ) -> Self {
        Self {
            process_id,
            n,
            f,
            fast_quorum_size,
            dot_to_info: HashMap::new(),
            gc_track: GCTrack::new(process_id, n),
        }
    }

    /// Returns the `Info` associated with `Dot`.
    /// If no `Info` is associated, an empty `Info` is returned.
    pub fn get(&mut self, dot: Dot) -> &mut I {
        // TODO borrow everything we need so that the borrow checker does not
        // complain
        let process_id = self.process_id;
        let n = self.n;
        let f = self.f;
        let fast_quorum_size = self.fast_quorum_size;
        self.dot_to_info
            .entry(dot)
            .or_insert_with(|| I::new(process_id, n, f, fast_quorum_size))
    }

    /// Records that a command has been committed.
    pub fn commit(&mut self, dot: Dot) {
        self.gc_track.commit(dot);
    }

    /// Records that set of `committed` commands by process `from`.
    pub fn committed_by(&mut self, from: ProcessId, committed: VClock<Dot>) {
        self.gc_track.committed_by(from, committed);
    }

    /// Performs garbage collection and returns a clock representing the set of
    /// commands committed locally.
    pub fn gc(&mut self) -> VClock<ProcessId> {
        for dot in self.gc_track.stable() {
            assert!(self.dot_to_info.remove(&dot).is_some())
        }
        self.gc_track.committed()
    }
}
