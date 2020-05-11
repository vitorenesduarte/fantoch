use crate::id::{Dot, ProcessId};
use crate::util;
use threshold::{AEClock, MaxSet, TClock, VClock};

#[derive(Clone)]
pub struct GCTrack {
    process_id: ProcessId,
    committed: AEClock<ProcessId>,
    matrix: TClock<ProcessId, MaxSet>,
    previous_gc: VClock<ProcessId>,
}

impl GCTrack {
    pub fn new(process_id: ProcessId, n: usize) -> Self {
        let committed = AEClock::with(util::process_ids(n));
        let matrix = TClock::new();
        let previous_gc = VClock::with(util::process_ids(n));

        Self {
            process_id,
            committed,
            matrix,
            previous_gc,
        }
    }

    /// Records that a command has been committed.
    pub fn commit(&mut self, dot: Dot) {
        self.committed.add(&dot.source(), dot.sequence());
    }

    /// Returns a clock representing the set of commands committed locally.
    /// Note that there might be more commands committed than the ones being
    /// represented by the returned clock.
    pub fn committed(&self) -> VClock<ProcessId> {
        let mut committed = VClock::new();
        self.committed
            .frontier()
            .into_iter()
            .for_each(|(actor, max)| {
                committed.add(actor, max);
            });
        committed
    }

    /// Records that set of `committed` commands by process `from`.
    pub fn committed_by(&mut self, from: ProcessId, committed: VClock<Dot>) {
        todo!()
    }

    /// Computes the new set of stable dots.
    pub fn stable(&mut self) -> Vec<Dot> {
        todo!()
    }
}
