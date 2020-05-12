use crate::id::{Dot, ProcessId};
use crate::util;
use std::collections::HashMap;
use threshold::{AEClock, EventSet, VClock};

#[derive(Clone)]
pub struct GCTrack {
    process_id: ProcessId,
    n: usize,
    committed: AEClock<ProcessId>,
    all_but_me: HashMap<ProcessId, VClock<ProcessId>>,
    previous_stable: VClock<ProcessId>,
}

impl GCTrack {
    pub fn new(process_id: ProcessId, n: usize) -> Self {
        // committed clocks from all processes but self
        let all_but_me = HashMap::with_capacity(n - 1);

        Self {
            process_id,
            n,
            committed: Self::bottom_aeclock(n),
            all_but_me,
            previous_stable: Self::bottom_vclock(n),
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
        self.committed.frontier()
    }

    /// Records that set of `committed` commands by process `from`.
    pub fn committed_by(
        &mut self,
        from: ProcessId,
        committed: VClock<ProcessId>,
    ) {
        self.all_but_me.insert(from, committed);
    }

    /// Computes the new set of stable dots.
    pub fn stable(&mut self) -> Vec<Dot> {
        let new_stable = self.stable_clock();
        let dots = self
            .previous_stable
            .iter()
            .flat_map(|(actor, previous)| {
                let current = new_stable
                    .get(actor)
                    .expect("actor should exist in the newly stable clock");
                // create a dot for each newly stable event from this actor
                let start = previous.frontier() + 1;
                let end = current.frontier();
                (start..=end).map(move |stable_event| {
                    Dot::new(actor.clone(), stable_event)
                })
            })
            .collect();
        // update the previous stable clock
        self.previous_stable = new_stable;
        // and return newly stable dots
        dots
    }

    // TODO we should design a fault-tolerant version of this
    fn stable_clock(&mut self) -> VClock<ProcessId> {
        if self.all_but_me.len() != self.n + 1 {
            // if we don't have info from all processes, then there are no
            // stable dots.
            return Self::bottom_vclock(self.n);
        }

        // start from our own frontier
        let mut stable = self.committed.frontier();
        // and intersect with all the other clocks
        self.all_but_me.values().for_each(|clock| {
            stable.meet(clock);
        });
        stable
    }

    fn bottom_aeclock(n: usize) -> AEClock<ProcessId> {
        AEClock::with(util::process_ids(n))
    }

    fn bottom_vclock(n: usize) -> VClock<ProcessId> {
        VClock::with(util::process_ids(n))
    }
}
