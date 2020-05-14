use fantoch::id::ProcessId;
use std::collections::HashMap;
use threshold::{AboveExSet, EventSet};

#[derive(Clone)]
pub struct GCTrack {
    process_id: ProcessId,
    n: usize,
    committed: AboveExSet,
    all_but_me: HashMap<ProcessId, u64>,
    previous_stable: u64,
}

impl GCTrack {
    pub fn new(process_id: ProcessId, n: usize) -> Self {
        // committed clocks from all processes but self
        let all_but_me = HashMap::with_capacity(n - 1);

        Self {
            process_id,
            n,
            committed: AboveExSet::new(),
            all_but_me,
            previous_stable: 0,
        }
    }

    /// Records that a command has been committed.
    pub fn commit(&mut self, slot: u64) {
        self.committed.add_event(slot);
    }

    /// Returns a clock representing the set of commands committed locally.
    /// Note that there might be more commands committed than the ones being
    /// represented by the returned clock.
    pub fn committed(&self) -> u64 {
        self.committed.frontier()
    }

    /// Records that set of `committed` commands by process `from`.
    pub fn committed_by(&mut self, from: ProcessId, committed: u64) {
        self.all_but_me.insert(from, committed);
    }

    /// Computes the new set of stable slots.
    pub fn stable(&mut self) -> (u64, u64) {
        // compute new stable slot
        let new_stable = self.stable_slot();
        // compute stable slot range
        let slot_range = (self.previous_stable + 1, new_stable);
        // update the previous stable slot
        self.previous_stable = new_stable;
        // and return newly stable slots
        slot_range
    }

    // TODO we should design a fault-tolerant version of this
    fn stable_slot(&mut self) -> u64 {
        if self.all_but_me.len() != self.n - 1 {
            // if we don't have info from all processes, then there are no
            // stable dots.
            return 0;
        }

        // start from our own frontier
        let mut stable = self.committed.frontier();
        // and intersect with all the other clocks
        self.all_but_me.values().for_each(|&clock| {
            stable = std::cmp::min(stable, clock);
        });
        stable
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slots((start, end): (u64, u64)) -> Vec<u64> {
        (start..=end).collect()
    }

    #[test]
    fn gc_flow() {
        let n = 2;
        // create new gc track for the our process: 1
        let mut gc = GCTrack::new(1, n);

        // let's also create a gc track for process 2
        let mut gc2 = GCTrack::new(2, n);

        // there's nothing committed and nothing stable
        assert_eq!(gc.committed(), 0);
        assert_eq!(gc.stable_slot(), 0);
        assert_eq!(slots(gc.stable()), vec![]);

        // and commit slot 2 locally
        gc.commit(2);

        // this doesn't change anything
        assert_eq!(gc.committed(), 0);
        assert_eq!(gc.stable_slot(), 0);
        assert_eq!(slots(gc.stable()), vec![]);

        // however, if we also commit slot 1, the committed clock will change
        gc.commit(1);
        assert_eq!(gc.committed(), 2);
        assert_eq!(gc.stable_slot(), 0);
        assert_eq!(slots(gc.stable()), vec![]);

        // if we update with the committed clock from process 2 nothing changes
        gc.committed_by(2, gc2.committed());
        assert_eq!(gc.committed(), 2);
        assert_eq!(gc.stable_slot(), 0);
        assert_eq!(slots(gc.stable()), vec![]);

        // let's commit slot 1 and slot 3 at process 2
        gc2.commit(1);
        gc2.commit(3);

        // now dot11 is stable at process 1
        gc.committed_by(2, gc2.committed());
        assert_eq!(gc.committed(), 2);
        assert_eq!(gc.stable_slot(), 1);
        assert_eq!(slots(gc.stable()), vec![1]);

        // if we call stable again, no new dot is returned
        assert_eq!(gc.stable_slot(), 1);
        assert_eq!(slots(gc.stable()), vec![]);

        // let's commit slot 3 at process 1 and slot 2 at process 2
        gc.commit(3);
        gc2.commit(2);

        // now both dot12 and dot13 are stable at process 1
        gc.committed_by(2, gc2.committed());
        assert_eq!(gc.committed(), 3);
        assert_eq!(gc.stable_slot(), 3);
        assert_eq!(slots(gc.stable()), vec![2, 3]);
        assert_eq!(slots(gc.stable()), vec![]);
    }
}
