use fantoche::id::ProcessId;
use std::collections::HashSet;
use threshold::{MaxSet, TClock, VClock};

type ThresholdClock = TClock<ProcessId, MaxSet>;

#[derive(Debug, Clone)]
pub struct QuorumClocks {
    // fast quorum size
    q: usize,
    // set of processes that have participated in this computation
    participants: HashSet<ProcessId>,
    // threshold clock
    threshold_clock: ThresholdClock,
}

impl QuorumClocks {
    /// Creates a `QuorumClocks` instance given the quorum size.
    pub fn new(q: usize) -> Self {
        Self {
            q,
            participants: HashSet::with_capacity(q),
            threshold_clock: ThresholdClock::with_capacitiy(q),
        }
    }

    /// Adds a new `clock` reported by `process_id`.
    pub fn add(&mut self, process_id: ProcessId, clock: VClock<ProcessId>) {
        assert!(self.participants.len() < self.q);

        // record new participant
        self.participants.insert(process_id);

        // add clock to the threshold clock
        self.threshold_clock.add(clock);
    }

    /// Check if we all fast quorum processes have reported their clock.
    pub fn all(&self) -> bool {
        self.participants.len() == self.q
    }

    /// Computes the threshold union.
    pub fn threshold_union(
        &self,
        threshold: usize,
    ) -> (VClock<ProcessId>, bool) {
        self.threshold_clock.threshold_union(threshold as u64)
    }

    /// Computes the union.
    pub fn union(&self) -> (VClock<ProcessId>, bool) {
        self.threshold_clock.union()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util;

    #[test]
    fn all() {
        // quorum clocks
        let q = 3;
        let mut quorum_clocks = QuorumClocks::new(q);

        // add clocks and check they're there
        quorum_clocks.add(0, util::vclock(vec![1, 2, 3, 4, 5]));
        assert!(!quorum_clocks.all());
        quorum_clocks.add(1, util::vclock(vec![1, 2, 3, 4, 5]));
        assert!(!quorum_clocks.all());
        quorum_clocks.add(2, util::vclock(vec![1, 2, 3, 4, 5]));
        assert!(quorum_clocks.all());
    }

    #[test]
    fn threshold_union() {
        // -------------
        // quorum clocks
        let q = 3;
        let mut quorum_clocks = QuorumClocks::new(q);

        // add clocks
        quorum_clocks.add(1, util::vclock(vec![1, 2, 3, 4, 5]));
        quorum_clocks.add(2, util::vclock(vec![1, 2, 3, 4, 5]));
        quorum_clocks.add(3, util::vclock(vec![1, 2, 3, 4, 5]));

        // check threshold union
        assert_eq!(
            quorum_clocks.threshold_union(1),
            (util::vclock(vec![1, 2, 3, 4, 5]), true),
        );
        assert_eq!(
            quorum_clocks.threshold_union(2),
            (util::vclock(vec![1, 2, 3, 4, 5]), true),
        );
        assert_eq!(
            quorum_clocks.threshold_union(3),
            (util::vclock(vec![1, 2, 3, 4, 5]), true),
        );

        // -------------
        // quorum clocks
        let q = 3;
        let mut quorum_clocks = QuorumClocks::new(q);

        // add clocks
        quorum_clocks.add(1, util::vclock(vec![1, 2, 2, 2, 5]));
        quorum_clocks.add(2, util::vclock(vec![1, 2, 3, 4, 5]));
        quorum_clocks.add(3, util::vclock(vec![1, 2, 3, 4, 6]));

        // check threshold union
        assert_eq!(
            quorum_clocks.threshold_union(1),
            (util::vclock(vec![1, 2, 3, 4, 6]), true),
        );
        assert_eq!(
            quorum_clocks.threshold_union(2),
            (util::vclock(vec![1, 2, 3, 4, 5]), false),
        );
        assert_eq!(
            quorum_clocks.threshold_union(3),
            (util::vclock(vec![1, 2, 2, 2, 5]), false),
        );
    }
}
