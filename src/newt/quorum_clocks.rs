use crate::base::ProcId;
use std::cmp::Ordering;
use std::collections::HashSet;

#[derive(Debug, Clone)]
pub struct QuorumClocks {
    // fast quorum size
    q: usize,
    // set of processes that have participated in this computation
    participants: HashSet<ProcId>,
    // cache current max clock
    max_clock: u64,
    // number of times the maximum clock has been reported
    max_clock_count: usize,
}

impl QuorumClocks {
    /// Creates a `QuorumClocks` instance given the quorum size.
    pub fn new(q: usize) -> Self {
        QuorumClocks {
            q,
            participants: HashSet::new(),
            max_clock: 0,
            max_clock_count: 0,
        }
    }

    /// Check if we have a clock from a given `ProcId`.
    pub fn contains(&self, proc_id: &ProcId) -> bool {
        self.participants.contains(proc_id)
    }

    /// Sets the new clock reported by `ProcId` and returns the maximum clock
    /// seen until now.
    pub fn add(&mut self, proc_id: ProcId, clock: u64) -> (u64, usize) {
        assert!(self.participants.len() < self.q);

        // record new participant
        self.participants.insert(proc_id);

        // update max clock and max clock count
        match self.max_clock.cmp(&clock) {
            Ordering::Less => {
                // new max clock
                self.max_clock = clock;
                self.max_clock_count = 1;
            }
            Ordering::Equal => {
                // same max clock, simply update its count
                self.max_clock_count += 1;
            }
            Ordering::Greater => {
                // max clock did not change, do nothing
            }
        };

        // return max clock and number of occurrences
        (self.max_clock, self.max_clock_count)
    }

    /// Check if we all fast quorum processes have reported their clock.
    pub fn all(&self) -> bool {
        self.participants.len() == self.q
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contains() {
        // quorum clocks
        let q = 3;
        let mut quorum_clocks = QuorumClocks::new(q);

        // add clocks and check they're there
        quorum_clocks.add(0, 10);
        assert!(quorum_clocks.contains(&0));
        assert!(!quorum_clocks.contains(&1));
        assert!(!quorum_clocks.contains(&2));

        quorum_clocks.add(1, 10);
        assert!(quorum_clocks.contains(&0));
        assert!(quorum_clocks.contains(&1));
        assert!(!quorum_clocks.contains(&2));

        quorum_clocks.add(2, 10);
        assert!(quorum_clocks.contains(&0));
        assert!(quorum_clocks.contains(&1));
        assert!(quorum_clocks.contains(&2));
    }

    #[test]
    fn all() {
        // quorum clocks
        let q = 3;
        let mut quorum_clocks = QuorumClocks::new(q);

        // add clocks and check they're there
        quorum_clocks.add(0, 10);
        assert!(!quorum_clocks.all());
        quorum_clocks.add(1, 10);
        assert!(!quorum_clocks.all());
        quorum_clocks.add(2, 10);
        assert!(quorum_clocks.all());
    }

    #[test]
    fn max_and_count() {
        // -------------
        // quorum clocks
        let q = 3;
        let mut quorum_clocks = QuorumClocks::new(q);

        // add clocks and check they're there
        assert_eq!(quorum_clocks.add(0, 10), (10, 1));
        assert_eq!(quorum_clocks.add(1, 10), (10, 2));
        assert_eq!(quorum_clocks.add(2, 10), (10, 3));

        // -------------
        // quorum clocks
        let q = 10;
        let mut quorum_clocks = QuorumClocks::new(q);

        // add clocks and check they're there
        assert_eq!(quorum_clocks.add(0, 10), (10, 1));
        assert_eq!(quorum_clocks.add(1, 9), (10, 1));
        assert_eq!(quorum_clocks.add(2, 10), (10, 2));
        assert_eq!(quorum_clocks.add(3, 9), (10, 2));
        assert_eq!(quorum_clocks.add(4, 9), (10, 2));
        assert_eq!(quorum_clocks.add(5, 12), (12, 1));
        assert_eq!(quorum_clocks.add(6, 12), (12, 2));
        assert_eq!(quorum_clocks.add(7, 10), (12, 2));
        assert_eq!(quorum_clocks.add(8, 12), (12, 3));
        assert_eq!(quorum_clocks.add(9, 13), (13, 1));
    }
}
