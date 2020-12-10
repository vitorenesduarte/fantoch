use fantoch::id::ProcessId;
use fantoch::HashSet;
use std::cmp::Ordering;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuorumClocks {
    // fast quorum size
    fast_quorum_size: usize,
    // set of processes that have participated in this computation
    participants: HashSet<ProcessId>,
    // cache current max clock
    max_clock: u64,
    // number of times the maximum clock has been reported
    max_clock_count: usize,
}

impl QuorumClocks {
    /// Creates a `QuorumClocks` instance given the quorum size.
    pub fn new(fast_quorum_size: usize) -> Self {
        Self {
            fast_quorum_size,
            participants: HashSet::with_capacity(fast_quorum_size),
            max_clock: 0,
            max_clock_count: 0,
        }
    }

    /// Adds a new `clock` reported by `process_id` and returns the maximum
    /// clock seen until now.
    pub fn add(&mut self, process_id: ProcessId, clock: u64) -> (u64, usize) {
        assert!(self.participants.len() < self.fast_quorum_size);

        // record new participant
        self.participants.insert(process_id);

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
        self.participants.len() == self.fast_quorum_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all() {
        // quorum clocks
        let q = 3;
        let mut quorum_clocks = QuorumClocks::new(q);

        // add clocks and check they're there
        quorum_clocks.add(1, 10);
        assert!(!quorum_clocks.all());
        quorum_clocks.add(2, 10);
        assert!(!quorum_clocks.all());
        quorum_clocks.add(3, 10);
        assert!(quorum_clocks.all());
    }

    #[test]
    fn max_and_count() {
        // -------------
        // quorum clocks
        let q = 3;
        let mut quorum_clocks = QuorumClocks::new(q);

        // add clocks and check they're there
        assert_eq!(quorum_clocks.add(1, 10), (10, 1));
        assert_eq!(quorum_clocks.add(2, 10), (10, 2));
        assert_eq!(quorum_clocks.add(3, 10), (10, 3));

        // -------------
        // quorum clocks
        let q = 10;
        let mut quorum_clocks = QuorumClocks::new(q);

        // add clocks and check they're there
        assert_eq!(quorum_clocks.add(1, 10), (10, 1));
        assert_eq!(quorum_clocks.add(2, 9), (10, 1));
        assert_eq!(quorum_clocks.add(3, 10), (10, 2));
        assert_eq!(quorum_clocks.add(4, 9), (10, 2));
        assert_eq!(quorum_clocks.add(5, 9), (10, 2));
        assert_eq!(quorum_clocks.add(6, 12), (12, 1));
        assert_eq!(quorum_clocks.add(7, 12), (12, 2));
        assert_eq!(quorum_clocks.add(8, 10), (12, 2));
        assert_eq!(quorum_clocks.add(9, 12), (12, 3));
        assert_eq!(quorum_clocks.add(10, 13), (13, 1));
    }
}
