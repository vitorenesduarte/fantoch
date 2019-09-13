use crate::base::ProcId;
use std::cmp::Ordering;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct QuorumClocks {
    // fast quorum size
    q: usize,
    clocks: HashMap<ProcId, u64>,
}

impl QuorumClocks {
    /// Creates an uninitiliazed `QuorumClocks` instance.
    pub fn uninit() -> Self {
        Self::from(0)
    }

    /// Creates an initiliazed `QuorumClocks` instance.
    pub fn from(q: usize) -> Self {
        QuorumClocks {
            q,
            clocks: HashMap::new(),
        }
    }

    /// Compute the clock of this command.
    pub fn add(&mut self, proc_id: ProcId, clock: u64) {
        assert!(self.clocks.len() < self.q);
        self.clocks.insert(proc_id, clock);
    }

    /// Check if we have a clock from a given `ProcId`.
    pub fn contains(&self, proc_id: &ProcId) -> bool {
        self.clocks.contains_key(proc_id)
    }

    /// Check if we have votes from all fast quorum processes.
    pub fn all(&self) -> bool {
        self.clocks.len() == self.q
    }

    /// Compute the maximum clock and the number of times it was reported by the
    /// quorum.
    pub fn max_and_count(&self) -> (u64, usize) {
        let mut max_count = 0;
        let max = self.clocks.iter().fold(0, |max, (_, proc_clock)| {
            match max.cmp(proc_clock) {
                Ordering::Less => {
                    // we have a new max
                    max_count = 1;
                    *proc_clock
                }
                Ordering::Equal => {
                    // same max, increment its count
                    max_count += 1;
                    max
                }
                Ordering::Greater => {
                    // nothing to see here
                    max
                }
            }
        });

        // return max and its count
        (max, max_count)
    }
}

#[cfg(test)]
mod tests {
    use crate::newt::quorum_clocks::QuorumClocks;

    #[test]
    fn contains() {
        // quorum clocks
        let q = 3;
        let mut quorum_clocks = QuorumClocks::from(q);

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
        let mut quorum_clocks = QuorumClocks::from(q);

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
        let mut quorum_clocks = QuorumClocks::from(q);

        // add clocks and check they're there
        quorum_clocks.add(0, 10);
        assert_eq!(quorum_clocks.max_and_count(), (10, 1));
        quorum_clocks.add(1, 10);
        assert_eq!(quorum_clocks.max_and_count(), (10, 2));
        quorum_clocks.add(2, 10);
        assert_eq!(quorum_clocks.max_and_count(), (10, 3));

        // -------------
        // quorum clocks
        let q = 10;
        let mut quorum_clocks = QuorumClocks::from(q);

        // add clocks and check they're there
        quorum_clocks.add(0, 10);
        assert_eq!(quorum_clocks.max_and_count(), (10, 1));
        quorum_clocks.add(1, 9);
        assert_eq!(quorum_clocks.max_and_count(), (10, 1));
        quorum_clocks.add(2, 10);
        assert_eq!(quorum_clocks.max_and_count(), (10, 2));
        quorum_clocks.add(3, 9);
        assert_eq!(quorum_clocks.max_and_count(), (10, 2));
        quorum_clocks.add(4, 9);
        assert_eq!(quorum_clocks.max_and_count(), (10, 2));
        quorum_clocks.add(5, 12);
        assert_eq!(quorum_clocks.max_and_count(), (12, 1));
        quorum_clocks.add(6, 12);
        assert_eq!(quorum_clocks.max_and_count(), (12, 2));
        quorum_clocks.add(7, 10);
        assert_eq!(quorum_clocks.max_and_count(), (12, 2));
        quorum_clocks.add(8, 12);
        assert_eq!(quorum_clocks.max_and_count(), (12, 3));
        quorum_clocks.add(9, 13);
        assert_eq!(quorum_clocks.max_and_count(), (13, 1));
    }
}
