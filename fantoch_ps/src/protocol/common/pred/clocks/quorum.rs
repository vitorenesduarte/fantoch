use super::Clock;
use fantoch::id::{Dot, ProcessId};
use fantoch::HashSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuorumClocks {
    // fast quorum size
    fast_quorum_size: usize,
    // majority quorum size
    majority_quorum_size: usize,
    // set of processes that have participated in this computation
    participants: HashSet<ProcessId>,
    // max of all `clock`s
    clock: Clock,
    // union of all predecessors
    deps: HashSet<Dot>,
    // and of all `ok`s
    ok: bool,
}

impl QuorumClocks {
    /// Creates a `QuorumClocks` instance given the quorum size.
    pub fn new(fq: usize, mq: usize, process_id: ProcessId) -> Self {
        Self {
            fast_quorum_size: fq,
            majority_quorum_size: mq,
            participants: HashSet::with_capacity(fq),
            clock: Clock::new(process_id),
            deps: HashSet::new(),
            ok: true,
        }
    }

    /// Adds new `deps` reported by `process_id`.
    pub fn add(
        &mut self,
        process_id: ProcessId,
        clock: Clock,
        deps: HashSet<Dot>,
        ok: bool,
    ) {
        assert!(self.participants.len() < self.fast_quorum_size);

        // record new participant
        self.participants.insert(process_id);

        // update clock and deps
        self.clock.join(&clock);
        self.deps.extend(deps);
        self.ok = self.ok && ok;
    }

    /// Check if we have all the replies we need.
    pub fn all(&self) -> bool {
        let replied = self.participants.len();
        // we have all the replies we need if either one of the following:
        // - (at least) a majority has replied, and one of those processes
        //   reported !ok
        // - the whole fast quorum replied (independently of their replies)
        let some_not_ok_after_majority =
            !self.ok && replied >= self.majority_quorum_size;
        let fast_quorum = replied == self.fast_quorum_size;
        some_not_ok_after_majority || fast_quorum
    }

    /// Returns the current aggregated result.
    pub fn aggregated(&self) -> (Clock, HashSet<Dot>, bool) {
        (self.clock, self.deps.clone(), self.ok)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fantoch::id::Dot;
    use std::iter::FromIterator;

    #[test]
    fn aggregated() {
        // quorum deps
        let fq = 3;
        let mq = 2;
        let process_id = 1;

        // agreement
        let mut quorum_clocks = QuorumClocks::new(fq, mq, process_id);
        let clock_1 = Clock::from(10, 1);
        let deps_1 = HashSet::from_iter(vec![Dot::new(1, 1)]);
        let ok_1 = true;
        let clock_2 = Clock::from(10, 2);
        let deps_2 = HashSet::from_iter(vec![Dot::new(1, 2)]);
        let ok_2 = true;
        let clock_3 = Clock::from(10, 3);
        let deps_3 = HashSet::from_iter(vec![Dot::new(1, 1)]);
        let ok_3 = true;
        quorum_clocks.add(1, clock_1, deps_1, ok_1);
        assert!(!quorum_clocks.all());
        quorum_clocks.add(2, clock_2, deps_2, ok_2);
        assert!(!quorum_clocks.all());
        quorum_clocks.add(3, clock_3, deps_3, ok_3);
        assert!(quorum_clocks.all());
        // check aggregated
        let (clock, deps, ok) = quorum_clocks.aggregated();
        assert_eq!(clock, Clock::from(10, 3));
        assert_eq!(
            deps,
            HashSet::from_iter(vec![Dot::new(1, 1), Dot::new(1, 2)])
        );
        assert!(ok);

        // disagreement
        let clock_1 = Clock::from(10, 1);
        let deps_1 = HashSet::from_iter(vec![Dot::new(1, 1)]);
        let ok_1 = true;
        let clock_2 = Clock::from(12, 2);
        let deps_2 = HashSet::from_iter(vec![Dot::new(1, 2), Dot::new(1, 3)]);
        let ok_2 = false;
        let clock_3 = Clock::from(10, 3);
        let deps_3 = HashSet::from_iter(vec![Dot::new(1, 4)]);
        let ok_3 = true;
        // order: 1, 2
        let mut quorum_clocks = QuorumClocks::new(fq, mq, process_id);
        quorum_clocks.add(1, clock_1, deps_1.clone(), ok_1);
        assert!(!quorum_clocks.all());
        quorum_clocks.add(2, clock_2, deps_2.clone(), ok_2);
        assert!(quorum_clocks.all());
        // check aggregated
        let (clock, deps, ok) = quorum_clocks.aggregated();
        assert_eq!(clock, Clock::from(12, 2));
        assert_eq!(
            deps,
            HashSet::from_iter(vec![
                Dot::new(1, 1),
                Dot::new(1, 2),
                Dot::new(1, 3)
            ])
        );
        assert!(!ok);

        // order: 1, 3, 2
        let mut quorum_clocks = QuorumClocks::new(fq, mq, process_id);
        quorum_clocks.add(1, clock_1, deps_1.clone(), ok_1);
        assert!(!quorum_clocks.all());
        quorum_clocks.add(3, clock_3, deps_3.clone(), ok_3);
        assert!(!quorum_clocks.all());
        quorum_clocks.add(2, clock_2, deps_2.clone(), ok_2);
        assert!(quorum_clocks.all());
        // check aggregated
        let (clock, deps, ok) = quorum_clocks.aggregated();
        assert_eq!(clock, Clock::from(12, 2));
        assert_eq!(
            deps,
            HashSet::from_iter(vec![
                Dot::new(1, 1),
                Dot::new(1, 2),
                Dot::new(1, 3),
                Dot::new(1, 4)
            ])
        );
        assert!(!ok);

        // order: 2, 3
        let mut quorum_clocks = QuorumClocks::new(fq, mq, process_id);
        quorum_clocks.add(2, clock_2, deps_2.clone(), ok_2);
        assert!(!quorum_clocks.all());
        quorum_clocks.add(3, clock_3, deps_3.clone(), ok_3);
        assert!(quorum_clocks.all());
        // check aggregated
        let (clock, deps, ok) = quorum_clocks.aggregated();
        assert_eq!(clock, Clock::from(12, 2));
        assert_eq!(
            deps,
            HashSet::from_iter(vec![
                Dot::new(1, 2),
                Dot::new(1, 3),
                Dot::new(1, 4)
            ])
        );
        assert!(!ok);
    }
}
