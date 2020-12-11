use super::Clock;
use fantoch::id::{Dot, ProcessId};
use fantoch::HashSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuorumClocks {
    // fast quorum size
    fast_quorum_size: usize,
    // majority quorum size
    write_quorum_size: usize,
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
    pub fn new(
        process_id: ProcessId,
        fast_quorum_size: usize,
        write_quorum_size: usize,
    ) -> Self {
        Self {
            fast_quorum_size,
            write_quorum_size,
            participants: HashSet::with_capacity(fast_quorum_size),
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
            !self.ok && replied >= self.write_quorum_size;
        let fast_quorum = replied == self.fast_quorum_size;
        some_not_ok_after_majority || fast_quorum
    }

    /// Returns the current aggregated result.
    pub fn aggregated(&mut self) -> (Clock, HashSet<Dot>, bool) {
        // resets `this.deps` so that it can be returned without having to clone
        // it
        let deps = std::mem::take(&mut self.deps);
        (self.clock, deps, self.ok)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuorumRetries {
    // majority quorum size
    write_quorum_size: usize,
    // set of processes that have participated in this computation
    participants: HashSet<ProcessId>,
    // union of all predecessors
    deps: HashSet<Dot>,
}

impl QuorumRetries {
    /// Creates a `QuorumRetries` instance given the quorum size.
    pub fn new(write_quorum_size: usize) -> Self {
        Self {
            write_quorum_size,
            participants: HashSet::with_capacity(write_quorum_size),
            deps: HashSet::new(),
        }
    }

    /// Adds new `deps` reported by `process_id`.
    pub fn add(&mut self, process_id: ProcessId, deps: HashSet<Dot>) {
        assert!(self.participants.len() < self.write_quorum_size);

        // record new participant
        self.participants.insert(process_id);
        self.deps.extend(deps);
    }

    /// Check if we have all the replies we need.
    pub fn all(&self) -> bool {
        self.participants.len() == self.write_quorum_size
    }

    /// Returns the current aggregated result.
    pub fn aggregated(&mut self) -> HashSet<Dot> {
        // resets `this.deps` so that it can be returned without having to clone
        // it
        std::mem::take(&mut self.deps)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fantoch::id::Dot;
    use std::iter::FromIterator;

    #[test]
    fn quorum_clocks() {
        // setup
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

    #[test]
    fn quorum_retries() {
        // setup
        let mq = 2;

        // agreement
        let mut quorum_retries = QuorumRetries::new(mq);
        let deps_1 = HashSet::from_iter(vec![Dot::new(1, 1)]);
        let deps_2 = HashSet::from_iter(vec![Dot::new(1, 2)]);
        quorum_retries.add(1, deps_1);
        assert!(!quorum_retries.all());
        quorum_retries.add(2, deps_2);
        assert!(quorum_retries.all());
        // check aggregated
        let deps = quorum_retries.aggregated();
        assert_eq!(
            deps,
            HashSet::from_iter(vec![Dot::new(1, 1), Dot::new(1, 2)])
        );
    }
}
