use super::Dependency;
use fantoch::id::ProcessId;
use fantoch::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuorumDeps {
    // fast quorum size
    // NOTE: the fast quorum size may end up being smaller than this if NFR
    //       is enabled (see `BaseProcess::maybe_adjust_fast_quorum`)
    fast_quorum_size: usize,
    // set of processes that have participated in this computation
    participants: HashSet<ProcessId>,
    // mapping from dep to the number of times it is reported by the fast
    // quorum
    threshold_deps: HashMap<Dependency, usize>,
}

impl QuorumDeps {
    /// Creates a `QuorumDeps` instance given the quorum size.
    pub fn new(fast_quorum_size: usize) -> Self {
        Self {
            fast_quorum_size,
            participants: HashSet::with_capacity(fast_quorum_size),
            threshold_deps: HashMap::new(),
        }
    }

    /// Maybe change the fast quorum size.
    pub fn maybe_adjust_fast_quorum_size(&mut self, fast_quorum_size: usize) {
        debug_assert!(self.participants.is_empty());
        self.fast_quorum_size = fast_quorum_size;
    }

    /// Adds new `deps` reported by `process_id`.
    pub fn add(&mut self, process_id: ProcessId, deps: HashSet<Dependency>) {
        debug_assert!(self.participants.len() < self.fast_quorum_size);

        // record new participant
        self.participants.insert(process_id);

        // add each dep to the threshold deps
        for dep in deps {
            *self.threshold_deps.entry(dep).or_default() += 1;
        }
    }

    /// Check if we all fast quorum processes have reported their deps.
    pub fn all(&self) -> bool {
        self.participants.len() == self.fast_quorum_size
    }

    /// Checks if threshold union == union and returns the union.
    pub fn check_threshold(
        &self,
        threshold: usize,
    ) -> (HashSet<Dependency>, bool) {
        debug_assert!(self.all());
        let mut equal_to_union = true;

        let deps: HashSet<_> = self
            .threshold_deps
            .iter()
            .map(|(dep, count)| {
                // it's equal to union if all deps were reported at least
                // `threshold` times
                equal_to_union = equal_to_union && *count >= threshold;
                dep.clone()
            })
            .collect();
        (deps, equal_to_union)
    }

    /// Checks if all deps reported are the same and returns the union.
    pub fn check_equal(&self) -> (HashSet<Dependency>, bool) {
        debug_assert!(self.all());

        let (deps, counts): (HashSet<Dependency>, HashSet<usize>) =
            self.threshold_deps.clone().into_iter().unzip();
        // we have equal deps reported if there's a single count, i.e.
        // i.e. when no dependencies are reported)
        let equal_deps_reported = match counts.len() {
            0 => {
                // this means that no dependencies were reported, which should
                // not be possible
                true
            }
            1 => {
                // we have equal deps if:
                // - dependencies are reported the same number of times
                // - their report count is equal to the number of fast quorum
                //   processes
                counts
                    .into_iter()
                    .next()
                    .expect("there must be a dep count")
                    == self.fast_quorum_size
            }
            _ => {
                // if there's a different count at least two dependencies, then
                // at least one of the set of dependencies reported didn't match
                false
            }
        };
        (deps, equal_deps_reported)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fantoch::id::Dot;
    use std::iter::FromIterator;

    fn new_dep(source: ProcessId, sequence: u64) -> Dependency {
        let dot = Dot::new(source, sequence);
        // we don't care about shards in these tests, so we can just set them to
        // `None`
        Dependency::from_noop(dot)
    }

    #[test]
    fn all_test() {
        // quorum deps
        let q = 3;
        let mut quorum_deps = QuorumDeps::new(q);

        // add all deps and check they're there
        let deps = HashSet::from_iter(vec![new_dep(1, 1), new_dep(1, 2)]);
        quorum_deps.add(0, deps.clone());
        assert!(!quorum_deps.all());
        quorum_deps.add(1, deps.clone());
        assert!(!quorum_deps.all());
        quorum_deps.add(2, deps.clone());
        assert!(quorum_deps.all());
    }

    #[test]
    fn check_threshold_test() {
        // -------------
        // quorum deps
        let q = 3;
        let mut quorum_deps = QuorumDeps::new(q);

        // add deps
        let deps_1_and_2 =
            HashSet::from_iter(vec![new_dep(1, 1), new_dep(1, 2)]);
        quorum_deps.add(1, deps_1_and_2.clone());
        quorum_deps.add(2, deps_1_and_2.clone());
        quorum_deps.add(3, deps_1_and_2.clone());

        // check threshold union
        assert_eq!(
            quorum_deps.check_threshold(1),
            (deps_1_and_2.clone(), true)
        );
        assert_eq!(
            quorum_deps.check_threshold(2),
            (deps_1_and_2.clone(), true)
        );
        assert_eq!(
            quorum_deps.check_threshold(3),
            (deps_1_and_2.clone(), true)
        );
        assert_eq!(
            quorum_deps.check_threshold(4),
            (deps_1_and_2.clone(), false)
        );

        // -------------
        // quorum deps
        let q = 3;
        let mut quorum_deps = QuorumDeps::new(q);

        // add clocks
        let deps_1_2_and_3 = HashSet::from_iter(vec![
            new_dep(1, 1),
            new_dep(1, 2),
            new_dep(1, 3),
        ]);
        quorum_deps.add(1, deps_1_2_and_3.clone());
        quorum_deps.add(2, deps_1_and_2.clone());
        quorum_deps.add(3, deps_1_and_2.clone());

        // check threshold union
        assert_eq!(
            quorum_deps.check_threshold(1),
            (deps_1_2_and_3.clone(), true)
        );
        assert_eq!(
            quorum_deps.check_threshold(2),
            (deps_1_2_and_3.clone(), false)
        );
        assert_eq!(
            quorum_deps.check_threshold(3),
            (deps_1_2_and_3.clone(), false)
        );
        assert_eq!(
            quorum_deps.check_threshold(4),
            (deps_1_2_and_3.clone(), false)
        );

        // -------------
        // quorum deps
        let q = 3;
        let mut quorum_deps = QuorumDeps::new(q);

        // add clocks
        let deps_1 = HashSet::from_iter(vec![new_dep(1, 1)]);
        quorum_deps.add(1, deps_1_2_and_3.clone());
        quorum_deps.add(2, deps_1_and_2.clone());
        quorum_deps.add(3, deps_1.clone());

        // check threshold union
        assert_eq!(
            quorum_deps.check_threshold(1),
            (deps_1_2_and_3.clone(), true)
        );
        assert_eq!(
            quorum_deps.check_threshold(2),
            (deps_1_2_and_3.clone(), false)
        );
        assert_eq!(
            quorum_deps.check_threshold(3),
            (deps_1_2_and_3.clone(), false)
        );
        assert_eq!(
            quorum_deps.check_threshold(4),
            (deps_1_2_and_3.clone(), false)
        );
    }

    #[test]
    fn check_equal_test() {
        // add deps
        let deps_1 = HashSet::from_iter(vec![new_dep(1, 1)]);
        let deps_1_and_2 =
            HashSet::from_iter(vec![new_dep(1, 1), new_dep(1, 2)]);
        let deps_1_and_3 =
            HashSet::from_iter(vec![new_dep(1, 1), new_dep(1, 3)]);
        let deps_2_and_3 =
            HashSet::from_iter(vec![new_dep(1, 2), new_dep(1, 3)]);
        let deps_1_2_and_3 = HashSet::from_iter(vec![
            new_dep(1, 1),
            new_dep(1, 2),
            new_dep(1, 3),
        ]);

        // -------------
        // quorum deps
        let mut quorum_deps = QuorumDeps::new(2);
        quorum_deps.add(1, HashSet::new());
        quorum_deps.add(2, HashSet::new());
        assert_eq!(quorum_deps.check_equal(), (HashSet::new(), true));

        // -------------
        // quorum deps
        let mut quorum_deps = QuorumDeps::new(3);
        quorum_deps.add(1, HashSet::new());
        quorum_deps.add(2, HashSet::new());
        quorum_deps.add(3, deps_1.clone());
        assert_eq!(quorum_deps.check_equal(), (deps_1.clone(), false));

        // -------------
        // quorum deps
        let mut quorum_deps = QuorumDeps::new(3);
        quorum_deps.add(1, deps_1.clone());
        quorum_deps.add(2, deps_1.clone());
        quorum_deps.add(3, deps_1.clone());
        assert_eq!(quorum_deps.check_equal(), (deps_1.clone(), true));

        // -------------
        // quorum deps
        let mut quorum_deps = QuorumDeps::new(2);
        quorum_deps.add(1, deps_1_and_2.clone());
        quorum_deps.add(2, deps_1_and_2.clone());
        assert_eq!(quorum_deps.check_equal(), (deps_1_and_2.clone(), true));

        // -------------
        // quorum deps
        let mut quorum_deps = QuorumDeps::new(2);
        quorum_deps.add(1, deps_1_and_2.clone());
        quorum_deps.add(2, HashSet::new());
        assert_eq!(quorum_deps.check_equal(), (deps_1_and_2.clone(), false));

        // -------------
        // quorum deps
        let mut quorum_deps = QuorumDeps::new(3);
        quorum_deps.add(1, deps_1_and_2);
        quorum_deps.add(2, deps_1_and_3);
        quorum_deps.add(3, deps_2_and_3);
        assert_eq!(quorum_deps.check_equal(), (deps_1_2_and_3, false));
    }

    #[test]
    fn check_equal_regression_test() {
        let q = 3;

        // add deps
        let deps_1 = HashSet::from_iter(vec![new_dep(1, 1)]);
        let deps_2 = HashSet::from_iter(vec![new_dep(1, 2)]);
        let deps_1_and_2 =
            HashSet::from_iter(vec![new_dep(1, 1), new_dep(1, 2)]);

        let mut quorum_deps = QuorumDeps::new(q);
        quorum_deps.add(1, deps_1);
        quorum_deps.add(2, deps_2);
        quorum_deps.add(3, deps_1_and_2.clone());

        assert_eq!(quorum_deps.check_equal(), (deps_1_and_2, false));
    }
}
