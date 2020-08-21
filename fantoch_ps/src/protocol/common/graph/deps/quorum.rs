use super::Dependency;
use fantoch::id::ProcessId;
use fantoch::{HashMap, HashSet};

type ThresholdDeps = HashMap<Dependency, usize>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuorumDeps {
    // fast quorum size
    q: usize,
    // set of processes that have participated in this computation
    participants: HashSet<ProcessId>,
    // threshold deps
    threshold_deps: ThresholdDeps,
}

impl QuorumDeps {
    /// Creates a `QuorumDeps` instance given the quorum size.
    pub fn new(q: usize) -> Self {
        Self {
            q,
            participants: HashSet::with_capacity(q),
            threshold_deps: ThresholdDeps::new(),
        }
    }

    /// Adds new `deps` reported by `process_id`.
    pub fn add(&mut self, process_id: ProcessId, deps: HashSet<Dependency>) {
        assert!(self.participants.len() < self.q);

        // record new participant
        self.participants.insert(process_id);

        // add each dep to the threshold deps
        for dep in deps {
            *self.threshold_deps.entry(dep).or_default() += 1;
        }
    }

    /// Check if we all fast quorum processes have reported their deps.
    pub fn all(&self) -> bool {
        self.participants.len() == self.q
    }

    /// Computes the threshold union.
    pub fn threshold_union(
        &self,
        threshold: usize,
    ) -> (HashSet<Dependency>, bool) {
        let deps: HashSet<_> = self
            .threshold_deps
            .iter()
            .filter_map(|(dep, count)| {
                if *count >= threshold {
                    Some(dep.clone())
                } else {
                    None
                }
            })
            .collect();
        // it's equal to union if we have all deps ever reported
        let equal_to_union = deps.len() == self.threshold_deps.len();
        (deps, equal_to_union)
    }

    /// Computes the union.
    pub fn union(&self) -> (HashSet<Dependency>, bool) {
        let (deps, counts): (HashSet<Dependency>, HashSet<usize>) =
            self.threshold_deps.clone().into_iter().unzip();
        // we have equal deps reported if there's a single count (or no count,
        // i.e. when no dependencies are reported)
        let equal_deps_reported = counts.len() <= 1;
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
    fn all() {
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
    fn threshold_union() {
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
            quorum_deps.threshold_union(1),
            (deps_1_and_2.clone(), true)
        );
        assert_eq!(
            quorum_deps.threshold_union(2),
            (deps_1_and_2.clone(), true)
        );
        assert_eq!(
            quorum_deps.threshold_union(3),
            (deps_1_and_2.clone(), true)
        );
        assert_eq!(quorum_deps.threshold_union(4), (HashSet::new(), false));

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
            quorum_deps.threshold_union(1),
            (deps_1_2_and_3.clone(), true)
        );
        assert_eq!(
            quorum_deps.threshold_union(2),
            (deps_1_and_2.clone(), false)
        );
        assert_eq!(
            quorum_deps.threshold_union(3),
            (deps_1_and_2.clone(), false)
        );
        assert_eq!(quorum_deps.threshold_union(4), (HashSet::new(), false));

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
            quorum_deps.threshold_union(1),
            (deps_1_2_and_3.clone(), true)
        );
        assert_eq!(
            quorum_deps.threshold_union(2),
            (deps_1_and_2.clone(), false)
        );
        assert_eq!(quorum_deps.threshold_union(3), (deps_1.clone(), false));
        assert_eq!(quorum_deps.threshold_union(4), (HashSet::new(), false));
    }

    #[test]
    fn union() {
        let q = 4;

        // add deps
        let deps_1 = HashSet::from_iter(vec![new_dep(1, 1)]);
        let deps_1_and_2 =
            HashSet::from_iter(vec![new_dep(1, 1), new_dep(1, 2)]);
        let deps_1_2_and_3 = HashSet::from_iter(vec![
            new_dep(1, 1),
            new_dep(1, 2),
            new_dep(1, 3),
        ]);

        // -------------
        // quorum deps
        let mut quorum_deps = QuorumDeps::new(q);
        assert_eq!(quorum_deps.union(), (HashSet::new(), true));
        quorum_deps.add(1, deps_1_and_2.clone());
        assert_eq!(quorum_deps.union(), (deps_1_and_2.clone(), true));
        quorum_deps.add(2, deps_1_and_2.clone());
        assert_eq!(quorum_deps.union(), (deps_1_and_2.clone(), true));
        quorum_deps.add(3, deps_1_and_2.clone());
        assert_eq!(quorum_deps.union(), (deps_1_and_2.clone(), true));
        quorum_deps.add(4, deps_1.clone());
        assert_eq!(quorum_deps.union(), (deps_1_and_2.clone(), false));

        // -------------
        // quorum deps
        let mut quorum_deps = QuorumDeps::new(q);
        assert_eq!(quorum_deps.union(), (HashSet::new(), true));
        quorum_deps.add(1, deps_1_and_2.clone());
        assert_eq!(quorum_deps.union(), (deps_1_and_2.clone(), true));
        quorum_deps.add(2, deps_1_and_2.clone());
        assert_eq!(quorum_deps.union(), (deps_1_and_2.clone(), true));
        quorum_deps.add(3, deps_1_and_2.clone());
        assert_eq!(quorum_deps.union(), (deps_1_and_2.clone(), true));
        quorum_deps.add(4, deps_1_2_and_3.clone());
        assert_eq!(quorum_deps.union(), (deps_1_2_and_3.clone(), false));
    }
}
