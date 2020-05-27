use crate::config::Config;
use crate::id::{Dot, DotGen, ProcessId};
use crate::metrics::Metrics;
use crate::protocol::{ProtocolMetrics, ProtocolMetricsKind};
use crate::time::SysTime;
use std::collections::HashSet;
use std::iter::FromIterator;

// a `BaseProcess` has all functionalities shared by Atlas, Newt, ...
#[derive(Clone)]
pub struct BaseProcess {
    pub process_id: ProcessId,
    pub config: Config,
    all: Option<HashSet<ProcessId>>,
    all_but_me: Option<HashSet<ProcessId>>,
    fast_quorum: Option<HashSet<ProcessId>>,
    write_quorum: Option<HashSet<ProcessId>>,
    fast_quorum_size: usize,
    write_quorum_size: usize,
    dot_gen: DotGen,
    metrics: ProtocolMetrics,
}

impl BaseProcess {
    /// Creates a new `BaseProcess`.
    pub fn new(
        process_id: ProcessId,
        config: Config,
        fast_quorum_size: usize,
        write_quorum_size: usize,
    ) -> Self {
        // since processes lead with ballot `id` when taking the slow path and
        // we may rely on the fact that a zero accepted ballot means the process
        // has never been through Paxos phase-2, all ids must non-zero
        assert!(process_id != 0);

        Self {
            process_id,
            config,
            all: None,
            all_but_me: None,
            fast_quorum: None,
            write_quorum: None,
            fast_quorum_size,
            write_quorum_size,
            dot_gen: DotGen::new(process_id),
            metrics: Metrics::new(),
        }
    }

    /// Updates the processes known by this process.
    /// The set of processes provided is already sorted by distance.
    pub fn discover(&mut self, processes: Vec<ProcessId>) -> bool {
        // create fast quorum by taking the first `fast_quorum_size` elements
        let fast_quorum: HashSet<_> = processes
            .clone()
            .into_iter()
            .take(self.fast_quorum_size)
            .collect();

        // create write quorum by taking the first `write_quorum_size` elements
        let write_quorum: HashSet<_> = processes
            .clone()
            .into_iter()
            .take(self.write_quorum_size)
            .collect();

        // set all processes
        let all = HashSet::from_iter(processes.clone());
        let all_but_me = HashSet::from_iter(
            processes.into_iter().filter(|&p| p != self.process_id),
        );

        self.all = Some(all);
        self.all_but_me = Some(all_but_me);

        // set fast quorum if we have enough fast quorum processes
        self.fast_quorum = if fast_quorum.len() == self.fast_quorum_size {
            Some(fast_quorum)
        } else {
            None
        };

        // set write quorum if we have enough write quorum processes
        self.write_quorum = if write_quorum.len() == self.write_quorum_size {
            Some(write_quorum)
        } else {
            None
        };

        // connected if fast quorum and write quorum are set
        self.fast_quorum.is_some() && self.write_quorum.is_some()
    }

    // Returns the next dot.
    pub fn next_dot(&mut self) -> Dot {
        self.dot_gen.next_id()
    }

    // Returns all processes.
    pub fn all(&self) -> HashSet<ProcessId> {
        self.all
            .clone()
            .expect("the set of all processes should be known")
    }

    // Returns all processes but self.
    pub fn all_but_me(&self) -> HashSet<ProcessId> {
        self.all_but_me
            .clone()
            .expect("the set of all processes (except self) should be known")
    }

    // Returns the fast quorum.
    pub fn fast_quorum(&self) -> HashSet<ProcessId> {
        self.fast_quorum
            .clone()
            .expect("the fast quorum should be known")
    }

    // Returns the write quorum.
    pub fn write_quorum(&self) -> HashSet<ProcessId> {
        self.write_quorum
            .clone()
            .expect("the slow quorum should be known")
    }

    // Return metrics.
    pub fn metrics(&self) -> &ProtocolMetrics {
        &self.metrics
    }

    // Increment fast path count.
    // TODO  rename
    pub fn fast_path(&mut self) {
        self.inc_by_metric(ProtocolMetricsKind::FastPath, 1);
    }

    // Increment slow path count.
    pub fn slow_path(&mut self) {
        self.inc_by_metric(ProtocolMetricsKind::SlowPath, 1);
    }

    // Accumulate more stable commands.
    pub fn stable(&mut self, len: usize) {
        self.inc_by_metric(ProtocolMetricsKind::Stable, len as u64);
    }

    fn inc_by_metric(&mut self, kind: ProtocolMetricsKind, by: u64) {
        self.metrics.aggregate(kind, |v| *v += by)
    }

    // Compute the min clock use for clock bumping in ewnt:
    // - if we should use real time, then that min clock is the maximum between
    //   real time and the suggested min clock;
    // - otherwise, the suggested min clock is used
    pub fn min_clock(&self, current_min: u64, time: &dyn SysTime) -> u64 {
        // use real time as min clock if we should
        if self.config.newt_real_time() {
            std::cmp::max(current_min, time.now())
        } else {
            current_min
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planet::{Planet, Region};
    use crate::util;
    use std::collections::BTreeSet;
    use std::iter::FromIterator;

    #[test]
    fn discover() {
        // processes
        let processes = vec![
            (0, Region::new("asia-east1")),
            (1, Region::new("asia-northeast1")),
            (2, Region::new("asia-south1")),
            (3, Region::new("asia-southeast1")),
            (4, Region::new("australia-southeast1")),
            (5, Region::new("europe-north1")),
            (6, Region::new("europe-west1")),
            (7, Region::new("europe-west2")),
            (8, Region::new("europe-west3")),
            (9, Region::new("europe-west4")),
            (10, Region::new("northamerica-northeast1")),
            (11, Region::new("southamerica-east1")),
            (12, Region::new("us-central1")),
            (13, Region::new("us-east1")),
            (14, Region::new("us-east4")),
            (15, Region::new("us-west1")),
            (16, Region::new("us-west2")),
        ];

        // config
        let n = 17;
        let f = 3;
        let config = Config::new(n, f);

        // bp
        let id = 8;
        let region = Region::new("europe-west3");
        let planet = Planet::new();
        let fast_quorum_size = 6;
        let write_quorum_size = 4;
        let mut bp =
            BaseProcess::new(id, config, fast_quorum_size, write_quorum_size);

        // no quorum is set yet
        assert_eq!(bp.fast_quorum, None);
        assert_eq!(bp.all, None);

        // discover processes and check we're connected
        let sorted =
            util::sort_processes_by_distance(&region, &planet, processes);
        assert!(bp.discover(sorted));

        // check set of all processes
        assert_eq!(
            BTreeSet::from_iter(bp.all()),
            BTreeSet::from_iter(vec![
                8, 9, 6, 7, 5, 14, 10, 13, 12, 15, 16, 11, 1, 0, 4, 3, 2
            ]),
        );

        // check set of all processes (but self)
        assert_eq!(
            BTreeSet::from_iter(bp.all_but_me()),
            BTreeSet::from_iter(vec![
                9, 6, 7, 5, 14, 10, 13, 12, 15, 16, 11, 1, 0, 4, 3, 2
            ]),
        );

        // check fast quorum
        assert_eq!(
            BTreeSet::from_iter(bp.fast_quorum()),
            BTreeSet::from_iter(vec![8, 9, 6, 7, 5, 14])
        );

        // check write quorum
        assert_eq!(
            BTreeSet::from_iter(bp.write_quorum()),
            BTreeSet::from_iter(vec![8, 9, 6, 7])
        );
    }

    #[test]
    fn discover_same_region() {
        // processes
        let processes = vec![
            (0, Region::new("asia-east1")),
            (1, Region::new("asia-east1")),
            (2, Region::new("europe-north1")),
            (3, Region::new("europe-north1")),
            (4, Region::new("europe-west1")),
        ];

        // config
        let n = 5;
        let f = 2;
        let config = Config::new(n, f);

        // bp
        let id = 2;
        let region = Region::new("europe-north1");
        let planet = Planet::new();
        let fast_quorum_size = 3;
        let write_quorum_size = 4;
        let mut bp =
            BaseProcess::new(id, config, fast_quorum_size, write_quorum_size);

        // discover processes and check we're connected
        let sorted =
            util::sort_processes_by_distance(&region, &planet, processes);
        assert!(bp.discover(sorted));

        // check set of all processes
        assert_eq!(
            BTreeSet::from_iter(bp.all()),
            BTreeSet::from_iter(vec![2, 3, 4, 0, 1])
        );

        // check set of all processes (but self)
        assert_eq!(
            BTreeSet::from_iter(bp.all_but_me()),
            BTreeSet::from_iter(vec![3, 4, 0, 1])
        );

        // check fast quorum
        assert_eq!(
            BTreeSet::from_iter(bp.fast_quorum()),
            BTreeSet::from_iter(vec![2, 3, 4])
        );

        // check write quorum
        assert_eq!(
            BTreeSet::from_iter(bp.write_quorum()),
            BTreeSet::from_iter(vec![2, 3, 4, 0])
        );
    }
}
