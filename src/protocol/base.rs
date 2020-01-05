use crate::config::Config;
use crate::id::{Dot, DotGen, ProcessId};
use crate::metrics::Metrics;
use crate::planet::{Planet, Region};
use crate::util;
use std::fmt;

// a `BaseProcess` has all functionalities shared by Atlas, Newt, ...
pub struct BaseProcess {
    pub process_id: ProcessId,
    pub region: Region,
    pub planet: Planet,
    pub config: Config,
    all_processes: Option<Vec<ProcessId>>,
    fast_quorum: Option<Vec<ProcessId>>,
    write_quorum: Option<Vec<ProcessId>>,
    fast_quorum_size: usize,
    write_quorum_size: usize,
    dot_gen: DotGen,
    metrics: Metrics<MetricsKind, u64>,
}

impl BaseProcess {
    /// Creates a new `BaseProcess`.
    pub fn new(
        process_id: ProcessId,
        region: Region,
        planet: Planet,
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
            region,
            planet,
            config,
            all_processes: None,
            fast_quorum: None,
            write_quorum: None,
            fast_quorum_size,
            write_quorum_size,
            dot_gen: DotGen::new(process_id),
            metrics: Metrics::new(),
        }
    }

    /// Updates the processes known by this process.
    pub fn discover(&mut self, mut processes: Vec<(ProcessId, Region)>) -> bool {
        // create all processes
        util::sort_processes_by_distance(&self.region, &self.planet, &mut processes);
        let all_processes: Vec<_> = processes.into_iter().map(|(id, _)| id).collect();

        // create fast quorum by taking the first `fast_quorum_size` elements
        let fast_quorum: Vec<_> = all_processes
            .clone()
            .into_iter()
            .take(self.fast_quorum_size)
            .collect();

        // create write quorum by taking the first `write_quorum_size` elements
        let write_quorum: Vec<_> = all_processes
            .clone()
            .into_iter()
            .take(self.write_quorum_size)
            .collect();

        // set all processes
        self.all_processes = Some(all_processes);

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
    pub fn all(&self) -> Vec<ProcessId> {
        self.all_processes
            .clone()
            .expect("the set of all processes should be known")
    }

    // Returns the fast quorum.
    pub fn fast_quorum(&self) -> Vec<ProcessId> {
        self.fast_quorum
            .clone()
            .expect("the fast quorum should be known")
    }

    // Returns the write quorum.
    pub fn write_quorum(&self) -> Vec<ProcessId> {
        self.write_quorum
            .clone()
            .expect("the slow quorum should be known")
    }

    pub fn show_stats(&self) {
        self.metrics.show_stats();
    }

    // Increment fast path count.
    pub fn fast_path(&mut self) {
        self.inc_metric(MetricsKind::FastPath);
    }

    // Increment slow path count.
    pub fn slow_path(&mut self) {
        self.inc_metric(MetricsKind::SlowPath);
    }

    fn inc_metric(&mut self, kind: MetricsKind) {
        self.metrics.update(kind, |v| *v += 1)
    }
}

#[derive(Clone, Hash, PartialEq, Eq)]
enum MetricsKind {
    FastPath,
    SlowPath,
}

impl fmt::Debug for MetricsKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            MetricsKind::FastPath => write!(f, "fast_path"),
            MetricsKind::SlowPath => write!(f, "slow_path"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let planet = Planet::new("latency/");
        let fast_quorum_size = 6;
        let write_quorum_size = 4;
        let mut bp = BaseProcess::new(
            id,
            region,
            planet,
            config,
            fast_quorum_size,
            write_quorum_size,
        );

        // no quorum is set yet
        assert_eq!(bp.fast_quorum, None);
        assert_eq!(bp.all_processes, None);

        // discover processes and check we're connected
        assert!(bp.discover(processes));

        // check set of all processes
        assert_eq!(
            bp.all(),
            vec![8, 9, 6, 7, 5, 14, 10, 13, 12, 15, 16, 11, 1, 0, 4, 3, 2]
        );

        // check fast quorum
        assert_eq!(bp.fast_quorum(), vec![8, 9, 6, 7, 5, 14]);

        // check write quorum
        assert_eq!(bp.write_quorum(), vec![8, 9, 6, 7]);
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
        let planet = Planet::new("latency/");
        let fast_quorum_size = 3;
        let write_quorum_size = 4;
        let mut bp = BaseProcess::new(
            id,
            region,
            planet,
            config,
            fast_quorum_size,
            write_quorum_size,
        );

        // discover processes and check we're connected
        assert!(bp.discover(processes));

        // check set of all processes
        assert_eq!(bp.all(), vec![2, 3, 4, 0, 1]);

        // check fast quorum
        assert_eq!(bp.fast_quorum(), vec![2, 3, 4]);

        // check write quorum
        assert_eq!(bp.write_quorum(), vec![2, 3, 4, 0]);
    }
}
