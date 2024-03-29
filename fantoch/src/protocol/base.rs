use crate::command::Command;
use crate::config::Config;
use crate::id::{Dot, DotGen, ProcessId, ShardId};
use crate::protocol::{ProtocolMetrics, ProtocolMetricsKind};
use crate::trace;
use crate::{HashMap, HashSet};
use std::iter::FromIterator;

// a `BaseProcess` has all functionalities shared by Atlas, Tempo, ...
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseProcess {
    pub process_id: ProcessId,
    pub shard_id: ShardId,
    pub config: Config,
    all: Option<HashSet<ProcessId>>,
    all_but_me: Option<HashSet<ProcessId>>,
    majority_quorum: Option<HashSet<ProcessId>>,
    fast_quorum: Option<HashSet<ProcessId>>,
    write_quorum: Option<HashSet<ProcessId>>,
    // mapping from shard id (that are not the same as mine) to the closest
    // process from that shard
    closest_shard_process: HashMap<ShardId, ProcessId>,
    fast_quorum_size: usize,
    write_quorum_size: usize,
    dot_gen: DotGen,
    metrics: ProtocolMetrics,
}

impl BaseProcess {
    /// Creates a new `BaseProcess`.
    pub fn new(
        process_id: ProcessId,
        shard_id: ShardId,
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
            shard_id,
            config,
            all: None,
            all_but_me: None,
            majority_quorum: None,
            fast_quorum: None,
            write_quorum: None,
            closest_shard_process: HashMap::new(),
            fast_quorum_size,
            write_quorum_size,
            dot_gen: DotGen::new(process_id),
            metrics: ProtocolMetrics::new(),
        }
    }

    /// Updates the processes known by this process.
    /// The set of processes provided is already sorted by distance.
    pub fn discover(
        &mut self,
        all_processes: Vec<(ProcessId, ShardId)>,
    ) -> bool {
        // reset closest shard process
        self.closest_shard_process =
            HashMap::with_capacity(self.config.shard_count() - 1);

        // select processes from my shard and compute `closest_shard_process`
        let processes: Vec<_> = all_processes
            .into_iter()
            .filter_map(|(process_id, shard_id)| {
                // check if process belongs to my shard
                if self.shard_id == shard_id {
                    // if yes, keep process id
                    Some(process_id)
                } else {
                    // if not, then it must be the closest process from that shard (i.e. from the same region) as mine
                    assert!(self.closest_shard_process.insert(shard_id, process_id).is_none(), "process should only connect to the closest process from each shard");
                    None
                }
            })
            .collect();

        let majority_quorum_size = self.config.majority_quorum_size();
        // create majority quorum by taking the first `majority_quorum_size`
        // elements
        let majority_quorum: HashSet<_> = processes
            .clone()
            .into_iter()
            .take(majority_quorum_size)
            .collect();

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

        // set majority quorum if we have enough processes
        self.majority_quorum = if majority_quorum.len() == majority_quorum_size
        {
            Some(majority_quorum)
        } else {
            None
        };

        // set fast quorum if we have enough processes
        self.fast_quorum = if fast_quorum.len() == self.fast_quorum_size {
            Some(fast_quorum)
        } else {
            None
        };

        // set write quorum if we have enough processes
        self.write_quorum = if write_quorum.len() == self.write_quorum_size {
            Some(write_quorum)
        } else {
            None
        };

        trace!(
            "p{}: all_but_me {:?} | majority_quorum {:?} | fast_quorum {:?} | write_quorum {:?} | closest_shard_process {:?}",
            self.process_id,
            self.all_but_me,
            self.majority_quorum,
            self.fast_quorum,
            self.write_quorum,
            self.closest_shard_process
        );

        // connected if quorums are set
        self.majority_quorum.is_some()
            && self.fast_quorum.is_some()
            && self.write_quorum.is_some()
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

    // Returns a majority quorum.
    pub fn majority_quorum(&self) -> HashSet<ProcessId> {
        self.majority_quorum
            .clone()
            .expect("the majority quorum should be known")
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

    // Returns the closest process for this shard.
    pub fn closest_process(&self, shard_id: &ShardId) -> ProcessId {
        *self
            .closest_shard_process
            .get(shard_id)
            .expect("closest shard process should be known")
    }

    // Returns the closest process mapping.
    pub fn closest_shard_process(&self) -> &HashMap<ShardId, ProcessId> {
        &self.closest_shard_process
    }

    // Computes the quorum to be used:
    // - use majority quorum if NFR is enabled, and `cmd` is a single-key read
    // - use fast quorum otherwise
    pub fn maybe_adjust_fast_quorum(
        &self,
        cmd: &Command,
    ) -> HashSet<ProcessId> {
        if self.config.nfr() && cmd.nfr_allowed() {
            self.majority_quorum()
        } else {
            self.fast_quorum()
        }
    }

    // Return metrics.
    pub fn metrics(&self) -> &ProtocolMetrics {
        &self.metrics
    }

    // Update fast path metrics.
    pub fn path(&mut self, fast_path: bool, read_only: bool) {
        if fast_path {
            self.metrics.aggregate(ProtocolMetricsKind::FastPath, 1);
            if read_only {
                self.metrics
                    .aggregate(ProtocolMetricsKind::FastPathReads, 1);
            }
        } else {
            self.metrics.aggregate(ProtocolMetricsKind::SlowPath, 1);
            if read_only {
                self.metrics
                    .aggregate(ProtocolMetricsKind::SlowPathReads, 1);
            }
        }
    }

    // Accumulate more stable commands.
    pub fn stable(&mut self, len: usize) {
        self.metrics
            .aggregate(ProtocolMetricsKind::Stable, len as u64);
    }

    // Collect a new metric.
    pub fn collect_metric(&mut self, kind: ProtocolMetricsKind, value: u64) {
        self.metrics.collect(kind, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::Command;
    use crate::id::Rifl;
    use crate::kvs::KVOp;
    use crate::planet::{Planet, Region};
    use crate::util;
    use std::collections::BTreeSet;
    use std::iter::FromIterator;

    #[test]
    fn discover() {
        // processes
        let shard_id = 0;
        let processes = vec![
            (0, shard_id, Region::new("asia-east1")),
            (1, shard_id, Region::new("asia-northeast1")),
            (2, shard_id, Region::new("asia-south1")),
            (3, shard_id, Region::new("asia-southeast1")),
            (4, shard_id, Region::new("australia-southeast1")),
            (5, shard_id, Region::new("europe-north1")),
            (6, shard_id, Region::new("europe-west1")),
            (7, shard_id, Region::new("europe-west2")),
            (8, shard_id, Region::new("europe-west3")),
            (9, shard_id, Region::new("europe-west4")),
            (10, shard_id, Region::new("northamerica-northeast1")),
            (11, shard_id, Region::new("southamerica-east1")),
            (12, shard_id, Region::new("us-central1")),
            (13, shard_id, Region::new("us-east1")),
            (14, shard_id, Region::new("us-east4")),
            (15, shard_id, Region::new("us-west1")),
            (16, shard_id, Region::new("us-west2")),
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
        let mut bp = BaseProcess::new(
            id,
            shard_id,
            config,
            fast_quorum_size,
            write_quorum_size,
        );

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
        let shard_id = 0;
        let processes = vec![
            (0, shard_id, Region::new("asia-east1")),
            (1, shard_id, Region::new("asia-east1")),
            (2, shard_id, Region::new("europe-north1")),
            (3, shard_id, Region::new("europe-north1")),
            (4, shard_id, Region::new("europe-west1")),
        ];

        // config
        let n = 5;
        let f = 2;
        let config = Config::new(n, f);

        // bp
        let id = 2;
        let shard_id = 0;
        let region = Region::new("europe-north1");
        let planet = Planet::new();
        let fast_quorum_size = 3;
        let write_quorum_size = 4;
        let mut bp = BaseProcess::new(
            id,
            shard_id,
            config,
            fast_quorum_size,
            write_quorum_size,
        );

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

    #[test]
    fn discover_two_shards() {
        let shard_id_0 = 0;
        let shard_id_1 = 1;

        // config
        let n = 3;
        let f = 1;
        let config = Config::new(n, f);

        // check for bp id = 1, shard_id = 0
        let fast_quorum_size = 2;
        let write_quorum_size = 2;
        let mut bp = BaseProcess::new(
            1,
            shard_id_0,
            config,
            fast_quorum_size,
            write_quorum_size,
        );

        // processes
        let sorted = vec![
            (1, shard_id_0),
            (4, shard_id_1),
            (2, shard_id_0),
            (3, shard_id_0),
        ];
        assert!(bp.discover(sorted));

        // check set of all processes
        assert_eq!(
            BTreeSet::from_iter(bp.all()),
            BTreeSet::from_iter(vec![1, 2, 3])
        );

        // check set of all processes (but self)
        assert_eq!(
            BTreeSet::from_iter(bp.all_but_me()),
            BTreeSet::from_iter(vec![2, 3])
        );

        // check fast quorum
        assert_eq!(
            BTreeSet::from_iter(bp.fast_quorum()),
            BTreeSet::from_iter(vec![1, 2])
        );

        // check write quorum
        assert_eq!(
            BTreeSet::from_iter(bp.write_quorum()),
            BTreeSet::from_iter(vec![1, 2])
        );

        assert_eq!(bp.closest_shard_process.len(), 1);
        assert_eq!(bp.closest_shard_process.get(&shard_id_0), None);
        assert_eq!(bp.closest_shard_process.get(&shard_id_1), Some(&4));

        // check replicated by
        let mut ops = HashMap::new();
        ops.insert(String::from("a"), vec![KVOp::Get]);

        // create command replicated by shard 0
        let rifl = Rifl::new(1, 1);
        let mut shard_to_ops = HashMap::new();
        shard_to_ops.insert(shard_id_0, ops.clone());
        let cmd_shard_0 = Command::new(rifl, shard_to_ops);
        assert!(cmd_shard_0.replicated_by(&shard_id_0));
        assert!(!cmd_shard_0.replicated_by(&shard_id_1));

        // create command replicated by shard 1
        let rifl = Rifl::new(1, 2);
        let mut shard_to_ops = HashMap::new();
        shard_to_ops.insert(shard_id_1, ops.clone());
        let cmd_shard_1 = Command::new(rifl, shard_to_ops);
        assert!(!cmd_shard_1.replicated_by(&shard_id_0));
        assert!(cmd_shard_1.replicated_by(&shard_id_1));

        // create command replicated by both shards
        let rifl = Rifl::new(1, 3);
        let mut shard_to_ops = HashMap::new();
        shard_to_ops.insert(shard_id_0, ops.clone());
        shard_to_ops.insert(shard_id_1, ops.clone());
        let cmd_both_shards = Command::new(rifl, shard_to_ops);
        assert!(cmd_both_shards.replicated_by(&shard_id_0));
        assert!(cmd_both_shards.replicated_by(&shard_id_1));
    }
}
