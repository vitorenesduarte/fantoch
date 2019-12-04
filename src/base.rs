use crate::config::Config;
use crate::planet::{Planet, Region};
use std::collections::HashMap;

pub type ProcId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Dot {
    proc_id: ProcId,
    seq: u64,
}

impl Dot {
    /// Creates a new `Dot` identifier.
    pub fn new(proc_id: ProcId, seq: u64) -> Self {
        Self { proc_id, seq }
    }

    /// Retrieves the identifier of the process that created this `Dot`.
    pub fn proc_id(&self) -> ProcId {
        self.proc_id
    }
}

// a `BaseProc` has all functionalities shared by Atlas, Newt, ...
pub struct BaseProc {
    pub id: ProcId,
    pub region: Region,
    pub planet: Planet,
    pub config: Config,
    // fast quorum size
    pub q: usize,
    pub cmd_count: u64,
    pub all_procs: Option<Vec<ProcId>>,
    pub fast_quorum: Option<Vec<ProcId>>,
}

impl BaseProc {
    /// Creates a new `BaseProc`.
    pub fn new(
        id: ProcId,
        region: Region,
        planet: Planet,
        config: Config,
        q: usize,
    ) -> Self {
        // since processes lead with ballot `id` when taking the slow path and
        // we may rely on the fact that a zero accepted ballot means the process
        // has never been through Paxos phase-2, all ids must non-zero
        assert!(id != 0);
        Self {
            id,
            region,
            planet,
            config,
            q,
            cmd_count: 0,
            all_procs: None,
            fast_quorum: None,
        }
    }

    /// Updates the processes known by this process.
    pub fn discover(&mut self, mut procs: Vec<(ProcId, Region)>) {
        // TODO the following computation could be cached
        let indexes: HashMap<_, _> = self
            .planet
            // get all regions sorted by distance from `self.region`
            .sorted(&self.region)
            .expect("region should be part of planet")
            .into_iter()
            // create a mapping from region to its index
            .enumerate()
            .map(|(index, (_distance, region))| (region, index))
            .collect();

        // use the region order index (based on distance) to order `procs`
        // - if two `procs` are from the same region, they're sorted by id
        procs.sort_unstable_by(|(id_a, a), (id_b, b)| {
            if a == b {
                id_a.cmp(id_b)
            } else {
                let index_a = indexes.get(a).expect("region should exist");
                let index_b = indexes.get(b).expect("region should exist");
                index_a.cmp(index_b)
            }
        });

        // create all procs
        let all_procs: Vec<_> = procs.into_iter().map(|(id, _)| id).collect();

        // create fast quorum by taking the first `q` elements
        let fast_quorum = all_procs.clone().into_iter().take(self.q).collect();

        // set fast quorum and all procs
        self.all_procs = Some(all_procs);
        self.fast_quorum = Some(fast_quorum);
    }

    /// Increments `cmd_count` and returns the next dot.
    pub fn next_dot(&mut self) -> Dot {
        self.cmd_count += 1;
        Dot::new(self.id, self.cmd_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_dot() {
        // config
        let n = 5;
        let f = 1;
        let config = Config::new(n, f);

        // bp
        let id = 1;
        let region = Region::new("europe-west3");
        let planet = Planet::new("latency/");
        let q = 2;
        let mut bp = BaseProc::new(id, region, planet, config, q);

        assert_eq!(bp.next_dot(), Dot::new(id, 1));
        assert_eq!(bp.next_dot(), Dot::new(id, 2));
        assert_eq!(bp.next_dot(), Dot::new(id, 3));
    }

    #[test]
    fn discover() {
        // procs
        let procs = vec![
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
        let q = 6;
        let mut bp = BaseProc::new(id, region, planet, config, q);

        // no quorum is set yet
        assert_eq!(bp.fast_quorum, None);
        assert_eq!(bp.all_procs, None);

        // discover procs
        bp.discover(procs);

        assert_eq!(
            bp.all_procs,
            Some(vec![
                8, 9, 6, 7, 5, 14, 10, 13, 12, 15, 16, 11, 1, 0, 4, 3, 2
            ])
        );
        assert_eq!(bp.fast_quorum, Some(vec![8, 9, 6, 7, 5, 14]));
    }

    #[test]
    fn discover_same_region() {
        // procs
        let procs = vec![
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
        let q = 3;
        let mut bp = BaseProc::new(id, region, planet, config, q);

        // discover procs
        bp.discover(procs);

        assert_eq!(bp.all_procs, Some(vec![2, 3, 4, 0, 1]));
        assert_eq!(bp.fast_quorum, Some(vec![2, 3, 4]));
    }
}
