use crate::config::Config;
use crate::planet::{Planet, Region};

pub type ProcId = u64;
pub type Dot = (ProcId, u64);

// a `BaseProc` has all functionalities shared by Atlas, Newt, ...
pub struct BaseProc {
    pub id: ProcId,
    pub region: Region,
    pub planet: Planet,
    pub config: Config,
    // fast quorum size
    pub q: usize,
    pub cmd_count: u64,
    pub fast_quorum: Option<Vec<ProcId>>,
    pub all_procs: Option<Vec<ProcId>>,
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
            fast_quorum: None,
            all_procs: None,
        }
    }

    /// Updates the processes known by this process.
    pub fn discover(&mut self, mut procs: Vec<(ProcId, Region)>) {
        let region_to_index = self
            .planet
            .sorted_by_distance_and_indexed(&self.region)
            .unwrap();

        // use the region order (based on distance) to order processes
        // - if two processes are from the same region, they're sorted by id
        procs.sort_by(|(id_a, region_a), (id_b, region_b)| {
            if region_a == region_b {
                id_a.cmp(id_b)
            } else {
                let index_a = region_to_index.get(region_a).unwrap();
                let index_b = region_to_index.get(region_b).unwrap();
                index_a.cmp(index_b)
            }
        });

        // create fast quorum by taking the first `q` elements
        let mut count = 0;
        let fast_quorum = procs
            .iter()
            .take_while(|_| {
                count += 1;
                count <= self.q
            })
            .map(|(id, _)| id)
            .cloned()
            .collect();

        // create all procs
        let all_procs = procs.into_iter().map(|(id, _)| id).collect();

        // set fast quorum and all procs
        self.fast_quorum = Some(fast_quorum);
        self.all_procs = Some(all_procs);
    }

    /// Increments `cmd_count` and returns the next dot.
    pub fn next_dot(&mut self) -> Dot {
        self.cmd_count += 1;
        (self.id, self.cmd_count)
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

        assert_eq!(bp.next_dot(), (id, 1));
        assert_eq!(bp.next_dot(), (id, 2));
        assert_eq!(bp.next_dot(), (id, 3));
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

        assert_eq!(bp.fast_quorum, Some(vec![8, 9, 6, 7, 5, 14]));
        assert_eq!(
            bp.all_procs,
            Some(vec![
                8, 9, 6, 7, 5, 14, 10, 13, 12, 15, 16, 11, 1, 0, 4, 3, 2
            ])
        );
    }
}
