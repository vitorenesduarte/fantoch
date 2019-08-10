use crate::config::Config;
use crate::planet::{Planet, Region};

pub type ProcId = usize;
pub type Dot = (ProcId, u64);

// a `BaseProc` has all functionalities shared by Atlas, Newt, ...
pub struct BaseProc {
    pub id: ProcId,
    pub region: Region,
    pub planet: Planet,
    pub config: Config,
    pub fast_quorum_size: usize,
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
        fast_quorum_size: usize,
    ) -> Self {
        BaseProc {
            id,
            region,
            planet,
            config,
            fast_quorum_size,
            cmd_count: 0,
            fast_quorum: None,
            all_procs: None,
        }
    }

    /// Updates the processes known by this process.
    pub fn discover(&mut self, mut procs: Vec<(ProcId, Region)>) {
        let region_to_index =
            self.planet.sorted_by_distance(&self.region).unwrap();

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

        // create fast quorum by taking the first `fast_quorum_size` elements
        let mut count = 0;
        let fast_quorum = procs
            .iter()
            .take_while(|_| {
                count += 1;
                count <= self.fast_quorum_size
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
    use crate::base::BaseProc;
    use crate::config::Config;
    use crate::planet::{Planet, Region};

    #[test]
    fn next_dot() {
        // config
        let n = 5;
        let f = 1;
        let config = Config::new(n, f);

        // bp
        let id = 0;
        let region = Region::new("europe-west3");
        let planet = Planet::new("latency/");
        let fast_quorum_size = 2;
        let mut bp =
            BaseProc::new(id, region, planet, config, fast_quorum_size);

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
        let fast_quorum_size = 6;
        let mut bp =
            BaseProc::new(id, region, planet, config, fast_quorum_size);

        // no quorum is set yet
        assert_eq!(bp.fast_quorum, None);
        assert_eq!(bp.all_procs, None);

        // discover procs
        bp.discover(procs);

        assert_eq!(bp.fast_quorum, Some(vec![8, 6, 9, 7, 5, 14]));
        assert_eq!(
            bp.all_procs,
            Some(vec![
                8, 6, 9, 7, 5, 14, 10, 13, 12, 15, 16, 11, 1, 0, 4, 3, 2
            ])
        );
    }
}
