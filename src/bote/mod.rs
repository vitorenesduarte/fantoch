mod protocol;
mod stats;

use crate::bote::stats::Stats;
use crate::planet::{Planet, Region};
use std::collections::HashMap;

pub struct Bote {
    planet: Planet,
}

impl Bote {
    pub fn new(planet: Planet) -> Self {
        Bote { planet }
    }

    pub fn run(&self) {
        println!("bote!");
    }

    fn latency_to_closest_quorum(
        &self,
        regions: Vec<Region>,
        quorum_size: usize,
    ) -> HashMap<Region, usize> {
        regions
            .clone() // TODO can we avoid this clone?
            .into_iter()
            .map(|from| {
                let (_, latency) =
                    self.nth_closest(quorum_size, &from, &regions);
                (from, latency)
            })
            .collect()
    }

    /// Compute the latency to the nth closest region.
    /// This same method can be used to find the:
    /// - latency to the closest quorum
    /// - latency to the closest region
    fn nth_closest(
        &self,
        nth: usize,
        from: &Region,
        regions: &Vec<Region>,
    ) -> (&Region, usize) {
        self.planet
            .sorted_by_distance(from)
            .unwrap()
            .into_iter()
            .filter(|(to, _)| regions.contains(to))
            .nth(nth - 1)
            .unwrap()
    }

    fn leaderless(
        &self,
        clients: Vec<Region>,
        servers: Vec<Region>,
        quorum_size: usize,
    ) -> Stats {
        let quorum_latencies =
            self.latency_to_closest_quorum(servers.clone(), quorum_size); // TODO can we also avoid this clone?
        let latencies: Vec<_> = clients
            .into_iter()
            .map(|client| {
                // compute the latency from client to the closest available
                // region
                let (closest, client_to_closest) =
                    self.nth_closest(1, &client, &servers);
                // compute the latency from such region to its closest quorum
                let closest_to_quorum = quorum_latencies.get(closest).unwrap();
                // the final latency is the sum of both the above
                client_to_closest + closest_to_quorum
            })
            .collect();
        // compute stats
        Stats::from(&latencies)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn latency_to_closest_quorum() {
        // create planet
        let lat_dir = "latency/";
        let planet = Planet::new(lat_dir);

        // create bote
        let bote = Bote::new(planet);

        // considered regions
        let w1 = Region::new("europe-west1");
        let w2 = Region::new("europe-west2");
        let w3 = Region::new("europe-west3");
        let w4 = Region::new("europe-west4");
        let w6 = Region::new("europe-west6");
        let regions =
            vec![w1.clone(), w2.clone(), w3.clone(), w4.clone(), w6.clone()];

        // quorum size 2
        let quorum_size = 2;
        let cql = bote.latency_to_closest_quorum(regions.clone(), quorum_size);
        assert_eq!(*cql.get(&w1).unwrap(), 7);
        assert_eq!(*cql.get(&w2).unwrap(), 9);
        assert_eq!(*cql.get(&w3).unwrap(), 7);
        assert_eq!(*cql.get(&w4).unwrap(), 7);
        assert_eq!(*cql.get(&w6).unwrap(), 7);

        // quorum size 3
        let quorum_size = 3;
        let cql = bote.latency_to_closest_quorum(regions.clone(), quorum_size);
        assert_eq!(*cql.get(&w1).unwrap(), 8);
        assert_eq!(*cql.get(&w2).unwrap(), 10);
        assert_eq!(*cql.get(&w3).unwrap(), 7);
        assert_eq!(*cql.get(&w4).unwrap(), 7);
        assert_eq!(*cql.get(&w6).unwrap(), 14);
    }
}
