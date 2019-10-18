pub mod float;
pub mod protocol;
pub mod search;
pub mod stats;

use crate::bote::stats::Stats;
use crate::planet::{Planet, Region};
use std::cmp::Ordering;
use std::collections::HashMap;

#[derive(Debug)]
pub struct Bote {
    planet: Planet,
}

impl Bote {
    pub fn new(lat_dir: &str) -> Self {
        let planet = Planet::new(lat_dir);
        Bote::from(planet)
    }

    pub fn from(planet: Planet) -> Self {
        Bote { planet }
    }

    pub fn leaderless(
        &self,
        servers: &Vec<Region>,
        clients: &Vec<Region>,
        quorum_size: usize,
    ) -> Stats {
        // compute the quorum latency for each server
        let quorum_latencies =
            self.latency_to_closest_quorum(servers, quorum_size);

        let latencies: Vec<_> = clients
            .into_iter()
            .map(|client| {
                // compute the latency from this client to the closest region
                let (client_to_closest, closest) =
                    self.nth_closest(1, client, servers);

                // compute the latency from such region to its closest quorum
                let closest_to_quorum = quorum_latencies.get(closest).unwrap();

                // client perceived latency is the sum of both
                client_to_closest + closest_to_quorum
            })
            .collect();

        // compute stats from these client perceived latencies
        Stats::from(&latencies)
    }

    pub fn best_mean_leader(
        &self,
        servers: &Vec<Region>,
        clients: &Vec<Region>,
        quorum_size: usize,
    ) -> Stats {
        self.best_leader(servers, clients, quorum_size, |a, b| {
            a.mean().cmp(&b.mean())
        })
    }

    pub fn best_cov_leader(
        &self,
        servers: &Vec<Region>,
        clients: &Vec<Region>,
        quorum_size: usize,
    ) -> Stats {
        self.best_leader(servers, clients, quorum_size, |a, b| {
            a.cov().cmp(&b.cov())
        })
    }

    pub fn best_mdtm_leader(
        &self,
        servers: &Vec<Region>,
        clients: &Vec<Region>,
        quorum_size: usize,
    ) -> Stats {
        self.best_leader(servers, clients, quorum_size, |a, b| {
            a.mdtm().cmp(&b.mdtm())
        })
    }

    fn best_leader<F>(
        &self,
        servers: &Vec<Region>,
        clients: &Vec<Region>,
        quorum_size: usize,
        compare: F,
    ) -> Stats
    where
        F: Fn(&Stats, &Stats) -> Ordering,
    {
        // sort stats using `compare`
        let mut stats = self.leaders_stats(servers, clients, quorum_size);
        stats.sort_unstable_by(compare);

        // get the lowest (in terms of `compare`) stat
        stats.into_iter().next().unwrap()
    }

    /// Compute stats for all possible leaders.
    fn leaders_stats(
        &self,
        servers: &Vec<Region>,
        clients: &Vec<Region>,
        quorum_size: usize,
    ) -> Vec<Stats> {
        self.leader(servers, clients, quorum_size)
            .into_iter()
            .map(|(_, stats)| stats)
            .collect()
    }

    fn leader<'a>(
        &self,
        servers: &'a Vec<Region>,
        clients: &Vec<Region>,
        quorum_size: usize,
    ) -> HashMap<&'a Region, Stats> {
        // compute the quorum latency for each possible leader
        let quorum_latencies =
            self.latency_to_closest_quorum(servers, quorum_size);

        // compute latency stats for each possible leader
        servers
            .iter()
            .map(|leader| {
                // compute the latency from leader to its closest quorum
                let leader_to_quorum = quorum_latencies.get(&leader).unwrap();

                // compute perceived latency for each client
                let latencies: Vec<_> = clients
                    .iter()
                    .map(|client| {
                        // compute the latency from client to leader
                        let client_to_leader =
                            self.planet.latency(client, &leader).unwrap();
                        // client perceived latency is the sum of both
                        client_to_leader + leader_to_quorum
                    })
                    .collect();

                // compute stats from these client perceived latencies
                let stats = Stats::from(&latencies);

                (leader, stats)
            })
            .collect()
    }

    /// Compute the latency to closest quorum of a size `quorum_size`.
    fn latency_to_closest_quorum<'a>(
        &self,
        regions: &'a Vec<Region>,
        quorum_size: usize,
    ) -> HashMap<&'a Region, usize> {
        regions
            .iter()
            .map(|from| {
                // for each region, get the latency to the `quorum_size`th
                // closest region
                let (latency, _) =
                    self.nth_closest(quorum_size, &from, &regions);
                (from, *latency)
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
    ) -> &(usize, Region) {
        self.planet
            // sort by distance
            .sorted_by_distance(from)
            .unwrap()
            .into_iter()
            // keep only the regions in this configuration
            .filter(|(_, to)| regions.contains(to))
            // select the nth region
            .nth(nth - 1)
            .unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn latency_to_closest_quorum() {
        // create bote
        let lat_dir = "latency/";
        let bote = Bote::new(lat_dir);

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
        let cql = bote.latency_to_closest_quorum(&regions, quorum_size);
        assert_eq!(*cql.get(&w1).unwrap(), 7);
        assert_eq!(*cql.get(&w2).unwrap(), 9);
        assert_eq!(*cql.get(&w3).unwrap(), 7);
        assert_eq!(*cql.get(&w4).unwrap(), 7);
        assert_eq!(*cql.get(&w6).unwrap(), 7);

        // quorum size 3
        let quorum_size = 3;
        let cql = bote.latency_to_closest_quorum(&regions, quorum_size);
        assert_eq!(*cql.get(&w1).unwrap(), 8);
        assert_eq!(*cql.get(&w2).unwrap(), 10);
        assert_eq!(*cql.get(&w3).unwrap(), 7);
        assert_eq!(*cql.get(&w4).unwrap(), 7);
        assert_eq!(*cql.get(&w6).unwrap(), 14);
    }

    #[test]
    fn leaderless() {
        // create bote
        let lat_dir = "latency/";
        let bote = Bote::new(lat_dir);

        // considered regions
        let w1 = Region::new("europe-west1");
        let w2 = Region::new("europe-west2");
        let w3 = Region::new("europe-west3");
        let w4 = Region::new("europe-west4");
        let w6 = Region::new("europe-west6");
        let regions =
            vec![w1.clone(), w2.clone(), w3.clone(), w4.clone(), w6.clone()];

        // quorum size 3
        let quorum_size = 3;
        let stats = bote.leaderless(&regions, &regions, quorum_size);
        // w1 -> 9, w2 -> 11, w3 -> 8, w4 -> 8, w6 -> 15
        assert_eq!(stats.show_mean(), "10.2");
        assert_eq!(stats.show_cov(), "0.3");
        assert_eq!(stats.show_mdtm(), "2.2");

        // quorum size 4
        let quorum_size = 4;
        let stats = bote.leaderless(&regions, &regions, quorum_size);
        // w1 -> 11, w2 -> 14, w3 -> 9, w4 -> 10, w6 -> 15
        assert_eq!(stats.show_mean(), "11.8");
        assert_eq!(stats.show_cov(), "0.2");
        assert_eq!(stats.show_mdtm(), "2.2");
    }

    #[test]
    fn leaderless_clients_subset() {
        // create bote
        let lat_dir = "latency/";
        let bote = Bote::new(lat_dir);

        // considered regions
        let w1 = Region::new("europe-west1");
        let w2 = Region::new("europe-west2");
        let w3 = Region::new("europe-west3");
        let w4 = Region::new("europe-west4");
        let w6 = Region::new("europe-west6");
        let servers =
            vec![w1.clone(), w2.clone(), w3.clone(), w4.clone(), w6.clone()];

        // subset of clients: w1 w2
        let clients = vec![w1.clone(), w2.clone()];

        // quorum size 3
        let quorum_size = 3;
        let stats = bote.leaderless(&servers, &clients, quorum_size);
        // w1 -> 9, w2 -> 11
        assert_eq!(stats.show_mean(), "10.0");
        assert_eq!(stats.show_cov(), "0.1");
        assert_eq!(stats.show_mdtm(), "1.0");

        // quorum size 4
        let quorum_size = 4;
        let stats = bote.leaderless(&servers, &clients, quorum_size);
        // w1 -> 11, w2 -> 14
        assert_eq!(stats.show_mean(), "12.5");
        assert_eq!(stats.show_cov(), "0.2");
        assert_eq!(stats.show_mdtm(), "1.5");

        // subset of clients: w1 w3 w6
        let clients = vec![w1.clone(), w3.clone(), w6.clone()];

        // quorum size 3
        let quorum_size = 3;
        let stats = bote.leaderless(&servers, &clients, quorum_size);
        // w1 -> 9, w3 -> 8, w6 -> 15
        assert_eq!(stats.show_mean(), "10.7");
        assert_eq!(stats.show_cov(), "0.4");
        assert_eq!(stats.show_mdtm(), "2.9");

        // quorum size 4
        let quorum_size = 4;
        let stats = bote.leaderless(&servers, &clients, quorum_size);
        // w1 -> 11, w3 -> 9, w6 -> 15
        assert_eq!(stats.show_mean(), "11.7");
        assert_eq!(stats.show_cov(), "0.3");
        assert_eq!(stats.show_mdtm(), "2.2");
    }

    #[test]
    fn leader() {
        // create bote
        let lat_dir = "latency/";
        let bote = Bote::new(lat_dir);

        // considered regions
        let w1 = Region::new("europe-west1");
        let w2 = Region::new("europe-west2");
        let w3 = Region::new("europe-west3");
        let w4 = Region::new("europe-west4");
        let w6 = Region::new("europe-west6");
        let regions =
            vec![w1.clone(), w2.clone(), w3.clone(), w4.clone(), w6.clone()];

        // quorum size 2:
        let quorum_size = 2;
        let leader_to_stats = bote.leader(&regions, &regions, quorum_size);

        // quorum latency for w1 is 7
        // w1 -> 8, w2 -> 17, w3 -> 15, w4 -> 14, w6 -> 21
        let stats = leader_to_stats.get(&w1).unwrap();
        assert_eq!(stats.show_mean(), "15.0");
        assert_eq!(stats.show_cov(), "0.3");
        assert_eq!(stats.show_mdtm(), "3.2");

        // quorum latency for w2 is 9
        // w1 -> 19, w2 -> 10, w3 -> 22, w4 -> 18, w6 -> 28
        let stats = leader_to_stats.get(&w2).unwrap();
        assert_eq!(stats.show_mean(), "19.4");
        assert_eq!(stats.show_cov(), "0.3");
        assert_eq!(stats.show_mdtm(), "4.5");

        // quorum latency for w3 is 7
        // w1 -> 15, w2 -> 20, w3 -> 8, w4 -> 14, w6 -> 14
        let stats = leader_to_stats.get(&w3).unwrap();
        assert_eq!(stats.show_mean(), "14.2");
        assert_eq!(stats.show_cov(), "0.3");
        assert_eq!(stats.show_mdtm(), "2.6");
    }

    #[test]
    fn leader_clients_subset() {
        // create bote
        let lat_dir = "latency/";
        let bote = Bote::new(lat_dir);

        // considered regions
        let w1 = Region::new("europe-west1");
        let w2 = Region::new("europe-west2");
        let w3 = Region::new("europe-west3");
        let w4 = Region::new("europe-west4");
        let w6 = Region::new("europe-west6");
        let servers =
            vec![w1.clone(), w2.clone(), w3.clone(), w4.clone(), w6.clone()];

        // quorum size 2:
        let quorum_size = 2;

        // subset of clients: w1 w2
        let clients = vec![w1.clone(), w2.clone()];
        let leader_to_stats = bote.leader(&servers, &clients, quorum_size);

        // quorum latency for w1 is 7
        // w1 -> 8, w2 -> 17
        let stats = leader_to_stats.get(&w1).unwrap();
        assert_eq!(stats.show_mean(), "12.5");
        assert_eq!(stats.show_cov(), "0.5");
        assert_eq!(stats.show_mdtm(), "4.5");

        // quorum latency for w2 is 9
        // w1 -> 19, w2 -> 10
        let stats = leader_to_stats.get(&w2).unwrap();
        assert_eq!(stats.show_mean(), "14.5");
        assert_eq!(stats.show_cov(), "0.4");
        assert_eq!(stats.show_mdtm(), "4.5");

        // quorum latency for w3 is 7
        // w1 -> 15, w2 -> 20
        let stats = leader_to_stats.get(&w3).unwrap();
        assert_eq!(stats.show_mean(), "17.5");
        assert_eq!(stats.show_cov(), "0.2");
        assert_eq!(stats.show_mdtm(), "2.5");

        // subset of clients: w1 w3 w6
        let clients = vec![w1.clone(), w3.clone(), w6.clone()];
        let leader_to_stats = bote.leader(&servers, &clients, quorum_size);

        // quorum latency for w1 is 7
        // w1 -> 8, w3 -> 15, w6 -> 21
        let stats = leader_to_stats.get(&w1).unwrap();
        assert_eq!(stats.show_mean(), "14.7");
        assert_eq!(stats.show_cov(), "0.4");
        assert_eq!(stats.show_mdtm(), "4.4");

        // quorum latency for w2 is 9
        // w1 -> 19, w3 -> 22, w6 -> 28
        let stats = leader_to_stats.get(&w2).unwrap();
        assert_eq!(stats.show_mean(), "23.0");
        assert_eq!(stats.show_cov(), "0.2");
        assert_eq!(stats.show_mdtm(), "3.3");

        // quorum latency for w3 is 7
        // w1 -> 15, w3 -> 8, w6 -> 14
        let stats = leader_to_stats.get(&w3).unwrap();
        assert_eq!(stats.show_mean(), "12.3");
        assert_eq!(stats.show_cov(), "0.3");
        assert_eq!(stats.show_mdtm(), "2.9");
    }

    #[test]
    fn best_latency_leader() {
        // create bote
        let lat_dir = "latency/";
        let bote = Bote::new(lat_dir);

        // considered regions
        let w1 = Region::new("europe-west1");
        let w2 = Region::new("europe-west2");
        let w3 = Region::new("europe-west3");
        let w4 = Region::new("europe-west4");
        let w6 = Region::new("europe-west6");
        let regions =
            vec![w1.clone(), w2.clone(), w3.clone(), w4.clone(), w6.clone()];

        // quorum size 2:
        let quorum_size = 2;
        let stats = bote.best_mean_leader(&regions, &regions, quorum_size);

        assert_eq!(stats.show_mean(), "14.2");
        assert_eq!(stats.show_cov(), "0.3");
        assert_eq!(stats.show_mdtm(), "2.6");
    }
}
