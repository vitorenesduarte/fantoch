// This module contains the definition of `Protocol`, `ClientPlacement` and `ProtocolStats`.
pub mod protocol;

// This module contains the definition of `Search`.
pub mod search;

// Re-exports.
pub use search::Search;

use crate::metrics::{Stats, StatsKind};
use crate::planet::{Planet, Region};

#[derive(Debug)]
pub struct Bote {
    planet: Planet,
}

impl Bote {
    pub fn new(lat_dir: &str) -> Self {
        let planet = Planet::new(lat_dir);
        Self::from(planet)
    }

    pub fn from(planet: Planet) -> Self {
        Self { planet }
    }

    /// Computes `Stats` for a leaderless-based protocol with a given
    /// `quorum_size`.
    ///
    /// Takes as input two lists of regions:
    /// - one list being the regions where `servers` are
    /// - one list being the regions where `clients` are
    pub fn leaderless(
        &self,
        servers: &[Region],
        clients: &[Region],
        quorum_size: usize,
    ) -> Stats<u64> {
        let latencies: Vec<_> = clients
            .iter()
            .map(|client| {
                // compute the latency from this client to the closest region
                let (client_to_closest, closest) = self.nth_closest(1, client, servers);

                // compute the latency from such region to its closest quorum
                let closest_to_quorum = self.quorum_latency(closest, servers, quorum_size);

                // client perceived latency is the sum of both
                client_to_closest + closest_to_quorum
            })
            .collect();

        // compute stats from these client perceived latencies
        Stats::from(latencies)
    }

    /// Computes `Stats` for a leader-based protocol with a given
    /// `quorum_size` for some `leader`.
    ///
    /// Takes as input two lists of regions:
    /// - one list being the regions where `servers` are
    /// - one list being the regions where `clients` are
    pub fn leader(
        &self,
        leader: &Region,
        servers: &[Region],
        clients: &[Region],
        quorum_size: usize,
    ) -> Stats<u64> {
        // compute the latency from leader to its closest quorum
        let leader_to_quorum = self.quorum_latency(leader, servers, quorum_size);

        // compute perceived latency for each client
        let latencies: Vec<_> = clients
            .iter()
            .map(|client| {
                // compute the latency from client to leader
                let client_to_leader = self.planet.ping_latency(client, &leader).unwrap();
                // client perceived latency is the sum of both
                client_to_leader + leader_to_quorum
            })
            .collect();

        // compute stats from these client perceived latencies
        Stats::from(latencies)
    }

    /// Computes the best leader (for some criteria) and its `Stats` for a
    /// leader-based protocol with a given `quorum_size`.
    ///
    /// Takes as input two lists of regions:
    /// - one list being the regions where `servers` are
    /// - one list being the regions where `clients` are
    ///
    /// The best leader is select based on sort criteria `stats_sort_by`.
    pub fn best_leader<'a>(
        &self,
        servers: &'a [Region],
        clients: &[Region],
        quorum_size: usize,
        stats_sort_by: StatsKind,
    ) -> (&'a Region, Stats<u64>) {
        // compute all stats
        let mut stats = self.all_leaders_stats(servers, clients, quorum_size);

        // select the best leader based on `stats_sort_by`
        stats.sort_unstable_by(|(_la, sa), (_lb, sb)| match stats_sort_by {
            StatsKind::Mean => sa.mean().cmp(&sb.mean()),
            StatsKind::COV => sa.cov().cmp(&sb.cov()),
            StatsKind::MDTM => sa.mdtm().cmp(&sb.mdtm()),
        });

        // get the lowest (in terms of `compare`) stat
        stats.into_iter().next().unwrap()
    }

    /// Computes `Stats` for a leader-based protocol with a given `quorum_size`
    /// for each possible leader.
    ///
    /// Takes as input two lists of regions:
    /// - one list being the regions where `servers` are
    /// - one list being the regions where `clients` are
    fn all_leaders_stats<'a>(
        &self,
        servers: &'a [Region],
        clients: &[Region],
        quorum_size: usize,
    ) -> Vec<(&'a Region, Stats<u64>)> {
        // compute stats for each possible leader
        servers
            .iter()
            .map(|leader| {
                // compute stats
                let stats = self.leader(leader, servers, clients, quorum_size);
                (leader, stats)
            })
            .collect()
    }

    /// Computes the latency to closest quorum of size `quorum_size`.
    /// It takes as input the considered source region `from` and all available
    /// `regions`.
    fn quorum_latency(&self, from: &Region, regions: &[Region], quorum_size: usize) -> u64 {
        let (latency, _) = self.nth_closest(quorum_size, &from, &regions);
        *latency
    }

    /// Compute the latency to the nth closest region.
    /// This same method can be used to find the:
    /// - latency to the closest quorum
    /// - latency to the closest region
    fn nth_closest(&self, nth: usize, from: &Region, regions: &[Region]) -> &(u64, Region) {
        self.planet
            // sort by distance
            .sorted(from)
            .unwrap()
            .iter()
            // keep only the regions in this configuration
            .filter(|(_, to)| regions.contains(to))
            // select the nth region
            .nth(nth - 1)
            .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn quorum_latencies() {
        // create bote
        let lat_dir = "latency/";
        let bote = Bote::new(lat_dir);

        // considered regions
        let w1 = Region::new("europe-west1");
        let w2 = Region::new("europe-west2");
        let w3 = Region::new("europe-west3");
        let w4 = Region::new("europe-west4");
        let w6 = Region::new("europe-west6");
        let regions = vec![w1.clone(), w2.clone(), w3.clone(), w4.clone(), w6.clone()];

        // quorum size 2
        let quorum_size = 2;
        assert_eq!(bote.quorum_latency(&w1, &regions, quorum_size), 7);
        assert_eq!(bote.quorum_latency(&w2, &regions, quorum_size), 9);
        assert_eq!(bote.quorum_latency(&w3, &regions, quorum_size), 7);
        assert_eq!(bote.quorum_latency(&w4, &regions, quorum_size), 7);
        assert_eq!(bote.quorum_latency(&w6, &regions, quorum_size), 7);

        // quorum size 3
        let quorum_size = 3;
        assert_eq!(bote.quorum_latency(&w1, &regions, quorum_size), 8);
        assert_eq!(bote.quorum_latency(&w2, &regions, quorum_size), 10);
        assert_eq!(bote.quorum_latency(&w3, &regions, quorum_size), 7);
        assert_eq!(bote.quorum_latency(&w4, &regions, quorum_size), 7);
        assert_eq!(bote.quorum_latency(&w6, &regions, quorum_size), 14);
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
        let regions = vec![w1.clone(), w2.clone(), w3.clone(), w4.clone(), w6.clone()];

        // quorum size 3
        let quorum_size = 3;
        let stats = bote.leaderless(&regions, &regions, quorum_size);
        // w1 -> 9, w2 -> 11, w3 -> 8, w4 -> 8, w6 -> 15
        assert_eq!(stats.mean().round(), "9.2");
        assert_eq!(stats.cov().round(), "0.3");
        assert_eq!(stats.mdtm().round(), "2.2");

        // quorum size 4
        let quorum_size = 4;
        let stats = bote.leaderless(&regions, &regions, quorum_size);
        // w1 -> 11, w2 -> 14, w3 -> 9, w4 -> 10, w6 -> 15
        assert_eq!(stats.mean().round(), "10.8");
        assert_eq!(stats.cov().round(), "0.2");
        assert_eq!(stats.mdtm().round(), "2.2");
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
        let servers = vec![w1.clone(), w2.clone(), w3.clone(), w4.clone(), w6.clone()];

        // subset of clients: w1 w2
        let clients = vec![w1.clone(), w2.clone()];

        // quorum size 3
        let quorum_size = 3;
        let stats = bote.leaderless(&servers, &clients, quorum_size);
        // w1 -> 9, w2 -> 11
        assert_eq!(stats.mean().round(), "9.0");
        assert_eq!(stats.cov().round(), "0.2");
        assert_eq!(stats.mdtm().round(), "1.0");

        // quorum size 4
        let quorum_size = 4;
        let stats = bote.leaderless(&servers, &clients, quorum_size);
        // w1 -> 11, w2 -> 14
        assert_eq!(stats.mean().round(), "11.5");
        assert_eq!(stats.cov().round(), "0.2");
        assert_eq!(stats.mdtm().round(), "1.5");

        // subset of clients: w1 w3 w6
        let clients = vec![w1.clone(), w3.clone(), w6.clone()];

        // quorum size 3
        let quorum_size = 3;
        let stats = bote.leaderless(&servers, &clients, quorum_size);
        // w1 -> 9, w3 -> 8, w6 -> 15
        assert_eq!(stats.mean().round(), "9.7");
        assert_eq!(stats.cov().round(), "0.4");
        assert_eq!(stats.mdtm().round(), "2.9");

        // quorum size 4
        let quorum_size = 4;
        let stats = bote.leaderless(&servers, &clients, quorum_size);
        // w1 -> 11, w3 -> 9, w6 -> 15
        assert_eq!(stats.mean().round(), "10.7");
        assert_eq!(stats.cov().round(), "0.3");
        assert_eq!(stats.mdtm().round(), "2.2");
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
        let regions = vec![w1.clone(), w2.clone(), w3.clone(), w4.clone(), w6.clone()];

        // quorum size 2:
        let quorum_size = 2;
        let leader_to_stats: HashMap<_, _> = bote
            .all_leaders_stats(&regions, &regions, quorum_size)
            .into_iter()
            .collect();

        // quorum latency for w1 is 7
        // w1 -> 8, w2 -> 17, w3 -> 15, w4 -> 14, w6 -> 21
        let stats = leader_to_stats.get(&w1).unwrap();
        assert_eq!(stats.mean().round(), "14.8");
        assert_eq!(stats.cov().round(), "0.3");
        assert_eq!(stats.mdtm().round(), "3.4");

        // quorum latency for w2 is 9
        // w1 -> 19, w2 -> 10, w3 -> 22, w4 -> 18, w6 -> 28
        let stats = leader_to_stats.get(&w2).unwrap();
        assert_eq!(stats.mean().round(), "19.2");
        assert_eq!(stats.cov().round(), "0.4");
        assert_eq!(stats.mdtm().round(), "4.6");

        // quorum latency for w3 is 7
        // w1 -> 15, w2 -> 20, w3 -> 8, w4 -> 14, w6 -> 14
        let stats = leader_to_stats.get(&w3).unwrap();
        assert_eq!(stats.mean().round(), "14.0");
        assert_eq!(stats.cov().round(), "0.3");
        assert_eq!(stats.mdtm().round(), "2.8");
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
        let servers = vec![w1.clone(), w2.clone(), w3.clone(), w4.clone(), w6.clone()];

        // quorum size 2:
        let quorum_size = 2;

        // subset of clients: w1 w2
        let clients = vec![w1.clone(), w2.clone()];
        let leader_to_stats: HashMap<_, _> = bote
            .all_leaders_stats(&servers, &clients, quorum_size)
            .into_iter()
            .collect();

        // quorum latency for w1 is 7
        // w1 -> 8, w2 -> 17
        let stats = leader_to_stats.get(&w1).unwrap();
        assert_eq!(stats.mean().round(), "12.0");
        assert_eq!(stats.cov().round(), "0.6");
        assert_eq!(stats.mdtm().round(), "5.0");

        // quorum latency for w2 is 9
        // w1 -> 19, w2 -> 10
        let stats = leader_to_stats.get(&w2).unwrap();
        assert_eq!(stats.mean().round(), "14.0");
        assert_eq!(stats.cov().round(), "0.5");
        assert_eq!(stats.mdtm().round(), "5.0");

        // quorum latency for w3 is 7
        // w1 -> 15, w2 -> 20
        let stats = leader_to_stats.get(&w3).unwrap();
        assert_eq!(stats.mean().round(), "17.5");
        assert_eq!(stats.cov().round(), "0.2");
        assert_eq!(stats.mdtm().round(), "2.5");

        // subset of clients: w1 w3 w6
        let clients = vec![w1.clone(), w3.clone(), w6.clone()];
        let leader_to_stats: HashMap<_, _> = bote
            .all_leaders_stats(&servers, &clients, quorum_size)
            .into_iter()
            .collect();

        // quorum latency for w1 is 7
        // w1 -> 8, w3 -> 15, w6 -> 21
        let stats = leader_to_stats.get(&w1).unwrap();
        assert_eq!(stats.mean().round(), "14.3");
        assert_eq!(stats.cov().round(), "0.5");
        assert_eq!(stats.mdtm().round(), "4.9");

        // quorum latency for w2 is 9
        // w1 -> 19, w3 -> 22, w6 -> 28
        let stats = leader_to_stats.get(&w2).unwrap();
        assert_eq!(stats.mean().round(), "23.0");
        assert_eq!(stats.cov().round(), "0.2");
        assert_eq!(stats.mdtm().round(), "3.3");

        // quorum latency for w3 is 7
        // w1 -> 15, w3 -> 8, w6 -> 14
        let stats = leader_to_stats.get(&w3).unwrap();
        assert_eq!(stats.mean().round(), "12.0");
        assert_eq!(stats.cov().round(), "0.4");
        assert_eq!(stats.mdtm().round(), "3.3");
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
        let regions = vec![w1.clone(), w2.clone(), w3.clone(), w4.clone(), w6.clone()];

        // quorum size 2:
        let quorum_size = 2;
        let (_, stats) = bote.best_leader(&regions, &regions, quorum_size, StatsKind::Mean);

        assert_eq!(stats.mean().round(), "14.0");
        assert_eq!(stats.cov().round(), "0.3");
        assert_eq!(stats.mdtm().round(), "2.8");
    }
}
