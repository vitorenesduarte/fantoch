use crate::bote::protocol::Protocol::{Atlas, EPaxos, FPaxos};
use crate::bote::protocol::{ClientPlacement, ProtocolStats};
use crate::bote::Bote;
use crate::elapsed;
use crate::metrics::{StatsKind, F64};
use crate::planet::{Planet, Region};
use permutator::Combination;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::iter::FromIterator;

macro_rules! timed {
    ( $s:expr, $x:expr ) => {{
        let (time, result) = elapsed!($x);
        println!("{}: {:?}", $s, time);
        result
    }};
}

// config and stats
type ConfigAndStats = (BTreeSet<Region>, ProtocolStats);

// configs: mapping from `n` to list of configurations of such size
type Configs = HashMap<usize, Vec<ConfigAndStats>>;

// all configs: mapping from clients to `Config`
type AllConfigs = Vec<(Vec<Region>, Configs)>;

// ranked: mapping from `n` to list of configurations of such size
// - these configurations are already a subset of all configurations that passed some filter and
//   have a score associated with it
type Ranked<'a> = HashMap<usize, Vec<(F64, &'a ConfigAndStats)>>;

// all ranked: mapping from clients to `Ranked`
type AllRanked<'a> = Vec<(&'a Vec<Region>, Ranked<'a>)>;

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct Search {
    all_configs: AllConfigs,
}

impl Search {
    pub fn new(
        min_n: usize,
        max_n: usize,
        search_input: SearchInput,
        save_search: bool,
        lat_dir: &str,
    ) -> Self {
        // get filename
        let filename = Self::filename(min_n, max_n, &search_input);

        timed!("get saved search", Self::get_saved_search(&filename)).unwrap_or_else(|| {
            // create planet
            let planet = Planet::new(lat_dir);

            // get regions for servers and clients
            let (servers, clients) = search_input.get_inputs(max_n, &planet);

            // create bote
            let bote = Bote::from(planet);

            // create empty config and get all configs
            let all_configs = timed!(
                "compute all configs",
                Self::compute_all_configs(min_n, max_n, servers, clients, bote)
            );

            // create a new `Search` instance
            let search = Self { all_configs };

            // save it if `save_search`
            if save_search {
                timed!("save search", Self::save_search(&filename, &search));
            }

            // and return it
            search
        })
    }

    pub fn sorted_evolving_configs(
        &self,
        p: &RankingParams,
    ) -> Vec<(F64, Vec<&ConfigAndStats>, &Vec<Region>)> {
        assert_eq!(p.min_n, 3);
        assert_eq!(p.max_n, 13);

        // first we should rank all configs
        let all_ranked = timed!("rank all", self.rank_all(p));

        // show how many ranked configs we have
        let count: usize = all_ranked
            .iter()
            .map(|(_, configs)| configs.iter().map(|(_, css)| css.len()).sum::<usize>())
            .sum();
        println!("config count: {}", count);

        // create result variable
        let mut configs = BTreeSet::new();

        // TODO Transform what's below in an iterator.
        // With access to `p.min_n` and `p.max_n` it should be possible.

        let mut i = 0;
        let count = all_ranked.len();
        all_ranked.into_iter().for_each(|(clients, ranked)| {
            i += 1;
            if i % 100 == 0 {
                println!("{} of {}", i, count);
            }

            Self::configs(&ranked, 3).for_each(|(score3, cs3)| {
                Self::super_configs(&ranked, 5, cs3, p).for_each(|(score5, cs5)| {
                    Self::super_configs(&ranked, 7, cs5, p).for_each(|(score7, cs7)| {
                        Self::super_configs(&ranked, 9, cs7, p).for_each(|(score9, cs9)| {
                            Self::super_configs(&ranked, 11, cs9, p).for_each(|(score11, cs11)| {
                                Self::super_configs(&ranked, 13, cs11, p).for_each(
                                    |(score13, cs13)| {
                                        let score =
                                            score3 + score5 + score7 + score9 + score11 + score13;
                                        let css = vec![cs3, cs5, cs7, cs9, cs11, cs13];
                                        let config = (score, css, clients);
                                        configs.insert(config);
                                    },
                                );
                            });
                        });
                    });
                });
            });
        });

        // `configs` is sorted ASC
        configs
            .into_iter()
            // sort DESC (highest score first)
            .rev()
            .collect()
    }

    pub fn stats_fmt(stats: &ProtocolStats, n: usize) -> String {
        ClientPlacement::all()
            .map(|placement| {
                // shows stats for all possible f
                let fmt: String = (1..=Self::max_f(n))
                    .map(|f| {
                        let atlas = stats.fmt(Atlas, f, placement);
                        let fpaxos = stats.fmt(FPaxos, f, placement);
                        format!("{} {} ", atlas, fpaxos)
                    })
                    .collect();

                // add epaxos
                let epaxos = stats.fmt(EPaxos, 0, placement);
                format!("{}{} ", fmt, epaxos)
            })
            .collect()
    }

    fn compute_all_configs(
        min_n: usize,
        max_n: usize,
        servers: Option<Vec<Region>>,
        all_clients: Vec<Vec<Region>>,
        bote: Bote,
    ) -> AllConfigs {
        // get the count of client configurations
        let clients_count = all_clients.len();

        all_clients
            // PARALLEL
            .into_par_iter()
            .enumerate()
            .inspect(|(i, _)| {
                // show progress
                if i % 100 == 0 {
                    println!("{} of {}", i, clients_count);
                }
            })
            .map(|(_, clients)| {
                // compute servers: if we have something, use what we got,
                // otherwise use the set of clients
                let servers = servers.as_ref().unwrap_or(&clients);

                // compute `Configs` for this set of clients
                let configs = Self::compute_configs(min_n, max_n, &servers, &clients, &bote);

                (clients, configs)
            })
            .collect()
    }

    fn compute_configs(
        min_n: usize,
        max_n: usize,
        regions: &[Region],
        clients: &[Region],
        bote: &Bote,
    ) -> Configs {
        (min_n..=max_n)
            .step_by(2)
            .map(|n| {
                let configs = regions
                    .combination(n)
                    .map(vec_cloned)
                    .map(|config| {
                        // compute stats
                        let stats = Self::compute_stats(&config, clients, bote);

                        // turn config into a `BTreeSet`
                        let config = BTreeSet::from_iter(config.into_iter());

                        (config, stats)
                    })
                    .collect();
                (n, configs)
            })
            .collect()
    }

    pub fn compute_stats(config: &[Region], all_clients: &[Region], bote: &Bote) -> ProtocolStats {
        // compute n
        let n = config.len();
        let mut stats = ProtocolStats::new();

        // compute best cov fpaxos f=1 leader
        // - this leader will then be used for both f=1 and f=2 stats
        let f = 1;
        let quorum_size = FPaxos.quorum_size(n, f);
        let (leader, _) = bote.best_leader(config, all_clients, quorum_size, StatsKind::COV);

        // compute stats for both `clients` and colocated clients i.e. `config`
        let which_clients = vec![
            (ClientPlacement::Input, all_clients),
            (ClientPlacement::Colocated, config),
        ];
        for (placement, clients) in which_clients {
            for f in 1..=Self::max_f(n) {
                // compute altas quorum size
                let quorum_size = Atlas.quorum_size(n, f);

                // compute atlas stats
                let atlas = bote.leaderless(config, clients, quorum_size);
                stats.insert(Atlas, f, placement, atlas);

                // compute fpaxos quorum size
                let quorum_size = FPaxos.quorum_size(n, f);

                // // compute best mean fpaxos stats
                let fpaxos = bote.leader(leader, config, clients, quorum_size);
                stats.insert(FPaxos, f, placement, fpaxos);
            }

            // compute epaxos quorum size
            let quorum_size = EPaxos.quorum_size(n, 0);

            // compute epaxos stats
            let epaxos = bote.leaderless(config, clients, quorum_size);
            stats.insert(EPaxos, 0, placement, epaxos);
        }

        // return all stats
        stats
    }

    fn rank_all<'a>(&'a self, params: &RankingParams) -> AllRanked<'a> {
        self.all_configs
            // PARALLEL
            .par_iter()
            .map(|(clients, configs)| (clients, Self::rank(configs, params)))
            .collect()
    }

    fn rank<'a>(configs: &'a Configs, params: &RankingParams) -> Ranked<'a> {
        configs
            .iter()
            .filter_map(|(&n, css)| {
                // only keep in the map `n` values between `min_n` and `max_n`
                if n >= params.min_n && n <= params.max_n {
                    let css = css
                        .iter()
                        .filter_map(|cs| {
                            // get stats
                            let stats = &cs.1;

                            // only keep valid configurations
                            match Self::compute_score(n, stats, params) {
                                (true, score) => Some((score, cs)),
                                _ => None,
                            }
                        })
                        .collect();
                    Some((n, css))
                } else {
                    None
                }
            })
            .collect()
    }

    // TODO `configs` and `super_configs` are super similar
    #[allow(clippy::iter_cloned_collect)]
    fn configs<'a>(
        ranked: &Ranked<'a>,
        n: usize,
    ) -> impl Iterator<Item = (F64, &'a ConfigAndStats)> {
        ranked
            .get(&n)
            .unwrap_or_else(|| panic!("configs for n = {} should be ranked!", n))
            .iter()
            .cloned()
            // TODO can we avoid collecting here?
            // I wasn't able to do it due to lifetime issues
            .collect::<Vec<_>>()
            .into_iter()
    }

    /// return ranked configurations such that:
    /// - their size is `n`
    /// - are a superset of `previous_config`
    fn super_configs<'a>(
        ranked: &Ranked<'a>,
        n: usize,
        (prev_config, prev_stats): &ConfigAndStats,
        params: &RankingParams,
    ) -> impl Iterator<Item = (F64, &'a ConfigAndStats)> {
        ranked
            .get(&n)
            .unwrap_or_else(|| panic!("super configs for n = {} should be ranked!", n))
            .iter()
            .filter(|(_, (config, stats))| {
                config.is_superset(prev_config)
                    && Self::min_mean_decrease(stats, prev_stats, n, params)
            })
            .cloned()
            // TODO can we avoid collecting here?
            // I wasn't able to do it due to lifetime issues
            .collect::<Vec<_>>()
            .into_iter()
    }

    /// Compute the mean latency decrease for Atlas f = 1 when the number of
    /// sites increases.
    fn min_mean_decrease(
        stats: &ProtocolStats,
        prev_stats: &ProtocolStats,
        n: usize,
        params: &RankingParams,
    ) -> bool {
        // compare only for the number of faults tolerated by `prev_stats`
        let n = n - 2;

        let placement = ClientPlacement::Input;

        params.ft_metric.fs(n).into_iter().all(|f| {
            let atlas = stats.get(Atlas, f, placement);
            let prev_atlas = prev_stats.get(Atlas, f, placement);
            prev_atlas.mean_improv(atlas) >= params.min_mean_decrease
        })
    }

    fn compute_score(n: usize, stats: &ProtocolStats, params: &RankingParams) -> (bool, F64) {
        // compute score and check if it is a valid configuration
        let mut valid = true;
        let mut score = F64::zero();

        // f values accounted for when computing score and config validity
        let fs = params.ft_metric.fs(n);

        // placement is input
        let placement = ClientPlacement::Input;

        for f in fs {
            // get atlas and fpaxos stats
            let atlas = stats.get(Atlas, f, placement);
            let fpaxos = stats.get(FPaxos, f, placement);

            // compute mean latency improvement of atlas wrto to fpaxos
            let fpaxos_mean_improv = fpaxos.mean_improv(atlas);

            // compute fairness improvement of atlas wrto to cov fpaxos
            let fpaxos_fairness_improv = fpaxos.cov_improv(atlas);

            // check if it's a valid config, i.e. there's enough:
            // - `min_mean_improv`
            // - `min_fairness_improv`
            valid = valid
                && fpaxos_mean_improv >= params.min_mean_fpaxos_improv
                && fpaxos_fairness_improv >= params.min_fairness_fpaxos_improv;

            // get epaxos stats
            let epaxos = stats.get(EPaxos, 0, placement);

            // compute mean latency improvement of atlas wrto to epaxos
            let epaxos_mean_improv = epaxos.mean_improv(atlas);

            // make sure we improve on EPaxos for large n
            if n == 11 || n == 13 {
                valid = valid && epaxos_mean_improv >= params.min_mean_epaxos_improv;
            }

            // update score: give extra weigth for epaxos improv
            let weight = F64::new(30_f64);
            score += fpaxos_mean_improv + (weight * epaxos_mean_improv);
        }

        (valid, score)
    }

    fn max_f(n: usize) -> usize {
        let max_f = 2;
        std::cmp::min(n / 2 as usize, max_f)
    }

    fn filename(min_n: usize, max_n: usize, search_input: &SearchInput) -> String {
        format!("{}_{}_{}.data", min_n, max_n, search_input)
    }

    fn get_saved_search(name: &str) -> Option<Search> {
        // open the file in read-only
        File::open(name)
            .ok()
            // create a buf reader
            .map(BufReader::new)
            // and try to deserialize
            .map(|reader| bincode::deserialize_from(reader).expect("error deserializing search"))
    }

    fn save_search(name: &str, search: &Search) {
        // if the file does not exist it will be created, otherwise truncated
        File::create(name)
            .ok()
            // create a buf writer
            .map(BufWriter::new)
            // and try to serialize
            .map(|writer| {
                bincode::serialize_into(writer, search).expect("error serializing search")
            })
            .unwrap_or_else(|| panic!("couldn't save seach"));
    }
}

/// identifies which regions considered for the search
pub enum SearchInput {
    /// search within selected 13 regions, clients deployed in the 13 regions
    R13C13,
    /// search within 2018 17 regions, clients deployed in the 17 regions
    R17C17,
    /// search within the 20 regions, clients deployed in the 20 regions
    R20C20,
    /// search within 2018 17 regions, clients deployed in the MAX regions
    /// - e.g. if the max number of regions is 11, clients are deployed in those 11 regions
    R17CMaxN,
}

impl fmt::Display for SearchInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SearchInput::R13C13 => write!(f, "R13C13"),
            SearchInput::R17C17 => write!(f, "R17C17"),
            SearchInput::R20C20 => write!(f, "R20C20"),
            SearchInput::R17CMaxN => write!(f, "R17CMaxN"),
        }
    }
}

impl SearchInput {
    /// It returns a tuple where the:
    /// - 1st component is the set of regions where to look for a configuration
    /// - 2nd component is a list of client locations
    fn get_inputs(&self, max_n: usize, planet: &Planet) -> (Option<Vec<Region>>, Vec<Vec<Region>>) {
        // selected 13-regions
        let regions13 = vec![
            Region::new("asia-southeast1"),
            Region::new("europe-west4"),
            Region::new("southamerica-east1"),
            Region::new("australia-southeast1"),
            Region::new("europe-west2"),
            Region::new("asia-south1"),
            Region::new("us-east1"),
            Region::new("asia-northeast1"),
            Region::new("europe-west1"),
            Region::new("asia-east1"),
            Region::new("us-west1"),
            Region::new("europe-west3"),
            Region::new("us-central1"),
        ];

        // 17-regions available in the end of 2018
        let regions17 = vec![
            Region::new("asia-east1"),
            Region::new("asia-northeast1"),
            Region::new("asia-south1"),
            Region::new("asia-southeast1"),
            Region::new("australia-southeast1"),
            Region::new("europe-north1"),
            Region::new("europe-west1"),
            Region::new("europe-west2"),
            Region::new("europe-west3"),
            Region::new("europe-west4"),
            Region::new("northamerica-northeast1"),
            Region::new("southamerica-east1"),
            Region::new("us-central1"),
            Region::new("us-east1"),
            Region::new("us-east4"),
            Region::new("us-west1"),
            Region::new("us-west2"),
        ];

        // all regions
        let mut all_regions = planet.regions();
        all_regions.sort();

        match self {
            SearchInput::R13C13 => {
                let clients = vec![regions13.clone()];
                (Some(regions13), clients)
            }
            SearchInput::R17C17 => {
                let clients = vec![regions17.clone()];
                (Some(regions17), clients)
            }
            SearchInput::R20C20 => {
                let clients = vec![all_regions.clone()];
                (Some(all_regions), clients)
            }
            SearchInput::R17CMaxN => {
                let clients = regions17.combination(max_n).map(vec_cloned).collect();
                (None, clients)
            }
        }
    }
}

fn vec_cloned<T: Clone>(vec: Vec<&T>) -> Vec<T> {
    vec.into_iter().cloned().collect()
}

pub struct RankingParams {
    min_mean_fpaxos_improv: F64,
    min_mean_epaxos_improv: F64,
    min_fairness_fpaxos_improv: F64,
    min_mean_decrease: F64,
    min_n: usize,
    max_n: usize,
    ft_metric: FTMetric,
}

impl RankingParams {
    pub fn new(
        min_mean_fpaxos_improv: isize,
        min_mean_epaxos_improv: isize,
        min_fairness_fpaxos_improv: isize,
        min_mean_decrease: isize,
        min_n: usize,
        max_n: usize,
        ft_metric: FTMetric,
    ) -> Self {
        Self {
            min_mean_fpaxos_improv: F64::new(min_mean_fpaxos_improv as f64),
            min_mean_epaxos_improv: F64::new(min_mean_epaxos_improv as f64),
            min_fairness_fpaxos_improv: F64::new(min_fairness_fpaxos_improv as f64),
            min_mean_decrease: F64::new(min_mean_decrease as f64),
            min_n,
            max_n,
            ft_metric,
        }
    }
}

/// metric considered for fault tolerance
pub enum FTMetric {
    F1,
    F1F2,
}

impl FTMetric {
    fn fs(&self, n: usize) -> Vec<usize> {
        let minority = n / 2 as usize;
        let max_f = match self {
            FTMetric::F1 => 1,
            FTMetric::F1F2 => 2,
        };
        (1..=std::cmp::min(minority, max_f)).collect()
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    // directory that contains all dat files
    const LAT_DIR: &str = "latency/";

    #[test]
    fn search() {
        // define some search params
        let min_n = 3;
        let max_n = 13;
        // originally `search_input = SearchInput::R17CMaxN`
        let search_input = SearchInput::R13C13;
        let save_search = false;

        // create search
        let search = Search::new(min_n, max_n, search_input, save_search, LAT_DIR);

        // define search params:
        // originally 30 was used for the `min_mean_improv`;
        // here we want the test to be run asap,
        // so we restrict the search the maximum possible
        let min_mean_fpaxos_improv = 110;
        let min_mean_epaxos_improv = 35;
        let min_fairness_fpaxos_improv = 0;
        let min_mean_decrease = 15;
        let ft_metric = FTMetric::F1F2;

        // create ranking params
        let params = RankingParams::new(
            min_mean_fpaxos_improv,
            min_mean_epaxos_improv,
            min_fairness_fpaxos_improv,
            min_mean_decrease,
            min_n,
            max_n,
            ft_metric,
        );

        // select the best config
        let (score, css, _clients) = search
            .sorted_evolving_configs(&params)
            .into_iter()
            .take(1) // take only the best one
            .next()
            .unwrap();

        // the final sorted config
        let mut sorted_config = Vec::new();
        for (config, stats) in css {
            // update sorted config
            for region in config {
                if !sorted_config.contains(region) {
                    sorted_config.push(region.clone())
                }
            }

            // check stats_fmt for n = 5
            let n = 5;
            if config.len() == n {
                let expected = "af1=(271, 0.20) ff1=(419, 0.24) af2=(314, 0.13) ff2=(428, 0.23) e=(271, 0.20) af1C=(234, 0.15) ff1C=(410, 0.31) af2C=(280, 0.05) ff2C=(419, 0.31) eC=(234, 0.15) ";
                assert_eq!(Search::stats_fmt(stats, n), expected);
            }
        }

        // check score
        let expected_score = "10360.3";
        assert_eq!(score.round(), expected_score);

        // check config
        let expected_config = vec![
            Region::new("asia-southeast1"),
            Region::new("europe-west4"),
            Region::new("southamerica-east1"),
            Region::new("australia-southeast1"),
            Region::new("europe-west2"),
            Region::new("asia-south1"),
            Region::new("us-east1"),
            Region::new("asia-northeast1"),
            Region::new("europe-west1"),
            Region::new("asia-east1"),
            Region::new("us-west1"),
            Region::new("europe-west3"),
            Region::new("us-central1"),
        ];
        assert_eq!(sorted_config, expected_config);
    }

    #[test]
    fn search_save() {
        let min_n = 3;
        let max_n = 5;
        let search_input = SearchInput::R17C17;
        let filename = Search::filename(min_n, max_n, &search_input);

        // create search and save it
        let save_search = true;
        let expected = Search::new(min_n, max_n, search_input, save_search, LAT_DIR);

        // get saved search and assert it is the same search
        let saved = Search::get_saved_search(&filename);
        assert!(saved.is_some());
        assert_eq!(saved.unwrap(), expected);

        // remove search file
        assert!(std::fs::remove_file(filename).is_ok());
    }
}
