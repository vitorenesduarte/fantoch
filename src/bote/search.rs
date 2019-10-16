use crate::bote::float::F64;
use crate::bote::protocol::Protocol;
use crate::bote::stats::{AllStats, Stats};
use crate::bote::Bote;
use crate::planet::{Planet, Region};
use permutator::Combination;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::iter::FromIterator;
use std::time::Instant;

macro_rules! timed {
    ( $s:expr, $x:expr ) => {{
        let start = Instant::now();
        let result = $x;
        let time = start.elapsed();
        println!("{}: {:?}", $s, time);
        result
    }};
}

// config and stats
type ConfigAndStats = (BTreeSet<Region>, AllStats);

// configs: mapping from `n` to list of configurations of such size
type Configs = HashMap<usize, Vec<ConfigAndStats>>;

// all configs: mapping from clients to `Config`
type AllConfigs = Vec<(Vec<Region>, Configs)>;

// ranked: mapping from `n` to list of configurations of such size
// - these configurations are already a subset of all configurations that passed
//   some filter and have a score associated with it
type Ranked<'a> = HashMap<usize, Vec<(F64, &'a ConfigAndStats)>>;

// all ranked: mapping from clients to `Ranked`
type AllRanked<'a> = Vec<(&'a Vec<Region>, Ranked<'a>)>;

#[derive(Deserialize, Serialize)]
pub struct Search {
    all_configs: AllConfigs,
}

impl Search {
    pub fn new(
        min_n: usize,
        max_n: usize,
        search_input: SearchInput,
        lat_dir: &str,
    ) -> Self {
        // get filename
        let filename = Self::filename(min_n, max_n, &search_input);

        timed!("get saved search", Search::get_saved_search(&filename))
            .unwrap_or_else(|| {
                // create planet
                let planet = Planet::new(lat_dir);

                // get regions for servers and clients
                let (servers, clients) =
                    search_input.get_inputs(max_n, &planet);

                // create bote
                let bote = Bote::from(planet);

                // create empty config and get all configs
                let all_configs = timed!(
                    "compute all configs",
                    Self::compute_all_configs(
                        min_n, max_n, servers, clients, bote
                    )
                );

                // create a new `Search` instance
                let search = Search { all_configs };

                // save it
                timed!("save search", Self::save_search(&filename, &search));

                // and return it
                search
            })
    }

    // pub fn sorted_configs(
    //     &self,
    //     params: &RankingParams,
    //     max_configs_per_n: usize,
    // ) -> BTreeMap<usize, Vec<(F64, &ConfigAndStats)>> {
    //     self.rank(params)
    //         .into_iter()
    //         .map(|(n, ranked)| {
    //             let sorted = ranked
    //                 .into_iter()
    //                 // sort ASC
    //                 .collect::<BTreeSet<_>>()
    //                 .into_iter()
    //                 // sort DESC (highest score first)
    //                 .rev()
    //                 // take the first `max_configs_per_n`
    //                 .take(max_configs_per_n)
    //                 .collect();
    //             (n, sorted)
    //         })
    //         .collect()
    // }

    pub fn sorted_evolving_configs(
        &self,
        p: &RankingParams,
    ) -> Vec<(F64, Vec<&ConfigAndStats>, &Vec<Region>)> {
        assert_eq!(p.min_n, 3);
        assert_eq!(p.max_n, 11);

        // first we should rank all configs
        let all_ranked = self.rank_all(p);

        // show how many ranked configs we have for each set of clients
        let count = all_ranked
            .iter()
            .map(|(_, configs)| {
                configs.iter().map(|(_, css)| css.len()).count()
            })
            .count();
        println!("config count: {}", count);

        // create result variable
        let mut configs = BTreeSet::new();

        // TODO Transform what's below in an iterator.
        // With access to `p.min_n` and `p.max_n` it should be possible.

        // PARALLEL
        let mut i = 0;
        let count = all_ranked.len();
        all_ranked.into_iter().for_each(|(clients, ranked)| {
            i += 1;
            if i % 10 == 0 {
                println!("{} of {}", i, count);
            }

            Self::configs(&ranked, 3).for_each(|(score3, cs3)| {
                Self::super_configs(&ranked, p, 5, cs3).for_each(
                    |(score5, cs5)| {
                        Self::super_configs(&ranked, p, 7, cs5).for_each(
                            |(score7, cs7)| {
                                Self::super_configs(&ranked, p, 9, cs7)
                                    .for_each(|(score9, cs9)| {
                                        Self::super_configs(
                                            &ranked, p, 11, cs9,
                                        )
                                        .for_each(|(score11, cs11)| {
                                            let score = score3
                                                + score5
                                                + score7
                                                + score9
                                                + score11;
                                            let css =
                                                vec![cs3, cs5, cs7, cs9, cs11];
                                            assert!(configs
                                                .insert((score, css, clients)))
                                        });
                                    });
                            },
                        );
                    },
                );
            });
        });

        // `configs` is sorted ASC
        configs
            .into_iter()
            // sort DESC (highest score first)
            .rev()
            .collect()
    }

    pub fn stats_fmt(
        stats: &AllStats,
        n: usize,
        params: &RankingParams,
    ) -> String {
        // shows stats for all possible f
        let fmt: String = (1..=Self::max_f(n))
            .map(|f| {
                let atlas = stats.get("atlas", f);
                let fpaxos = Self::get_fpaxos_stats(stats, f, params);
                format!("a{}={:?} f{}={:?} ", f, atlas, f, fpaxos)
            })
            .collect();

        // add epaxos
        let epaxos = stats.get("epaxos", 0);
        format!("{}e={:?}", fmt, epaxos)
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
                if i % 10 == 0 {
                    println!("{} of {}", i, clients_count);
                }
            })
            .map(|(_, clients)| {
                // compute servers: if we have something, use what we got,
                // otherwise use the set of clients
                let servers = servers.as_ref().unwrap_or(&clients);

                // compute `Configs` for this set of clients
                let configs = Self::compute_configs(
                    min_n, max_n, &servers, &clients, &bote,
                );

                (clients, configs)
            })
            .collect()
    }

    fn compute_configs(
        min_n: usize,
        max_n: usize,
        regions: &Vec<Region>,
        clients: &Vec<Region>,
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

    fn compute_stats(
        config: &Vec<Region>,
        clients: &Vec<Region>,
        bote: &Bote,
    ) -> AllStats {
        // compute n
        let n = config.len();
        let mut stats = AllStats::new();

        for f in 1..=Self::max_f(n) {
            // compute altas quorum size
            let quorum_size = Protocol::Atlas.quorum_size(n, f);

            // compute atlas stats
            let atlas = bote.leaderless(config, clients, quorum_size);
            stats.insert("atlas", f, atlas);

            // compute fpaxos quorum size
            let quorum_size = Protocol::FPaxos.quorum_size(n, f);

            // // compute best mean fpaxos stats
            // let fpaxos = bote.best_mean_leader(config, clients, quorum_size);
            // stats.insert("fpaxos_mean", f, fpaxos);

            // compute best cov fpaxos stats
            let fpaxos = bote.best_cov_leader(config, clients, quorum_size);
            stats.insert("fpaxos_cov", f, fpaxos);

            // compute best mdtm fpaxos stats
            let fpaxos = bote.best_mdtm_leader(config, clients, quorum_size);
            stats.insert("fpaxos_mdtm", f, fpaxos);
        }

        // compute epaxos quorum size
        let quorum_size = Protocol::EPaxos.quorum_size(n, 0);

        // compute epaxos stats
        let epaxos = bote.leaderless(config, clients, quorum_size);
        stats.insert("epaxos", 0, epaxos);

        // return all stats
        stats
    }

    fn rank_all<'a>(&'a self, params: &RankingParams) -> AllRanked<'a> {
        self.all_configs
            .iter()
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
                        .into_iter()
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
    fn configs<'a>(
        ranked: &Ranked<'a>,
        n: usize,
    ) -> impl Iterator<Item = (F64, &'a ConfigAndStats)> {
        ranked
            .get(&n)
            .unwrap_or_else(|| {
                panic!("configs for n = {} should be ranked!", n)
            })
            .into_iter()
            .map(|&r| r)
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
        params: &RankingParams,
        n: usize,
        (prev_config, prev_stats): &ConfigAndStats,
    ) -> impl Iterator<Item = (F64, &'a ConfigAndStats)> {
        ranked
            .get(&n)
            .unwrap_or_else(|| {
                panic!("super configs for n = {} should be ranked!", n)
            })
            .into_iter()
            .filter(|(_, (config, stats))| {
                config.is_superset(prev_config)
                    && Self::mean_decrease(prev_stats, stats)
                        >= params.min_mean_decrease
            })
            // TODO Is `.cloned()` equivalent to `.map(|&r| r)` here?
            .map(|&r| r)
            // TODO can we avoid collecting here?
            // I wasn't able to do it due to lifetime issues
            .collect::<Vec<_>>()
            .into_iter()
    }

    /// Compute the mean latency decrease for Atlas f = 1 when the number of
    /// sites increases.
    fn mean_decrease(prev_stats: &AllStats, stats: &AllStats) -> F64 {
        let f = 1;
        let prev_atlas = prev_stats.get("atlas", f);
        let atlas = stats.get("atlas", f);
        prev_atlas.mean_improv(atlas)
    }

    fn compute_score(
        n: usize,
        stats: &AllStats,
        params: &RankingParams,
    ) -> (bool, F64) {
        // compute score and check if it is a valid configuration
        let mut valid = true;
        let mut score = F64::zero();

        // f values accounted for when computing score and config validity
        let fs = params.ft_metric.fs(n);

        for f in fs {
            // get atlas and fpaxos stats
            let atlas = stats.get("atlas", f);
            let fpaxos = Self::get_fpaxos_stats(stats, f, params);

            // compute mean latency improvement of atlas wrto to fpaxos
            let fpaxos_mean_improv = fpaxos.mean_improv(atlas);

            // compute fairness improvement of atlas wrto to fpaxos
            let fpaxos_fairness_improv = match params.fairness_metric {
                FairnessMetric::COV => fpaxos.cov_improv(atlas),
                FairnessMetric::MDTM => fpaxos.mdtm_improv(atlas),
            };

            // check if it's a valid config, i.e. there's enough:
            // - `min_mean_improv`
            // - `min_fairness_improv`
            valid = valid
                && fpaxos_mean_improv >= params.min_mean_improv
                && fpaxos_fairness_improv >= params.min_fairness_improv;

            // let epaxos = stats.get("epaxos", 0);
            //
            // let epaxos_mean_improv = if f == 2 {
            //     epaxos.mean_improv(atlas)
            // } else {
            //     F64::zero()
            // };
            //
            // // update score
            // score += fpaxos_mean_improv + epaxos_mean_improv;

            // update score
            score += fpaxos_mean_improv;
        }

        (valid, score)
    }

    fn get_fpaxos_stats<'a>(
        stats: &'a AllStats,
        f: usize,
        params: &RankingParams,
    ) -> &'a Stats {
        match params.fairness_metric {
            FairnessMetric::COV => stats.get("fpaxos_cov", f),
            FairnessMetric::MDTM => stats.get("fpaxos_mdtm", f),
        }
    }

    fn max_f(n: usize) -> usize {
        let max_f = 2;
        std::cmp::min(n / 2 as usize, max_f)
    }

    fn filename(
        min_n: usize,
        max_n: usize,
        search_input: &SearchInput,
    ) -> String {
        format!("{}_{}_{}.data", min_n, max_n, search_input)
    }

    fn get_saved_search(name: &String) -> Option<Search> {
        // open the file in read-only
        File::open(name)
            .ok()
            // create a buf reader
            .map(|file| BufReader::new(file))
            // and try to deserialize
            .map(|reader| {
                serde_json::from_reader(reader)
                    .expect("error deserializing search")
            })
    }

    fn save_search(name: &String, search: &Search) {
        // if the file does not exist it will be created, otherwise truncated
        File::create(name)
            .ok()
            // create a buf writer
            .map(|file| BufWriter::new(file))
            // and try to serialize
            .map(|writer| {
                serde_json::to_writer(writer, search)
                    .expect("error serializing search")
            });
    }
}

/// identifies which regions considered for the search
#[allow(dead_code)]
pub enum SearchInput {
    /// search within 2018 17 regions, clients deployed in the MAX regions
    /// - e.g. if the max number of regions is 11, clients are deployed in
    ///   those 11 regions
    R17CMaxN,
    /// search within 2018 17 regions, clients deployed in the 17 regions
    R17C17,
    /// search within the 20 regions, clients deployed in the 20 regions
    R20C20,
}

impl fmt::Display for SearchInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SearchInput::R17CMaxN => write!(f, "R17CMaxN"),
            SearchInput::R17C17 => write!(f, "R17C17"),
            SearchInput::R20C20 => write!(f, "R20C20"),
        }
    }
}

impl SearchInput {
    /// It returns a tuple where the:
    /// - 1st component is the set of regions where to look for a configuration
    /// - 2nd component is a list of client locations
    fn get_inputs(
        &self,
        max_n: usize,
        planet: &Planet,
    ) -> (Option<Vec<Region>>, Vec<Vec<Region>>) {
        // compute 17-regions (from end of 2018)
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

        // compute all regions
        let mut regions = planet.regions();
        regions.sort();

        match self {
            SearchInput::R17CMaxN => {
                let all_clients =
                    regions17.combination(max_n).map(vec_cloned).collect();
                (None, all_clients)
            }
            SearchInput::R17C17 => {
                let all_clients = vec![regions17.clone()];
                (Some(regions17), all_clients)
            }
            SearchInput::R20C20 => {
                let all_clients = vec![regions.clone()];
                (Some(regions), all_clients)
            }
        }
    }
}

fn vec_cloned<T: Clone>(vec: Vec<&T>) -> Vec<T> {
    vec.into_iter().cloned().collect()
}

pub struct RankingParams {
    min_mean_improv: F64,
    min_fairness_improv: F64,
    min_mean_decrease: F64,
    min_n: usize,
    max_n: usize,
    fairness_metric: FairnessMetric,
    ft_metric: FTMetric,
}

impl RankingParams {
    pub fn new(
        min_mean_improv: isize,
        min_fairness_improv: isize,
        min_mean_decrease: isize,
        min_n: usize,
        max_n: usize,
        fairness_metric: FairnessMetric,
        ft_metric: FTMetric,
    ) -> Self {
        RankingParams {
            min_mean_improv: F64::new(min_mean_improv as f64),
            min_fairness_improv: F64::new(min_fairness_improv as f64),
            min_mean_decrease: F64::new(min_mean_decrease as f64),
            min_n,
            max_n,
            fairness_metric,
            ft_metric,
        }
    }
}

/// metric considered for selecting FPaxos leader
#[allow(dead_code)]
pub enum FairnessMetric {
    COV,  // coefficient of variation (stddev / mean)
    MDTM, // mean distance to mean
}

/// metric considered for fault tolerance
#[allow(dead_code)]
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
