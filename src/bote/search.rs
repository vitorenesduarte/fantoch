use crate::bote::protocol::Protocol;
use crate::bote::stats::Stats;
use crate::bote::Bote;
use crate::planet::{Planet, Region};
use permutator::Combination;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::iter::FromIterator;

// mapping from protocol name to its stats
type AllStats = BTreeMap<String, Stats>;

// config and stats
type ConfigAndStats = (BTreeSet<Region>, AllStats);

// all configs
type AllConfigs = HashMap<usize, Vec<ConfigAndStats>>;

// ranked
type Ranked<'a> = HashMap<usize, Vec<(isize, &'a ConfigAndStats)>>;

#[derive(Deserialize, Serialize)]
pub struct Search {
    regions: Vec<Region>,
    clients: Option<Vec<Region>>,
    all_configs: AllConfigs,
}

impl Search {
    pub fn new(
        min_n: usize,
        max_n: usize,
        search_input: SearchInput,
        lat_dir: &str,
    ) -> Self {
        let filename = Self::filename(min_n, max_n, &search_input);

        Search::get_saved_search(&filename).unwrap_or_else(|| {
            // create planet
            let planet = Planet::new(lat_dir);

            // get all regions
            let mut all_regions = planet.regions();
            all_regions.sort();

            // create bote
            let bote = Bote::from(planet.clone());

            // get regions for servers and clients
            let (regions, clients) =
                Self::search_inputs(&search_input, &planet);

            // create empty config and get all configs
            let all_configs = Self::compute_all_configs(
                min_n, max_n, &regions, &clients, &bote,
            );

            // create a new `Search` instance
            let search = Search {
                regions,
                clients,
                all_configs,
            };

            // save it
            Self::save_search(&filename, &search);

            // and return it
            search
        })
    }

    pub fn sorted_configs(
        &self,
        params: &RankingParams,
        max_configs_per_n: usize,
    ) -> BTreeMap<usize, Vec<(isize, &ConfigAndStats)>> {
        self.rank(params)
            .into_iter()
            .map(|(n, ranked)| {
                let sorted = ranked
                    .into_iter()
                    // sort ASC
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    // sort DESC (highest score first)
                    .rev()
                    // take the first `max_configs_per_n`
                    .take(max_configs_per_n)
                    .collect();
                (n, sorted)
            })
            .collect()
    }

    pub fn sorted_evolved_configs(
        &self,
        params: &RankingParams,
        max_configs: usize,
    ) -> Vec<(isize, Vec<&ConfigAndStats>)> {
        assert_eq!(params.min_n, 3);
        assert_eq!(params.max_n, 13);

        // first we should rank all configs
        let ranked = self.rank(params);

        // create result variable
        let mut configs = BTreeSet::new();

        // TODO Transform what's below in an iterator.
        // With access to `p.min_n` and `p.max_n` it should be possible.

        let ranked3 = ranked.get(&3).unwrap();
        let count = ranked3.len();
        let mut i = 0;

        Self::configs(&ranked, 3).for_each(|(score3, cs3)| {
            i += 1;
            println!("{} of {}", i, count);

            Self::super_configs(&ranked, 5, cs3).for_each(|(score5, cs5)| {
                Self::super_configs(&ranked, 7, cs5).for_each(
                    |(score7, cs7)| {
                        Self::super_configs(&ranked, 9, cs7).for_each(
                            |(score9, cs9)| {
                                Self::super_configs(&ranked, 11, cs9).for_each(
                                    |(score11, cs11)| {
                                        Self::super_configs(&ranked, 13, cs11)
                                            .for_each(|(score13, cs13)| {
                                                let score = score3
                                                    + score5
                                                    + score7
                                                    + score9
                                                    + score11
                                                    + score13;
                                                let css = vec![
                                                    cs3, cs5, cs7, cs9, cs11,
                                                    cs13,
                                                ];
                                                assert!(configs
                                                    .insert((score, css)))
                                            });
                                    },
                                );
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
            // take the first `max_configs_per_n`
            .take(max_configs)
            .collect()
    }

    pub fn stats_fmt(stats: &AllStats, n: usize) -> String {
        // create stats for all possible f
        let fmt: String = (1..=Self::max_f(n))
            .map(|f| {
                let atlas = stats.get(&Self::protocol_key("atlas", f)).unwrap();
                let lfpaxos =
                    stats.get(&Self::protocol_key("lfpaxos", f)).unwrap();
                let ffpaxos =
                    stats.get(&Self::protocol_key("ffpaxos", f)).unwrap();
                format!(
                    "a{}={:?} lf{}={:?} ff{}={:?} ",
                    f, atlas, f, lfpaxos, f, ffpaxos
                )
            })
            .collect();

        // add epaxos stats
        let epaxos = stats.get(&Self::epaxos_protocol_key()).unwrap();
        format!("{}e={:?}", fmt, epaxos)
    }

    fn compute_all_configs(
        min_n: usize,
        max_n: usize,
        regions: &Vec<Region>,
        clients: &Option<Vec<Region>>,
        bote: &Bote,
    ) -> AllConfigs {
        (min_n..=max_n)
            .step_by(2)
            .map(|n| {
                let configs = regions
                    .combination(n)
                    .map(|config| {
                        // clone config
                        let config: Vec<_> =
                            config.into_iter().cloned().collect();

                        // compute clients
                        let clients = clients.as_ref().unwrap_or(&config);

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
        let mut stats = BTreeMap::new();

        for f in 1..=Self::max_f(n) {
            // compute atlas stats
            let atlas = bote.leaderless(
                config,
                clients,
                Protocol::Atlas.quorum_size(n, f),
            );
            stats.insert(Self::protocol_key("atlas", f), atlas);

            // compute best latency fpaxos stats
            let lfpaxos = bote.best_latency_leader(
                config,
                clients,
                Protocol::FPaxos.quorum_size(n, f),
            );
            stats.insert(Self::protocol_key("lfpaxos", f), lfpaxos);

            // compute best fairness fpaxos stats
            let ffpaxos = bote.best_fairness_leader(
                config,
                clients,
                Protocol::FPaxos.quorum_size(n, f),
            );
            stats.insert(Self::protocol_key("ffpaxos", f), ffpaxos);
        }

        // compute epaxos stats
        let epaxos = bote.leaderless(
            config,
            clients,
            Protocol::EPaxos.quorum_size(n, 0),
        );
        stats.insert(Self::epaxos_protocol_key(), epaxos);

        // return all stats
        stats
    }

    fn rank<'a>(&'a self, params: &RankingParams) -> Ranked<'a> {
        self.all_configs
            .iter()
            .filter_map(|(&n, css)| {
                // only keep in the map `n` values between `min_n` and `max_n`
                if n >= params.min_n && n <= params.max_n {
                    let css = css
                        .into_iter()
                        .filter_map(|cs| {
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
    ) -> impl Iterator<Item = (isize, &'a ConfigAndStats)> {
        ranked
            .get(&n)
            .unwrap()
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
        n: usize,
        (prev_config, _prev_stats): &ConfigAndStats,
    ) -> impl Iterator<Item = (isize, &'a ConfigAndStats)> {
        ranked
            .get(&n)
            .unwrap()
            .into_iter()
            .filter(|(_, (config, _))| {
                config.is_superset(prev_config)
                // if config.is_superset(prev_config) {
                //     let f = 1;
                //     let atlas_key = Self::protocol_key("atlas", f);
                //     let prev_atlas =
                // prev_stats.get(&atlas_key).unwrap().mean();
                //     let atlas = stats.get(&atlas_key).unwrap().mean();
                //     let improv = Self::sub(prev_atlas, atlas);
                // // true
                // } else {
                //     false
                // }
            })
            // TODO Is `.cloned()` equivalent to `.map(|&r| r)` here?
            .map(|&r| r)
            // TODO can we avoid collecting here?
            // I wasn't able to do it due to lifetime issues
            .collect::<Vec<_>>()
            .into_iter()
    }

    fn compute_score(
        n: usize,
        stats: &AllStats,
        params: &RankingParams,
    ) -> (bool, isize) {
        // compute score and check if it is a valid configuration
        let mut valid = true;
        let mut score: isize = 0;

        // f values accounted for when computing score and config validity
        let fs = params.ranking_ft.fs(n);

        for f in fs {
            let atlas = stats.get(&Self::protocol_key("atlas", f)).unwrap();
            let lfpaxos = stats.get(&Self::protocol_key("lfpaxos", f)).unwrap();
            let ffpaxos = stats.get(&Self::protocol_key("ffpaxos", f)).unwrap();
            let epaxos = stats.get(&Self::epaxos_protocol_key()).unwrap();

            // compute latency and fairness improvement of atlas wrto lfpaxos
            let (lfpaxos_lat_score, lfpaxos_lat_improv) =
                lfpaxos.mean_score(atlas, params.max_lat);
            let (lfpaxos_fair_score, lfpaxos_fair_improv) =
                ffpaxos.fairness_score(atlas, params.max_fair);

            // check if it's a valid config
            valid = valid
                && lfpaxos_lat_improv >= params.min_lat_improv
                && lfpaxos_fair_improv >= params.min_fair_improv;

            // compute latency and fairness improvement of atlas wrto to ffpaxos
            let (ffpaxos_lat_score, ffpaxos_lat_improv) =
                ffpaxos.mean_score(atlas, params.max_lat);
            let (ffpaxos_fair_score, ffpaxos_fair_improv) =
                ffpaxos.fairness_score(atlas, params.max_fair);

            // compute latency improvement of atlas wrto to epaxos
            let (epaxos_lat_score, epaxos_lat_improv) = if f == 2 {
                epaxos.mean_score(atlas, params.max_lat)
            } else {
                (0, 0)
            };

            // compute final scores
            // let lat_score = lfpaxos_lat_score + ffpaxos_lat_score;
            // let fair_score = lfpaxos_fair_score + ffpaxos_fair_score;

            // DIFF make score be the improvement
            let lat_score = lfpaxos_lat_improv + ffpaxos_lat_improv;
            let fair_score = lfpaxos_fair_improv + ffpaxos_fair_improv;

            // compute score depending on the ranking metric
            score += match params.ranking_metric {
                RankingMetric::Latency => lat_score,
                RankingMetric::Fairness => fair_score,
                RankingMetric::LatencyAndFairness => lat_score + fair_score,
            };
        }

        (valid, score)
    }

    fn max_f(n: usize) -> usize {
        let max_f = 2;
        std::cmp::min(n / 2 as usize, max_f)
    }

    fn protocol_key(prefix: &str, f: usize) -> String {
        format!("{}f{}", prefix, f).to_string()
    }

    fn epaxos_protocol_key() -> String {
        "epaxos".to_string()
    }

    /// It returns a tuple where the:
    /// - 1st component is the set of regions where to look for a configuration
    /// - 2nd component might be a set of clients
    ///
    /// If the 2nd component is `None`, clients are colocated with servers.
    fn search_inputs(
        search_input: &SearchInput,
        planet: &Planet,
    ) -> (Vec<Region>, Option<Vec<Region>>) {
        // compute all regions
        let mut regions = planet.regions();
        regions.sort();

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

        match search_input {
            SearchInput::R20 => (regions, None),
            SearchInput::R20C20 => (regions.clone(), Some(regions)),
            SearchInput::R17C17 => (regions17.clone(), Some(regions17)),
        }
    }

    fn filename(
        min_n: usize,
        max_n: usize,
        search_input: &SearchInput,
    ) -> String {
        format!("{}_{}_{}.data", min_n, max_n, search_input)
    }

    fn get_saved_search(name: &String) -> Option<Search> {
        std::fs::read_to_string(name)
            .ok()
            .map(|json| serde_json::from_str::<Search>(&json).unwrap())
    }

    fn save_search(name: &String, search: &Search) {
        std::fs::write(name, serde_json::to_string(search).unwrap()).unwrap()
    }
}

/// identifies which regions considered for the search
#[allow(dead_code)]
pub enum SearchInput {
    /// search within the 20 regions, clients colocated with servers
    R20,
    /// search within the 20 regions, clients deployed in the 20 regions
    R20C20,
    /// search within 2018 17 regions, clients deployed in the 17 regions
    R17C17,
}

impl std::fmt::Display for SearchInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SearchInput::R20 => write!(f, "R20"),
            SearchInput::R20C20 => write!(f, "R20C20"),
            SearchInput::R17C17 => write!(f, "R17C17"),
        }
    }
}

pub struct RankingParams {
    min_lat_improv: isize,
    min_fair_improv: isize,
    max_lat: isize,
    max_fair: isize,
    min_n: usize,
    max_n: usize,
    ranking_metric: RankingMetric,
    ranking_ft: RankingFT,
}

impl RankingParams {
    pub fn new(
        min_lat_improv: isize,
        min_fair_improv: isize,
        max_lat: isize,
        max_fair: isize,
        min_n: usize,
        max_n: usize,
        ranking_metric: RankingMetric,
        ranking_ft: RankingFT,
    ) -> Self {
        RankingParams {
            min_lat_improv,
            min_fair_improv,
            max_lat,
            max_fair,
            min_n,
            max_n,
            ranking_metric,
            ranking_ft,
        }
    }
}

/// what's consider when raking configurations
#[allow(dead_code)]
pub enum RankingMetric {
    Latency,
    Fairness,
    LatencyAndFairness,
}

/// fault tolerance considered when ranking configurations
#[allow(dead_code)]
pub enum RankingFT {
    F1,
    F1F2,
}

impl RankingFT {
    fn fs(&self, n: usize) -> Vec<usize> {
        let minority = n / 2 as usize;
        let max_f = match self {
            RankingFT::F1 => 1,
            RankingFT::F1F2 => 2,
        };
        (1..=std::cmp::min(minority, max_f)).collect()
    }
}
