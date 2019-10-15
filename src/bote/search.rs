use crate::bote::float::F64;
use crate::bote::protocol::Protocol;
use crate::bote::stats::{AllStats, Stats};
use crate::bote::Bote;
use crate::planet::{Planet, Region};
use permutator::Combination;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::iter::FromIterator;

// config and stats
type ConfigAndStats = (BTreeSet<Region>, AllStats);

// all configs
type AllConfigs = HashMap<usize, Vec<ConfigAndStats>>;

// ranked
type Ranked<'a> = HashMap<usize, Vec<(F64, &'a ConfigAndStats)>>;

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
            let (regions, clients) = search_input.get_inputs(&planet);

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
    ) -> BTreeMap<usize, Vec<(F64, &ConfigAndStats)>> {
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

    pub fn sorted_evolving_configs(
        &self,
        p: &RankingParams,
        max_configs: usize,
    ) -> Vec<(F64, Vec<&ConfigAndStats>)> {
        assert_eq!(p.min_n, 3);
        assert_eq!(p.max_n, 11);

        // first we should rank all configs
        let ranked = self.rank(p);

        // show how many ranked configs we have for each n
        ranked
            .iter()
            .map(|(n, css)| (n, css.len()))
            .collect::<BTreeMap<_, _>>()
            .into_iter()
            .for_each(|(n, count)| println!("{}: {}", n, count));

        // create result variable
        let mut configs = BTreeSet::new();

        // TODO Transform what's below in an iterator.
        // With access to `p.min_n` and `p.max_n` it should be possible.

        let ranked3 = ranked.get(&3).unwrap();
        let count = ranked3.len();
        let mut i = 0;

        Self::configs(&ranked, 3).for_each(|(score3, cs3)| {
            i += 1;
            if i % 10 == 0 {
                println!("{} of {}", i, count);
            }

            Self::super_configs(&ranked, p, 5, cs3).for_each(
                |(score5, cs5)| {
                    Self::super_configs(&ranked, p, 7, cs5).for_each(
                        |(score7, cs7)| {
                            Self::super_configs(&ranked, p, 9, cs7).for_each(
                                |(score9, cs9)| {
                                    Self::super_configs(&ranked, p, 11, cs9).for_each(
                                        |(score11, cs11)| {
                                            let score = score3 + score5 + score7 + score9 + score11;
                                            let css = vec![cs3, cs5, cs7, cs9, cs11];
                                            assert!(configs.insert((score, css)))
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
            // take the first `max_configs`
            .take(max_configs)
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

    fn rank<'a>(&'a self, params: &RankingParams) -> Ranked<'a> {
        self.all_configs
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
        params: &RankingParams,
        n: usize,
        (prev_config, prev_stats): &ConfigAndStats,
    ) -> impl Iterator<Item = (F64, &'a ConfigAndStats)> {
        ranked
            .get(&n)
            .unwrap()
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

            // check if it's a valid config, i.e.,
            // - there's enough `mean_improv`
            // - there's enough `fairness_improv`
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
    /// search within 2018 17 regions, clients colocated with servers
    R17,
    /// search within 2018 17 regions, clients deployed in the 17 regions
    R17C17,
    /// search within the 20 regions, clients colocated with servers
    R20,
    /// search within the 20 regions, clients deployed in the 20 regions
    R20C20,
}

impl std::fmt::Display for SearchInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SearchInput::R17 => write!(f, "R17"),
            SearchInput::R17C17 => write!(f, "R17C17"),
            SearchInput::R20 => write!(f, "R20"),
            SearchInput::R20C20 => write!(f, "R20C20"),
        }
    }
}

impl SearchInput {
    /// It returns a tuple where the:
    /// - 1st component is the set of regions where to look for a configuration
    /// - 2nd component might be a set of clients
    ///
    /// If the 2nd component is `None`, clients are colocated with servers.
    fn get_inputs(
        &self,
        planet: &Planet,
    ) -> (Vec<Region>, Option<Vec<Region>>) {
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
            SearchInput::R17 => (regions17, None),
            SearchInput::R17C17 => (regions17.clone(), Some(regions17)),
            SearchInput::R20 => (regions, None),
            SearchInput::R20C20 => (regions.clone(), Some(regions)),
        }
    }
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
