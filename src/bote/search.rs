use crate::bote::protocol::Protocol;
use crate::bote::stats::Stats;
use crate::bote::Bote;
use crate::planet::{Planet, Region};
use permutator::Combination;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::iter::FromIterator;

// mapping from protocol name to its stats
type AllStats = BTreeMap<String, Stats>;
// config score and stats (more like: score, config and stats)
type ConfigSS = (isize, BTreeSet<Region>, AllStats);
pub struct Search {
    params: SearchParams,
    all_configs: HashMap<usize, BTreeSet<ConfigSS>>,
}

impl Search {
    pub fn new(
        min_lat_improv: isize,
        min_fairness_improv: isize,
        min_n: usize,
        max_n: usize,
        search_metric: SearchMetric,
        search_ft_filter: SearchFTFilter,
        search_input: SearchInput,
        lat_dir: &str,
    ) -> Self {
        // create planet
        let planet = Planet::new(lat_dir);
        // get all regions
        let all_regions = planet.regions();

        // create bote
        let bote = Bote::from(planet.clone());

        // create search params
        let params = SearchParams::new(
            min_lat_improv,
            min_fairness_improv,
            min_n,
            max_n,
            search_metric,
            search_ft_filter,
            bote,
        );

        // let n = 13;
        // let mut configs = BTreeSet::new();
        // let count = all_regions.combination(n).count();
        // let mut i = 0;
        //
        // all_regions.combination(n).for_each(|config| {
        //     i += 1;
        //     if i % 10000 == 0 {
        //         println!("{} of {}", i, count);
        //     }
        //
        //     // clone config
        //     let config: Vec<_> = config.into_iter().cloned().collect();
        //
        //     // compute config score
        //     match Self::compute_score(&config, &all_regions, &params) {
        //         (true, score, stats) => {
        //             configs.insert((
        //                 score,
        //                 BTreeSet::from_iter(config.into_iter()),
        //                 stats,
        //             ));
        //         }
        //         _ => {}
        //     };
        // });
        //
        // configs.into_iter().rev().take(10).for_each(
        //     |(score, config, stats)| {
        //         println!("{}: {:?}", score, config);
        //         Self::show_stats(n, &stats);
        //         println!("");
        //     },
        // );
        //
        // panic!("a");

        // get regions for servers and clients
        let (regions, clients) = Self::search_inputs(&search_input, &planet);

        // create empty config and get all configs
        let all_configs =
            Self::compute_all_configs(&regions, &clients, &params);

        // return a new `Search` instance
        Search {
            params,
            all_configs,
        }
    }

    pub fn best_configs(&self, max_configs_per_n: usize) {
        (self.params.min_n..=self.params.max_n)
            .step_by(2)
            .for_each(|n| {
                println!("n = {}", n);
                self.get_configs(n).rev().take(max_configs_per_n).for_each(
                    |(score, config, stats)| {
                        println!("{}: {:?}", score, config);
                        Self::show_stats(n, stats);
                        print!("\n");
                    },
                );
            });
    }

    pub fn evolving_configs(&self) {
        // create result variable
        let mut configs = BTreeSet::new();

        // TODO turn what's below in an iterator
        // - this iterator should receive `self.params.max_n`
        // Currently we're assuming that `self.params.max_n == 11`

        let count = self.get_configs(3).count();
        let mut i = 0;

        self.get_configs(3).for_each(|(score3, config3, stats3)| {
            i += 1;
            println!("{} of {}", i, count);
            self.get_configs_superset(5, config3).for_each(
                |(score5, config5, stats5)| {
                    self.get_configs_superset(7, config5).for_each(
                        |(score7, config7, stats7)| {
                            self.get_configs_superset(9, config7).for_each(
                                |(score9, config9, stats9)| {
                                    self.get_configs_superset(11, config9)
                                        .for_each(
                                            |(score11, config11, stats11)| {
                                    self.get_configs_superset(13, config11)
                                        .for_each(
                                            |(score13, config13, stats13)| {
                                                let score = score3
                                                    + score5
                                                    + score7
                                                    + score9
                                                    + score11 + score13;
                                                let config = vec![
                                                    (config3, stats3),
                                                    (config5, stats5),
                                                    (config7, stats7),
                                                    (config9, stats9),
                                                    (config11, stats11),
                                                    (config13, stats13),
                                                ];
                                                assert!(configs
                                                    .insert((score, config)))
                                            },
                                        );
                                            },
                                        );
                                },
                            );
                        },
                    );
                },
            );
        });

        Self::show(configs)
    }

    fn show(configs: BTreeSet<(isize, Vec<(&BTreeSet<Region>, &AllStats)>)>) {
        let max_configs = 1000;
        for (score, config_evolution) in
            configs.into_iter().rev().take(max_configs)
        {
            // compute sorted config and collect all stats
            let mut sorted_config = Vec::new();
            let mut all_stats = Vec::new();

            for (config, stats) in config_evolution {
                // update sorted config
                for region in config {
                    if !sorted_config.contains(&region) {
                        sorted_config.push(region)
                    }
                }

                // save stats
                let n = config.len();
                all_stats.push((n, stats));
            }

            println!("{}: {:?}", score, sorted_config);
            for (n, stats) in all_stats {
                print!("[n={}] ", n);
                Self::show_stats(n, stats);
                print!(" | ");
            }
            println!("");
        }
    }

    fn show_stats(n: usize, stats: &AllStats) {
        // and show stats for all possible f
        for f in 1..=Self::max_f(n) {
            let atlas = stats.get(&Self::protocol_key("atlas", f)).unwrap();
            let fpaxos = stats.get(&Self::protocol_key("fpaxos", f)).unwrap();
            print!("a{}={:?} f{}={:?} ", f, atlas, f, fpaxos);
        }
        let epaxos = stats.get(&Self::epaxos_protocol_key()).unwrap();
        print!("e={:?}", epaxos);
    }

    /// find configurations such that:
    /// - their size is `n`
    fn get_configs(
        &self,
        n: usize,
    ) -> impl DoubleEndedIterator<Item = &ConfigSS> {
        self.all_configs.get(&n).unwrap().into_iter()
    }
    /// find configurations such that:
    /// - their size is `n`
    /// - are a superset of `previous_config`
    fn get_configs_superset(
        &self,
        n: usize,
        previous_config: &BTreeSet<Region>,
    ) -> impl Iterator<Item = &ConfigSS> {
        self.all_configs
            .get(&n)
            .unwrap()
            .into_iter()
            .filter(|(_, config, _)| config.is_superset(previous_config))
            // TODO can we avoid collecting here?
            // I wasn't able to do it due to lifetime issues
            .collect::<Vec<_>>()
            .into_iter()
    }

    fn compute_all_configs(
        regions: &Vec<Region>,
        clients: &Vec<Region>,
        params: &SearchParams,
    ) -> HashMap<usize, BTreeSet<ConfigSS>> {
        (params.min_n..=params.max_n)
            .step_by(2)
            .map(|n| {
                let configs = regions
                    .combination(n)
                    .filter_map(|config| {
                        // clone config
                        let config: Vec<_> =
                            config.into_iter().cloned().collect();
                        // compute config score
                        match Self::compute_score(&config, clients, params) {
                            (true, score, stats) => Some((
                                score,
                                BTreeSet::from_iter(config.into_iter()),
                                stats,
                            )),
                            _ => None,
                        }
                    })
                    .collect();
                (n, configs)
            })
            .collect()
    }

    fn compute_score(
        config: &Vec<Region>,
        clients: &Vec<Region>,
        params: &SearchParams,
    ) -> (bool, isize, AllStats) {
        // compute n
        let n = config.len();

        // compute stats for all protocols
        let stats = Self::compute_stats(config, clients, params);

        // compute score and check if it is a valid configuration
        let mut valid = true;
        let mut score: isize = 0;
        let mut count: isize = 0;

        // f values accounted for when computing score and config validity
        let fs = params.search_ft_filter.fs(n);

        for f in fs {
            let atlas = stats.get(&Self::protocol_key("atlas", f)).unwrap();
            let fpaxos = stats.get(&Self::protocol_key("fpaxos", f)).unwrap();

            // compute improvements of atlas wrto to fpaxos
            let lat_improv = Self::sub(fpaxos.mean(), atlas.mean());
            let fairness_improv =
                Self::sub(fpaxos.fairness(), atlas.fairness());
            let min_max_dist_improv =
                Self::sub(fpaxos.min_max_dist(), atlas.min_max_dist());

            // compute its score depending on the search metric
            score += match params.search_metric {
                SearchMetric::Latency => lat_improv,
                SearchMetric::Fairness => fairness_improv,
                SearchMetric::MinMaxDistance => min_max_dist_improv,
                SearchMetric::LatencyAndFairness => {
                    lat_improv + fairness_improv
                }
            };
            count += 1;

            // check if this config is valid
            valid = valid
                && lat_improv >= params.min_lat_improv
                && fairness_improv >= params.min_fairness_improv;
        }

        // get score average
        score = score / count;

        (valid, score, stats)
    }

    fn compute_stats(
        config: &Vec<Region>,
        clients: &Vec<Region>,
        params: &SearchParams,
    ) -> AllStats {
        // compute n
        let n = config.len();
        let mut stats = BTreeMap::new();

        for f in 1..=Self::max_f(n) {
            // compute atlas stats
            let atlas = params.bote.leaderless(
                config,
                clients,
                Protocol::Atlas.quorum_size(n, f),
            );
            stats.insert(Self::protocol_key("atlas", f), atlas);

            // compute fpaxos stats
            let fpaxos = params.bote.best_fair_leader(
                config,
                clients,
                Protocol::FPaxos.quorum_size(n, f),
            );
            stats.insert(Self::protocol_key("fpaxos", f), fpaxos);
        }

        // compute epaxos stats
        let epaxos = params.bote.leaderless(
            config,
            clients,
            Protocol::EPaxos.quorum_size(n, 0),
        );
        stats.insert(Self::epaxos_protocol_key(), epaxos);

        // return all stats
        stats
    }

    fn sub(a: usize, b: usize) -> isize {
        (a as isize) - (b as isize)
    }

    fn max_f(n: usize) -> usize {
        let max_f = 3;
        std::cmp::min(n / 2 as usize, max_f)
    }

    fn protocol_key(prefix: &str, f: usize) -> String {
        format!("{}f{}", prefix, f).to_string()
    }

    fn epaxos_protocol_key() -> String {
        "epaxos".to_string()
    }

    fn search_inputs(
        search_input: &SearchInput,
        planet: &Planet,
    ) -> (Vec<Region>, Vec<Region>) {
        // compute all regions
        let mut regions = planet.regions();
        regions.sort();

        // compute clients11
        let mut clients11 = vec![
            Region::new("asia-east2"),
            Region::new("asia-northeast1"),
            Region::new("asia-south1"),
            Region::new("asia-southeast1"),
            Region::new("australia-southeast1"),
            Region::new("europe-north1"),
            Region::new("europe-west2"),
            Region::new("northamerica-northeast1"),
            Region::new("southamerica-east1"),
            Region::new("us-east1"),
            Region::new("us-west2"),
        ];
        clients11.sort();

        // compute clients9
        let mut clients9 = vec![
            Region::new("asia-east2"),
            Region::new("asia-northeast1"),
            Region::new("asia-south1"),
            Region::new("australia-southeast1"),
            Region::new("europe-north1"),
            Region::new("europe-west2"),
            Region::new("southamerica-east1"),
            Region::new("us-east1"),
            Region::new("us-west2"),
        ];
        clients9.sort();

        match search_input {
            SearchInput::R20C20 => (regions.clone(), regions),
            SearchInput::R20C11 => (regions, clients11),
            SearchInput::R11C11 => (clients11.clone(), clients11),
            SearchInput::R09C09 => (clients9.clone(), clients9),
        }
    }
}

struct SearchParams {
    min_lat_improv: isize,
    min_fairness_improv: isize,
    min_n: usize,
    max_n: usize,
    search_metric: SearchMetric,
    search_ft_filter: SearchFTFilter,
    bote: Bote,
}

impl SearchParams {
    pub fn new(
        min_lat_improv: isize,
        min_fairness_improv: isize,
        min_n: usize,
        max_n: usize,
        search_metric: SearchMetric,
        search_ft_filter: SearchFTFilter,
        bote: Bote,
    ) -> Self {
        SearchParams {
            min_lat_improv,
            min_fairness_improv,
            min_n,
            max_n,
            search_metric,
            search_ft_filter,
            bote,
        }
    }
}

/// identifies which regions considered for the search
#[allow(dead_code)]
pub enum SearchInput {
    /// 20-clients considered, config search within the 20 regions
    R20C20,
    /// 11-clients considered, config search within the 20 regions
    R20C11,
    /// 11-clients considered, config search within the same 11 regions
    R11C11,
    /// 9-clients considered, config search within the same 9 regions
    R09C09,
}

/// what's consider when raking configurations
#[allow(dead_code)]
pub enum SearchMetric {
    Latency,
    Fairness,
    MinMaxDistance,
    LatencyAndFairness,
}

/// fault tolerance considered when searching for configurations
#[allow(dead_code)]
pub enum SearchFTFilter {
    F1,
    F1F2,
    F1F2F3,
}

impl SearchFTFilter {
    fn fs(&self, n: usize) -> Vec<usize> {
        let minority = n / 2 as usize;
        let max_f = match self {
            SearchFTFilter::F1 => 1,
            SearchFTFilter::F1F2 => 2,
            SearchFTFilter::F1F2F3 => 3,
        };
        (1..=std::cmp::min(minority, max_f)).collect()
    }
}
