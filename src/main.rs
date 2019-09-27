use permutator::Combination;
use planet_sim::bote::protocol::Protocol;
use planet_sim::bote::stats::Stats;
use planet_sim::bote::Bote;
use planet_sim::planet::{Planet, Region};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::iter::FromIterator;

// directory that contains all dat files
const LAT_DIR: &str = "latency/";
const MIN_LAT_IMPROV: isize = 0;
const MIN_FAIRNESS_IMPROV: isize = 0;
const MIN_SCORE: isize = MIN_LAT_IMPROV + MIN_FAIRNESS_IMPROV;

// config score and stats
type AllStats = BTreeMap<String, Stats>;
type ConfigSS = (isize, BTreeSet<Region>, AllStats);

enum SearchStrategy {
    C20S20,
    C11S11,
    C11S20,
    C9S9,
}

fn main() {
    // create planet
    let planet = Planet::new(LAT_DIR);

    // show clients11 matrix
    // let (_, clients) = get_search_parameters(&planet, SearchStrategy::C9S9);
    // planet.show_distance_matrix(clients);

    // get actual servers and clients
    let strategy = SearchStrategy::C20S20;
    let (servers, clients) = get_search_parameters(&planet, strategy);

    // create bote
    let bote = Bote::from(planet);
    let mut n_to_configs: HashMap<usize, Vec<ConfigSS>> = HashMap::new();

    println!("configs computation started");
    for &n in [3, 5, 7, 9].into_iter() {
        println!("n = {}", n);
        let configs = servers
            .combination(n)
            .filter_map(|config| {
                // clone config
                let config: Vec<Region> = config.into_iter().cloned().collect();

                // compute config score
                match compute_score(&bote, &config, &clients, n) {
                    (true, score, stats) => Some((
                        score,
                        BTreeSet::from_iter(config.into_iter()),
                        stats,
                    )),
                    _ => None,
                }
            })
            .collect();

        // save configs
        n_to_configs.insert(n, configs);
    }

    println!("configs aggregation started");
    let count = get_configs(&n_to_configs, 3).count();
    let mut i = 0;
    let mut configs = BTreeSet::new();

    get_configs(&n_to_configs, 3).for_each(|(score3, config3, stats3)| {
        i += 1;
        println!("{} of {}", i, count);

        get_configs(&n_to_configs, 5)
            .filter(|(_, config5, _)| config3.is_subset(config5))
            .for_each(|(score5, config5, stats5)| {
                get_configs(&n_to_configs, 7)
                    .filter(|(_, config7, _)| config5.is_subset(config7))
                    .for_each(|(score7, config7, stats7)| {
                        get_configs(&n_to_configs, 9)
                            .filter(|(_, config9, _)| {
                                config7.is_subset(config9)
                            })
                            .for_each(|(score9, config9, stats9)| {
                                let score = score3 + score5 + score7 + score9;
                                let config = vec![
                                    (config3, stats3),
                                    (config5, stats5),
                                    (config7, stats7),
                                    (config9, stats9),
                                ];
                                assert!(configs.insert((score, config)));
                            });
                    });
            });
    });

    // show configs
    let max_configs = 1000;
    for (score, config_evolution) in configs.into_iter().rev().take(max_configs)
    {
        let mut sorted_config = Vec::new();
        print!("{}", score);
        for (config, stats) in config_evolution {
            // update sorted config
            for region in config {
                if !sorted_config.contains(&region) {
                    sorted_config.push(region)
                }
            }

            // compute n and max f
            let n = config.len();

            print!(" |");

            // and show stats for all possible f
            for f in 1..=max_f(n) {
                let atlas = stats.get(&key("atlas", f)).unwrap();
                let fpaxos = stats.get(&key("fpaxos", f)).unwrap();
                print!(" a{}={:?} f{}={:?}", f, atlas, f, fpaxos);
            }
            let epaxos = stats.get("epaxos").unwrap();
            print!(" e={:?}", epaxos);
        }
        print!("\n");
        println!("{:?}", sorted_config);
    }
}

fn get_search_parameters(
    planet: &Planet,
    strategy: SearchStrategy,
) -> (Vec<Region>, Vec<Region>) {
    // compute all regions
    let mut regions = planet.regions();
    regions.sort();

    // compute clients11
    let mut clients11 = vec![
        Region::new("asia-east1"),
        Region::new("asia-east2"),
        Region::new("asia-northeast1"),
        Region::new("asia-south1"),
        Region::new("asia-southeast1"),
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
        Region::new("asia-southeast1"),
        Region::new("europe-north1"),
        Region::new("europe-west2"),
        Region::new("southamerica-east1"),
        Region::new("us-east1"),
        Region::new("us-west2"),
    ];
    clients9.sort();

    match strategy {
        SearchStrategy::C20S20 => (regions.clone(), regions),
        SearchStrategy::C11S11 => (clients11.clone(), clients11),
        SearchStrategy::C11S20 => (clients11, regions),
        SearchStrategy::C9S9 => (clients9.clone(), clients9),
    }
}

fn get_configs(
    n_to_configs: &HashMap<usize, Vec<ConfigSS>>,
    n: usize,
) -> impl Iterator<Item = &ConfigSS> {
    n_to_configs.get(&n).unwrap().into_iter()
}

fn compute_score(
    bote: &Bote,
    servers: &Vec<Region>,
    clients: &Vec<Region>,
    n: usize,
) -> (bool, isize, AllStats) {
    assert_eq!(servers.len(), n);

    // compute stats for all protocols
    let stats = compute_stats(bote, servers, clients, n);

    // compute score and check if it is a valid configuration
    let mut valid = true;
    let mut score: isize = 0;
    let mut count: isize = 0;

    // f values accounted for when computing score and config validity
    let fs = vec![1];
    // let fs = if n == 3 { vec![1] } else { vec![1, 2] };
    // let fs = if n == 3 { vec![1] } else { vec![2] };

    for f in fs.into_iter() {
        let atlas = stats.get(&key("atlas", f)).unwrap();
        let fpaxos = stats.get(&key("fpaxos", f)).unwrap();

        // compute improvements of atlas wrto to fpaxos
        let lat_improv = (fpaxos.mean() as isize) - (atlas.mean() as isize);
        let fairness_improv =
            (fpaxos.fairness() as isize) - (atlas.fairness() as isize);

        // compute its score
        score += lat_improv + fairness_improv;
        count += 1;

        // check if this config is valid
        valid = valid && lat_improv >= MIN_LAT_IMPROV;
    }

    // get score average
    score = score / count;
    // check if this config is valid
    valid = valid && score >= MIN_SCORE;

    (valid, score, stats)
}

fn compute_stats(
    bote: &Bote,
    servers: &Vec<Region>,
    clients: &Vec<Region>,
    n: usize,
) -> AllStats {
    let mut stats = BTreeMap::new();

    for f in 1..=max_f(n) {
        // compute atlas stats
        let atlas = bote.leaderless(
            servers,
            clients,
            Protocol::Atlas.quorum_size(n, f),
        );
        stats.insert(key("atlas", f), atlas);

        // compute fpaxos stats
        let fpaxos = bote.best_mean_leader(
            servers,
            clients,
            Protocol::FPaxos.quorum_size(n, f),
        );
        stats.insert(key("fpaxos", f), fpaxos);
    }

    // compute epaxos stats
    let epaxos =
        bote.leaderless(servers, clients, Protocol::EPaxos.quorum_size(n, 0));
    stats.insert("epaxos".to_string(), epaxos);

    // return all stats
    stats
}

fn max_f(n: usize) -> usize {
    let max_f = 3;
    std::cmp::min(n / 2 as usize, max_f)
}

fn key(prefix: &str, f: usize) -> String {
    format!("{}f{}", prefix, f).to_string()
}
