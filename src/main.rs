use permutator::Combination;
use planet_sim::bote::protocol::Protocol;
use planet_sim::bote::stats::Stats;
use planet_sim::bote::Bote;
use planet_sim::planet::{Planet, Region};

// directory that contains all dat files
const LAT_DIR: &str = "latency/";

fn main() {
    // create planet
    let planet = Planet::new(LAT_DIR);

    // compute all regions
    let mut regions = planet.regions();
    regions.sort();
    println!("regions: {:?}", regions);

    // compute all clients
    let clients = vec![
        Region::new("us-west2"),
        Region::new("us-east1"),
        Region::new("northamerica-northeast1"),
        Region::new("southamerica-east1"),
        Region::new("europe-west2"),
        Region::new("europe-north1"),
        Region::new("asia-south1"),
        Region::new("asia-southeast1"),
        Region::new("asia-east1"),
        Region::new("asia-east2"),
        Region::new("asia-northeast1"),
    ];
    planet.show_distance_matrix(clients.clone());

    // create bote
    let bote = Bote::from(planet);

    // let configs = [
    //     (3, 1),
    //     (5, 1),
    //     (5, 2),
    //     (7, 1),
    //     (7, 2),
    //     (7, 3),
    //     (9, 1),
    //     (9, 2),
    //     (9, 3),
    //     (11, 1),
    //     (11, 2),
    //     (11, 3),
    // ];

    // only f=1 for now
    let configs = [(3, 1), (5, 1), (7, 1), (9, 1), (11, 1)];
    let min_score = 0;

    // only consider servers where there are clients for now
    // let regions = clients.clone();

    for &(n, f) in configs.into_iter() {
        let mut configs = vec![];

        regions.combination(n).for_each(|config| {
            // clone config
            let config: Vec<Region> = config.into_iter().cloned().collect();

            // compute config score
            let score = compute_score(&bote, &config, &clients, n, f);

            // save score and configuration
            if score >= min_score {
                configs.push((score, config));
            }
        });

        // the worst configs are the last ones
        configs.sort();

        for (_, regions) in configs.into_iter().rev() {
            let (leaderless, leader) =
                compute_stats(&bote, &regions, &clients, n, f);
            println!(
                "{}-{} atlas={:?} fpaxos={:?} {:?}",
                n, f, leaderless, leader, regions
            );
        }
    }
}

fn compute_score(
    bote: &Bote,
    servers: &Vec<Region>,
    clients: &Vec<Region>,
    n: usize,
    f: usize,
) -> isize {
    // compute stats for both protocols
    let (leaderless, leader) = compute_stats(bote, servers, clients, n, f);

    // compute config score, which is the sum of:
    // - latency improvement
    // - fairness improvement
    let latency_improv: isize =
        leader.mean() as isize - leaderless.mean() as isize;
    let fairness_improv: isize =
        leader.fairness() as isize - leaderless.fairness() as isize;
    latency_improv + fairness_improv
}

fn compute_stats(
    bote: &Bote,
    servers: &Vec<Region>,
    clients: &Vec<Region>,
    n: usize,
    f: usize,
) -> (Stats, Stats) {
    // compute atlas stats
    let leaderless_stats = bote.leaderless(
        servers.clone(),
        clients.clone(),
        Protocol::Atlas.quorum_size(n, f),
    );

    // compute fpaxos stats
    let leader_stats = bote.best_mean_leader(
        servers.clone(),
        clients.clone(),
        Protocol::FPaxos.quorum_size(n, f),
    );

    // return both
    (leaderless_stats, leader_stats)
}
