use planet_sim::bote::Bote;
use planet_sim::client::Workload;
use planet_sim::config::Config;
use planet_sim::metrics::Histogram;
use planet_sim::planet::{Planet, Region};
use planet_sim::protocol::{Atlas, EPaxos, Newt, Protocol};
use std::thread;

const STACK_SIZE: usize = 64 * 1024 * 1024; // 64mb

fn main() {
    println!(">running newt n = 5 | f = 1...");
    let config = Config::new(5, 1);
    run_in_thread(move || increasing_load::<Newt>(config));

    println!(">running atlas n = 5 | f = 1...");
    let mut config = Config::new(5, 1);
    config.set_transitive_conflicts(true);
    run_in_thread(move || increasing_load::<Atlas>(config));

    println!(">running atlas n = 5 | f = 2...");
    let mut config = Config::new(5, 2);
    config.set_transitive_conflicts(true);
    run_in_thread(move || increasing_load::<Atlas>(config));

    println!(">running epaxos n = 5...");
    let mut config = Config::new(5, 2);
    config.set_transitive_conflicts(true);
    run_in_thread(move || increasing_load::<EPaxos>(config));

    println!(">running fpaxos n = 5 | f = 1");
    let config = Config::new(5, 1);
    increasing_load_fpaxos(config);
}

#[allow(dead_code)]
fn equidistant<P: Protocol>() {
    // intra-region distance
    let distance = 200;

    // number of processes and f
    let configs = vec![(3, 1), (5, 2)];

    // total clients
    let total_clients = 1000;

    // clients workload
    let conflict_rate = 2;
    let total_commands = 500;
    let workload = Workload::new(conflict_rate, total_commands);

    for &(n, f) in &configs {
        // create planet and regions
        let (process_regions, planet) = Planet::equidistant(distance, n);

        // client regions
        let client_regions = process_regions.clone();

        let clients_per_region = total_clients / n;
        println!("running processes={} | clients={}", n, clients_per_region);
        println!();

        // config
        let config = Config::new(n, f);

        run::<P>(
            config,
            workload,
            clients_per_region,
            process_regions,
            client_regions,
            planet,
        );
    }
}

#[allow(dead_code)]
fn increasing_load<P: Protocol>(config: Config) {
    let planet = Planet::new("latency/");
    let regions5 = vec![
        Region::new("asia-south1"),
        Region::new("europe-north1"),
        Region::new("southamerica-east1"),
        Region::new("australia-southeast1"),
        Region::new("europe-west1"),
    ];

    // number of clients
    let cs = vec![8, 16, 32, 64, 128, 256, 512];

    // clients workload
    let conflict_rate = 10;
    let total_commands = 500;
    let workload = Workload::new(conflict_rate, total_commands);

    for &clients_per_region in &cs {
        println!("running clients={}", clients_per_region);
        println!();

        // process regions
        let process_regions = regions5.clone();

        // client regions
        let client_regions = regions5.clone();

        run::<P>(
            config,
            workload,
            clients_per_region,
            process_regions,
            client_regions,
            planet.clone(),
        );
    }
}

#[allow(dead_code)]
fn increasing_load_fpaxos(config: Config) {
    let planet = Planet::new("latency/");
    let bote = Bote::from(planet);

    // servers and clients
    let servers = vec![
        Region::new("asia-south1"),
        Region::new("europe-north1"),
        Region::new("southamerica-east1"),
        Region::new("australia-southeast1"),
        Region::new("europe-west1"),
    ];
    let clients = servers.clone();

    // compute quorum size
    let quorum_size = config.f() + 1;

    // for each possible leader
    servers.iter().for_each(|leader| {
        println!("leader: {:?}", leader);
        let latencies = bote.leader(leader, &servers, &clients, quorum_size);
        // show latency for each client
        latencies.clone().into_iter().for_each(|(region, latency)| {
            // create histogram
            let histogram = Histogram::from(vec![latency]);
            println!("region = {:?} |   {:?}", region, histogram);
        });

        // global histogram
        let histogram = Histogram::from(latencies.into_iter().map(|(_, latency)| latency));
        println!("n = {} AND c = {} |  {:?}", config.n(), 1, histogram);
    });
}

#[allow(dead_code)]
fn increasing_regions<P: Protocol>() {
    let planet = Planet::new("latency/");
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

    // number of processes and f
    let ns = vec![3, 5, 7, 9, 11, 13];
    let f = 1;

    // clients workload
    let conflict_rate = 2;
    let total_commands = 500;
    let workload = Workload::new(conflict_rate, total_commands);

    // clients per region
    let clients_per_region = 1000 / 13;
    assert_eq!(clients_per_region, 76);

    for &n in &ns {
        println!("running processes={}", n);
        println!();

        // config
        let config = Config::new(n, f);

        // process regions
        let process_regions = regions13.clone().into_iter().take(n).collect();

        // client regions
        let client_regions = regions13.clone();

        run::<P>(
            config,
            workload,
            clients_per_region,
            process_regions,
            client_regions,
            planet.clone(),
        );
    }
}

fn run<P: Protocol>(
    config: Config,
    workload: Workload,
    clients_per_region: usize,
    process_regions: Vec<Region>,
    client_regions: Vec<Region>,
    planet: Planet,
) {
    // compute number of regions and total number of expected commands per region
    let region_count = client_regions.len();
    let expected_commands = workload.total_commands() * clients_per_region * region_count;

    // run simulation and get latencies
    let latencies = planet_sim::sim::run_simulation::<P>(
        config,
        workload,
        clients_per_region,
        process_regions,
        client_regions,
        planet,
    );
    println!("simulation ended...");

    // compute stats
    let (issued_commands, histogram) = latencies.into_iter().fold(
        (0, Histogram::new()),
        |(issued_commands_acc, mut histogram_acc), (region, (issued_commands, histogram))| {
            println!("region = {:?} |   {:?}", region, histogram);
            // merge histograms
            histogram_acc.merge(&histogram);
            (issued_commands_acc + issued_commands, histogram_acc)
        },
    );

    if issued_commands != expected_commands {
        panic!(
            "only issued {} out of {} commands",
            issued_commands, expected_commands,
        );
    }
    println!(
        "n = {} AND c = {} |  {:?}",
        config.n(),
        clients_per_region,
        histogram
    );
}

fn run_in_thread<F>(run: F)
where
    F: FnOnce(),
    F: Send + 'static,
{
    // Spawn thread with explicit stack size
    let child = thread::Builder::new()
        .stack_size(STACK_SIZE)
        .spawn(run)
        .unwrap();

    // Wait for thread to join
    child.join().unwrap();
}
