use fantoch::client::Workload;
use fantoch::config::Config;
use fantoch::metrics::Histogram;
use fantoch::planet::{Planet, Region};
use fantoch::protocol::Protocol;
use fantoch::sim::Runner;
use fantoch_ps::protocol::{AtlasSequential, NewtSequential};
use std::thread;

const STACK_SIZE: usize = 64 * 1024 * 1024; // 64mb

fn main() {
    // println!(">running newt n = 5 | f = 1 | real_time = false");
    // let mut config = Config::new(5, 1);
    // config.set_newt_tiny_quorums(true);
    // config.set_newt_real_time(false);
    // run_in_thread(move || increasing_load::<NewtSequential>(config));

    println!(">running atlas n = 5 | f = 1 | transitive_conflicts = true");
    let mut config = Config::new(5, 1);
    config.set_transitive_conflicts(true);
    run_in_thread(move || increasing_load::<AtlasSequential>(config));

    // println!(">running atlas n = 5 | f = 1 | transitive_conflicts = false");
    // let mut config = Config::new(5, 1);
    // config.set_transitive_conflicts(false);
    // run_in_thread(move || increasing_load::<AtlasSequential>(config));

    for interval in vec![100, 50, 10, 5, 1] {
        println!(
            ">running newt n = 5 | f = 1 | clock_bump_interval = {}ms",
            interval
        );
        let mut config = Config::new(5, 1);
        config.set_newt_tiny_quorums(true);
        config.set_newt_real_time(true);
        config.set_newt_clock_bump_interval(interval);
        run_in_thread(move || increasing_load::<NewtSequential>(config));
    }
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
    let payload_size = 0;
    let workload = Workload::new(conflict_rate, total_commands, payload_size);

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
    let planet = Planet::new();
    let regions = vec![
        Region::new("asia-south1"),
        Region::new("europe-north1"),
        Region::new("southamerica-east1"),
        Region::new("australia-southeast1"),
        Region::new("europe-west1"),
    ];
    assert_eq!(
        regions.len(),
        config.n(),
        "the number of regions should be equals to n"
    );

    // number of clients
    let cs = vec![8, 16, 32, 64, 128, 256];

    // clients workload
    let conflict_rate = 10;
    let total_commands = 500;
    let payload_size = 0;
    let workload = Workload::new(conflict_rate, total_commands, payload_size);

    // TODO check if the protocol is leader-based, and if yes, run for all
    // possible leader configurations

    for &clients_per_region in &cs {
        println!("running clients={}", clients_per_region);
        println!();

        // process regions
        let process_regions = regions.clone();

        // client regions
        let client_regions = regions.clone();

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
fn increasing_regions<P: Protocol>() {
    let planet = Planet::new();
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
    let payload_size = 0;
    let workload = Workload::new(conflict_rate, total_commands, payload_size);

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
    // compute number of regions and total number of expected commands per
    // region
    let region_count = client_regions.len();
    let expected_commands =
        workload.total_commands() * clients_per_region * region_count;

    // run simulation and get latencies
    let mut runner: Runner<P> = Runner::new(
        planet,
        config,
        workload,
        clients_per_region,
        process_regions,
        client_regions,
    );
    let (processes_metrics, clients_latencies) = runner.run(None);
    println!("simulation ended...");

    // show processes stats
    processes_metrics
        .into_iter()
        .for_each(|(process_id, metrics)| {
            println!("process {} metrics:", process_id);
            println!("{:?}", metrics);
        });

    // compute clients stats
    let (issued_commands, histogram) = clients_latencies.into_iter().fold(
        (0, Histogram::new()),
        |(issued_commands_acc, mut histogram_acc),
         (region, (issued_commands, histogram))| {
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

    // wait for thread to end
    if let Err(e) = child.join() {
        println!("error in thread: {:?}", e);
    }
}
