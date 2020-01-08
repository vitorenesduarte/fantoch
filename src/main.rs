use planet_sim::client::Workload;
use planet_sim::config::Config;
use planet_sim::metrics::Stats;
use planet_sim::planet::{Planet, Region};
use planet_sim::protocol::{Atlas, EPaxos, Newt, Process};
use std::thread;

const STACK_SIZE: usize = 64 * 1024 * 1024; // 64mb

fn main() {
    // println!(">running newt n = 5 | f = 1...");
    // let config = Config::new(5, 1);
    // run_in_thread(move || increasing_load::<Newt>(config));
    //
    // println!(">running atlas n = 5 | f = 1...");
    // let mut config = Config::new(5, 1);
    // config.set_transitive_conflicts(true);
    // run_in_thread(move || increasing_load::<Atlas>(config));

    println!(">running atlas n = 5 | f = 2...");
    let mut config = Config::new(5, 2);
    config.set_transitive_conflicts(true);
    run_in_thread(move || increasing_load::<Atlas>(config));

    // println!(">running epaxos n = 5...");
    // let mut config = Config::new(5, 2);
    // config.set_transitive_conflicts(true);
    // run_in_thread(move || increasing_load::<EPaxos>(config));
}

fn equidistant<P: Process>() {
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

fn increasing_load<P: Process>(config: Config) {
    let planet = Planet::new("latency/");
    let regions5 = vec![
        Region::new("asia-south1"),
        Region::new("europe-north1"),
        Region::new("southamerica-east1"),
        Region::new("australia-southeast1"),
        Region::new("europe-west1"),
    ];

    // number of clients
    let cs = vec![8, 16, 32, 64, 128, 256, 512, 768, 1024, 2048];
    let cs = vec![1280, 1576];

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

fn increasing_regions<P: Process>() {
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

fn run<P: Process>(
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
    let (issued_commands, mean_sum, p5_sum, p95_sum, p99_sum, p999_sum, p9999_sum) = latencies
        .into_iter()
        .map(|(_region, (region_issued_commands, region_stats))| {
            (
                region_issued_commands,
                region_stats.mean().value(),
                region_stats.percentile(0.5).value(),
                region_stats.percentile(0.95).value(),
                region_stats.percentile(0.99).value(),
                region_stats.percentile(0.999).value(),
                region_stats.percentile(0.9999).value(),
            )
        })
        .fold(
            (0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
            |(issued_commands_acc, mean_acc, p5_acc, p95_acc, p99_acc, p999_acc, p9999_acc),
             (issued_commands, mean, p5, p95, p99, p999, p9999)| {
                (
                    issued_commands_acc + issued_commands,
                    mean_acc + mean,
                    p5_acc + p5,
                    p95_acc + p95,
                    p99_acc + p99,
                    p999_acc + p999,
                    p9999_acc + p9999,
                )
            },
        );

    if issued_commands != expected_commands {
        panic!(
            "only issued {} out of {} commands",
            issued_commands, expected_commands,
        );
    }
    // TODO averaging of percentiles is just wrong, but we'll do it for now
    let region_count = region_count as f64;
    println!(
        "n = {} AND c = {} | avg={} p5={} p95={} p99={} p99.9={} p99.99={}",
        config.n(),
        clients_per_region,
        (mean_sum / region_count).round() as u64,
        (p5_sum / region_count).round() as u64,
        (p95_sum / region_count).round() as u64,
        (p99_sum / region_count).round() as u64,
        (p999_sum / region_count).round() as u64,
        (p9999_sum / region_count).round() as u64,
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
