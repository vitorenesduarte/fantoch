use planet_sim::client::Workload;
use planet_sim::config::Config;
use planet_sim::planet::{Planet, Region};
use planet_sim::protocol::{Atlas, Newt, Process};
use planet_sim::sim::Runner;
use std::thread;

const STACK_SIZE: usize = 64 * 1024 * 1024; // 64mb

fn main() {
    println!(">running epaxos...");
    run_in_thread(|| epaxos::<Atlas>());
    // println!(">running atlas...");
    // run_in_thread(|| increasing_load::<Atlas>());
    // println!(">running newt...");
    // run_in_thread(|| increasing_load::<Newt>());
}

fn epaxos<P: Process>() {
    let regions5 = vec![
        Region::new("A"),
        Region::new("B"),
        Region::new("C"),
        Region::new("D"),
        Region::new("E"),
    ];

    // number of processes and f
    let ns = vec![3, 5];
    let f = 1;

    // clients workload
    let conflict_rate = 2;
    let total_commands = 500;
    let workload = Workload::new(conflict_rate, total_commands);

    for &n in &ns {
        let clients_per_region = 1000 / n;
        println!("running processes={} | clients={}", n, clients_per_region);
        println!();

        // config
        let config = Config::new(n, f);

        // process regions
        let process_regions = regions5.clone().into_iter().take(n).collect();

        // process regions
        let client_regions = regions5.clone().into_iter().take(n).collect();

        run_simulation::<P>(
            config,
            workload,
            clients_per_region,
            process_regions,
            client_regions,
        );
    }
}

fn increasing_load<P: Process>() {
    let regions5 = vec![
        Region::new("asia-south1"),
        Region::new("europe-north1"),
        Region::new("southamerica-east1"),
        Region::new("australia-southeast1"),
        Region::new("europe-west1"),
    ];

    // config
    let n = 5;
    let f = 1;
    let mut config = Config::new(n, f);
    config.set_transitive_conflicts(false);

    // number of clients
    let cs = vec![256];

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

        run_simulation::<P>(
            config,
            workload,
            clients_per_region,
            process_regions,
            client_regions,
        );
    }
}

fn increasing_regions<P: Process>() {
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

        run_simulation::<P>(
            config,
            workload,
            clients_per_region,
            process_regions,
            client_regions,
        );
    }
}

fn run_simulation<P: Process>(
    config: Config,
    workload: Workload,
    clients_per_region: usize,
    process_regions: Vec<Region>,
    client_regions: Vec<Region>,
) {
    // planet
    let planet = Planet::new("latency/");

    // function that creates ping pong processes
    let create_process =
        |process_id, region, planet, config| P::new(process_id, region, planet, config);

    // create runner
    let mut runner = Runner::new(
        planet,
        config,
        create_process,
        workload,
        clients_per_region,
        process_regions,
        client_regions,
    );

    // run simulation and get stats
    let stats = runner.run();
    println!("{:?}", stats);

    // compute number of expected commands per region
    let expected_commands_per_region = workload.total_commands() * clients_per_region;

    let (mean_sum, p5_sum, p95_sum, p99_sum, p999_sum) = stats
        .iter()
        .map(|(region, (region_issued_commands, region_stats))| {
            if *region_issued_commands != expected_commands_per_region {
                panic!(
                    "region {:?} has only issued {} out of {} commands",
                    region, region_issued_commands, expected_commands_per_region
                );
            }
            (
                region_stats.mean().value(),
                region_stats.percentile(0.5).value(),
                region_stats.percentile(0.95).value(),
                region_stats.percentile(0.99).value(),
                region_stats.percentile(0.999).value(),
            )
        })
        .fold(
            (0.0, 0.0, 0.0, 0.0, 0.0),
            |(mean_acc, p5_acc, p95_acc, p99_acc, p999_acc), (mean, p5, p95, p99, p999)| {
                (
                    mean_acc + mean,
                    p5_acc + p5,
                    p95_acc + p95,
                    p99_acc + p99,
                    p999_acc + p999,
                )
            },
        );
    // TODO averaging of percentiles is just wrong, but we'll do it for now
    let stats_count = stats.len() as f64;
    println!(
        "n={} AND c={} | avg={} p5={} p95={} p99={} p99.9={}",
        config.n(),
        clients_per_region,
        mean_sum / stats_count,
        p5_sum / stats_count,
        p95_sum / stats_count,
        p99_sum / stats_count,
        p999_sum / stats_count
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
