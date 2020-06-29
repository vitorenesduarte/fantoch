use fantoch::client::Workload;
use fantoch::config::Config;
use fantoch::planet::{Planet, Region};
use fantoch::protocol::Protocol;
use fantoch::sim::Runner;
use fantoch_ps::protocol::{
    AtlasSequential, EPaxosSequential, FPaxos, NewtSequential,
};
use std::collections::HashMap;
use std::thread;

const STACK_SIZE: usize = 64 * 1024 * 1024; // 64mb

fn main() {
    let aws = true;
    newt_real_time(aws);

    aws_distance_matrix();
    epaxos_aws();
    newt_vs_spanner();
}

fn aws_distance_matrix() {
    let planet = Planet::from("../latency_aws/");
    let mut regions = planet.regions();
    regions.sort();
    println!("{}", planet.distance_matrix(regions).unwrap());
}

fn epaxos_aws() {
    let planet = Planet::from("../latency_aws/");
    let regions = vec![
        Region::new("af-south-1"),
        Region::new("ap-east-1"),
        Region::new("eu-west-2"),
        Region::new("me-south-1"),
        Region::new("us-west-2"),
    ];

    println!("{}", planet.distance_matrix(regions.clone()).unwrap());

    // processes
    let process_regions = regions.clone();
    let n = 5;
    let f = 2;
    let transitive_conflicts = true;
    let mut config = Config::new(n, f);
    config.set_transitive_conflicts(transitive_conflicts);

    // make sure stability is running
    config.set_gc_interval(100);

    // clients
    let client_regions = regions.clone();
    let conflict_rate = 100;
    let total_commands = 1000;
    let payload_size = 0;
    let workload = Workload::new(conflict_rate, total_commands, payload_size);

    for clients_per_region in vec![1] {
        // for clients_per_region in vec![10, 20, 50, 100, 200] {
        println!(">running epaxos n = {} | f = {}", n, f);

        run::<EPaxosSequential>(
            config,
            workload,
            clients_per_region,
            process_regions.clone(),
            client_regions.clone(),
            planet.clone(),
        );
    }
}

fn newt_real_time_aws() -> (Planet, Vec<Region>) {
    let planet = Planet::from("../latency_aws");
    let regions = vec![
        Region::new("eu-west-1"),
        Region::new("us-west-1"),
        Region::new("ap-southeast-1"),
        Region::new("ca-central-1"),
        Region::new("sa-east-1"),
    ];
    (planet, regions)
}

fn newt_real_time_gcp() -> (Planet, Vec<Region>) {
    let planet = Planet::new();
    let regions = vec![
        Region::new("asia-south1"),
        Region::new("europe-north1"),
        Region::new("southamerica-east1"),
        Region::new("australia-southeast1"),
        Region::new("europe-west1"),
    ];
    (planet, regions)
}

fn newt_real_time(aws: bool) {
    let (planet, regions) = if aws {
        newt_real_time_aws()
    } else {
        newt_real_time_gcp()
    };
    println!("{}", planet.distance_matrix(regions.clone()).unwrap());
    let f = 1;
    let ns = vec![3, 5];
    for n in ns {
        let regions: Vec<_> = regions.clone().into_iter().take(n).collect();

        println!(">running atlas n = {} | f = {}", n, f);
        let mut config = Config::new(n, f);
        config.set_transitive_conflicts(true);
        let planet_ = planet.clone();
        let regions_ = regions.clone();
        run_in_thread(move || {
            increasing_load::<AtlasSequential>(planet_, regions_, config)
        });

        let leader = 1;
        println!(
            ">running fpaxos n = {} | f = {} | leader = {:?}",
            n,
            f,
            regions[leader - 1]
        );
        let mut config = Config::new(n, f);
        config.set_leader(1);
        let planet_ = planet.clone();
        let regions_ = regions.clone();
        run_in_thread(move || {
            increasing_load::<FPaxos>(planet_, regions_, config)
        });

        let configs = if n == 3 {
            // tiny quorums does nothing for n = 3
            vec![
                // tiny quorums, clock bump interval, skip fast ack
                (false, None, false),
                (false, Some(10), false),
            ]
        } else {
            vec![
                // tiny quorums, clock bump interval, skip fast ack
                (false, None, false),
                (false, Some(10), false),
                (true, Some(10), false),
                (true, Some(10), true),
            ]
        };

        for (tiny_quorums, clock_bump_interval, skip_fast_ack) in configs {
            println!(
                ">running newt n = {} | f = {} | tiny = {} | clock_bump_interval = {} | skip_fast_ack = {}",
                n, f, tiny_quorums, clock_bump_interval.unwrap_or(0), skip_fast_ack
            );
            let mut config = Config::new(n, f);
            config.set_newt_tiny_quorums(tiny_quorums);
            if let Some(interval) = clock_bump_interval {
                config.set_newt_clock_bump_interval(interval);
            }
            config.set_skip_fast_ack(skip_fast_ack);

            let planet_ = planet.clone();
            let regions_ = regions.clone();
            run_in_thread(move || {
                increasing_load::<NewtSequential>(planet_, regions_, config)
            });
        }
    }
}

fn newt_vs_spanner() {
    let planet = Planet::new();
    let regions: Vec<_> = planet
        .regions()
        .into_iter()
        .filter(|region| region != &Region::new("us-east1"))
        .collect();

    let f = 1;
    let n = 19;

    for interval in vec![5] {
        println!(
            ">running newt n = {} | f = {} | clock_bump_interval = {}ms",
            n, f, interval
        );
        let mut config = Config::new(n, f);
        config.set_newt_tiny_quorums(true);
        config.set_newt_clock_bump_interval(interval);
        let planet = planet.clone();
        let regions = regions.clone();
        run_in_thread(move || {
            increasing_load::<NewtSequential>(planet, regions, config)
        });
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
fn increasing_load<P: Protocol>(
    planet: Planet,
    regions: Vec<Region>,
    mut config: Config,
) {
    // make sure stability is running
    config.set_gc_interval(100);

    let cs = vec![4, 32, 256, 512, 1024, 2048];

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
    let issued_commands = clients_latencies
        .values()
        .map(|(issued_commands, _histogram)| issued_commands)
        .sum::<usize>();

    if issued_commands != expected_commands {
        panic!(
            "only issued {} out of {} commands",
            issued_commands, expected_commands,
        );
    }

    /*
    // compute clients stats
    let histogram = clients_latencies.into_iter().fold(
        Histogram::new(),
        |mut histogram_acc, (region, (_issued_commands, histogram))| {
            println!("region = {:<14} | {:?}", region.name(), histogram);
            // merge histograms
            histogram_acc.merge(&histogram);
            histogram_acc
        },
    );

    println!(
        "n = {} AND c = {:<9} | {:?}",
        config.n(),
        clients_per_region,
        histogram
    );
    */

    // do average of percentiles, instead of merging all histograms, as above.
    // TODO: go back to the version above
    let metrics = vec!["min", "max", "avg", "p95", "p99", "p99.9", "p99.99"];
    let mut latencies_to_avg: HashMap<&str, Vec<_>> = Default::default();

    clients_latencies.into_iter().for_each(
        |(region, (_issued_commands, histogram))| {
            println!("region = {:<14} | {:?}", region.name(), histogram);
            latencies_to_avg
                .entry("min")
                .or_default()
                .push(histogram.min().value().round());
            latencies_to_avg
                .entry("max")
                .or_default()
                .push(histogram.max().value().round());
            latencies_to_avg
                .entry("avg")
                .or_default()
                .push(histogram.mean().value().round());
            latencies_to_avg
                .entry("p95")
                .or_default()
                .push(histogram.percentile(0.95).value().round());
            latencies_to_avg
                .entry("p99")
                .or_default()
                .push(histogram.percentile(0.99).value().round());
            latencies_to_avg
                .entry("p99.9")
                .or_default()
                .push(histogram.percentile(0.999).value().round());
            latencies_to_avg
                .entry("p99.99")
                .or_default()
                .push(histogram.percentile(0.9999).value().round());
        },
    );

    let mut line =
        format!("n = {} AND c = {:<9} |", config.n(), clients_per_region);
    for metric in metrics {
        let latencies = latencies_to_avg
            .remove(metric)
            .expect("metric should exist");
        // there should be as many latencies as regions
        assert_eq!(latencies.len(), config.n());
        let avg = latencies.into_iter().sum::<f64>() as usize / config.n();
        line = format!("{} {}={:<6}", line, metric, avg);
    }
    println!("{}", line);
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
