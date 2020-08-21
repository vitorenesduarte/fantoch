use fantoch::client::{KeyGen, ShardGen, Workload};
use fantoch::config::Config;
use fantoch::id::ProcessId;
use fantoch::planet::{Planet, Region};
use fantoch::protocol::{Protocol, ProtocolMetrics};
use fantoch::sim::Runner;
use fantoch::HashMap;
use fantoch_prof::metrics::Histogram;
use fantoch_ps::protocol::{
    AtlasSequential, EPaxosSequential, FPaxos, NewtSequential,
};
use rayon::prelude::*;
use std::time::Duration;

const STACK_SIZE: usize = 64 * 1024 * 1024; // 64mb

macro_rules! config {
    ($n:expr, $f:expr, $tiny_quorums:expr, $clock_bump_interval:expr, $skip_fast_ack:expr) => {{
        let mut config = Config::new($n, $f);
        config.set_newt_tiny_quorums($tiny_quorums);
        if let Some(interval) = $clock_bump_interval {
            config.set_newt_clock_bump_interval(interval);
        }
        // make sure stability is running
        config.set_gc_interval(Duration::from_millis(1000));
        config.set_skip_fast_ack($skip_fast_ack);
        config
    }};
}

fn main() {
    aws_distance_matrix();

    // build rayon thread pool:
    // - give me two cpus to work
    let spare = 2;
    let threads = num_cpus::get() - spare;
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .stack_size(STACK_SIZE)
        .build_global()
        .unwrap();

    let aws = true;
    newt_real_time(aws);
}

fn aws_distance_matrix() {
    let planet = Planet::from("../latency_aws/");
    let mut regions = planet.regions();
    regions.sort();
    println!("{}", planet.distance_matrix(regions).unwrap());
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

    let ns = vec![3, 5];
    let clients_per_region =
        vec![4, 8, 16, 32, 64, 128, 256, 512, 1024, 1024 * 2, 1024 * 4];

    ns.into_par_iter().for_each(|n| {
        let regions: Vec<_> = regions.clone().into_iter().take(n).collect();

        let configs = if n == 3 {
            vec![
                // (protocol, (n, f, tiny quorums, clock bump interval, skip
                // fast ack))
                ("Atlas", config!(n, 1, false, None, false)),
                ("EPaxos", config!(n, 1, false, None, false)),
                ("FPaxos", config!(n, 1, false, None, false)),
                ("Newt", config!(n, 1, false, None, false)),
            ]
        } else if n == 5 {
            vec![
                // (protocol, (n, f, tiny quorums, clock bump interval, skip
                // fast ack))
                ("Atlas", config!(n, 1, false, None, false)),
                ("Atlas", config!(n, 2, false, None, false)),
                ("EPaxos", config!(n, 0, false, None, false)),
                ("FPaxos", config!(n, 1, false, None, false)),
                ("FPaxos", config!(n, 2, false, None, false)),
                ("Newt", config!(n, 1, false, None, false)),
                ("Newt", config!(n, 2, false, None, false)),
            ]
        } else {
            panic!("unsupported number of processes {}", n);
        };

        clients_per_region.par_iter().for_each(|&clients| {
            configs.par_iter().for_each(|&(protocol, mut config)| {
                // TODO check if the protocol is leader-based, and if yes, run
                // for all possible leader configurations

                // set leader if FPaxos
                if protocol == "FPaxos" {
                    config.set_leader(1);
                }

                // clients workload
                let shards_per_command = 1;
                let shard_gen = ShardGen::Random { shard_count: 1 };
                let keys_per_shard = 1;
                let key_gen = KeyGen::ConflictRate { conflict_rate: 10 };
                let total_commands = 500;
                let payload_size = 0;
                let workload = Workload::new(
                    shards_per_command,
                    shard_gen,
                    keys_per_shard,
                    key_gen,
                    total_commands,
                    payload_size,
                );

                // process regions, client regions and planet
                let process_regions = regions.clone();
                let client_regions = regions.clone();
                let planet = planet.clone();

                let (process_metrics, client_latencies) = match protocol {
                    "Atlas" => run::<AtlasSequential>(
                        config,
                        workload,
                        clients,
                        process_regions,
                        client_regions,
                        planet,
                    ),
                    "EPaxos" => run::<EPaxosSequential>(
                        config,
                        workload,
                        clients,
                        process_regions,
                        client_regions,
                        planet,
                    ),
                    "FPaxos" => run::<FPaxos>(
                        config,
                        workload,
                        clients,
                        process_regions,
                        client_regions,
                        planet,
                    ),
                    "Newt" => run::<NewtSequential>(
                        config,
                        workload,
                        clients,
                        process_regions,
                        client_regions,
                        planet,
                    ),
                    _ => panic!("unsupported protocol {:?}", protocol),
                };
                handle_run_result(
                    protocol,
                    config,
                    clients,
                    process_metrics,
                    client_latencies,
                );
            })
        })
    });
}

#[allow(dead_code)]
fn equidistant<P: Protocol + Eq>(protocol_name: &str) {
    // intra-region distance
    let distance = 200;

    // number of processes and f
    let configs = vec![(3, 1), (5, 2)];

    // total clients
    let total_clients = 1000;

    // clients workload
    let shards_per_command = 1;
    let shard_gen = ShardGen::Random { shard_count: 1 };
    let keys_per_shard = 1;
    let key_gen = KeyGen::ConflictRate { conflict_rate: 2 };
    let total_commands = 500;
    let payload_size = 0;
    let workload = Workload::new(
        shards_per_command,
        shard_gen,
        keys_per_shard,
        key_gen,
        total_commands,
        payload_size,
    );

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

        let (process_metrics, client_latencies) = run::<P>(
            config,
            workload,
            clients_per_region,
            process_regions,
            client_regions,
            planet,
        );
        handle_run_result(
            protocol_name,
            config,
            clients_per_region,
            process_metrics,
            client_latencies,
        );
    }
}

#[allow(dead_code)]
fn increasing_regions<P: Protocol + Eq>(protocol_name: &str) {
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
    let shards_per_command = 1;
    let shard_gen = ShardGen::Random { shard_count: 1 };
    let keys_per_shard = 1;
    let key_gen = KeyGen::ConflictRate { conflict_rate: 2 };
    let total_commands = 500;
    let payload_size = 0;
    let workload = Workload::new(
        shards_per_command,
        shard_gen,
        keys_per_shard,
        key_gen,
        total_commands,
        payload_size,
    );

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

        let (process_metrics, client_latencies) = run::<P>(
            config,
            workload,
            clients_per_region,
            process_regions,
            client_regions,
            planet.clone(),
        );
        handle_run_result(
            protocol_name,
            config,
            clients_per_region,
            process_metrics,
            client_latencies,
        );
    }
}

fn run<P: Protocol + Eq>(
    config: Config,
    workload: Workload,
    clients_per_region: usize,
    process_regions: Vec<Region>,
    client_regions: Vec<Region>,
    planet: Planet,
) -> (
    HashMap<ProcessId, ProtocolMetrics>,
    HashMap<Region, (usize, Histogram)>,
) {
    // compute number of regions and total number of expected commands per
    // region
    let region_count = client_regions.len();
    let expected_commands =
        workload.commands_per_client() * clients_per_region * region_count;

    // run simulation and get latencies
    let mut runner: Runner<P> = Runner::new(
        planet,
        config,
        workload,
        clients_per_region,
        process_regions,
        client_regions,
    );
    let (process_metrics, client_latencies) = runner.run(None);

    // compute clients stats
    let issued_commands = client_latencies
        .values()
        .map(|(issued_commands, _histogram)| issued_commands)
        .sum::<usize>();

    if issued_commands != expected_commands {
        panic!(
            "only issued {} out of {} commands",
            issued_commands, expected_commands,
        );
    }

    (process_metrics, client_latencies)
}

fn handle_run_result(
    protocol_name: &str,
    config: Config,
    clients_per_region: usize,
    process_metrics: HashMap<ProcessId, ProtocolMetrics>,
    client_latencies: HashMap<Region, (usize, Histogram)>,
) {
    // show processes stats
    process_metrics
        .into_iter()
        .for_each(|(process_id, metrics)| {
            println!("process {} metrics:", process_id);
            println!("{:?}", metrics);
        });

    // compute clients stats
    let histogram = client_latencies.into_iter().fold(
        Histogram::new(),
        |mut histogram_acc, (region, (_issued_commands, histogram))| {
            println!("region = {:<14} | {:?}", region.name(), histogram);
            // merge histograms
            histogram_acc.merge(&histogram);
            histogram_acc
        },
    );

    println!(
        "{} n = {} f = {} c = {:<9} | {:?}",
        protocol_name,
        config.n(),
        config.f(),
        clients_per_region,
        histogram
    );
}
