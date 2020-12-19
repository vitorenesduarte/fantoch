use fantoch::client::{KeyGen, Workload};
use fantoch::config::Config;
use fantoch::executor::{ExecutorMetrics, ExecutorMetricsKind};
use fantoch::id::ProcessId;
use fantoch::planet::{Planet, Region};
use fantoch::protocol::{Protocol, ProtocolMetrics, ProtocolMetricsKind};
use fantoch::sim::Runner;
use fantoch::HashMap;
use fantoch_prof::metrics::Histogram;
use fantoch_ps::protocol::{
    AtlasSequential, CaesarSequential, EPaxosSequential, FPaxos, NewtSequential,
};
use rayon::prelude::*;
use std::time::Duration;

const STACK_SIZE: usize = 64 * 1024 * 1024; // 64mb

macro_rules! config {
    ($n:expr, $f:expr, $tiny_quorums:expr, $clock_bump_interval:expr, $skip_fast_ack:expr, $wait_condition:expr) => {{
        let mut config = Config::new($n, $f);
        config.set_newt_tiny_quorums($tiny_quorums);
        if let Some(interval) = $clock_bump_interval {
            config.set_newt_clock_bump_interval(interval);
        }
        // make sure detached votes are sent
        config.set_newt_detached_send_interval(Duration::from_millis(5));

        // set caesar's wait condition
        config.set_caesar_wait_condition($wait_condition);

        // make sure stability is running
        config.set_gc_interval(Duration::from_millis(10));

        // make sure executed notification are being sent (which will only
        // affect the protocols that have implemented such functionality)
        config.set_executor_executed_notification_interval(
            Duration::from_millis(10),
        );

        config.set_skip_fast_ack($skip_fast_ack);
        config
    }};
}

fn main() {
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
    newt(aws);
    // fairest_leader();
}

fn aws_planet() -> (Planet, Vec<Region>) {
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

#[allow(dead_code)]
// average latencies observed during many AWS runs
fn aws_runs_planet() -> (Planet, Vec<Region>) {
    let eu = Region::new("EU");
    let us = Region::new("US");
    let ap = Region::new("AP");
    let ca = Region::new("CA");
    let sa = Region::new("SA");
    let regions =
        vec![eu.clone(), us.clone(), ap.clone(), ca.clone(), sa.clone()];
    let latencies = vec![
        (
            eu.clone(),
            vec![
                (eu.clone(), 0),
                (us.clone(), 136),
                (ap.clone(), 180),
                (ca.clone(), 73),
                (sa.clone(), 177),
            ],
        ),
        (
            us.clone(),
            vec![
                (eu.clone(), 136),
                (us.clone(), 0),
                (ap.clone(), 174),
                (ca.clone(), 79),
                (sa.clone(), 174),
            ],
        ),
        (
            ap.clone(),
            vec![
                (eu.clone(), 180),
                (us.clone(), 174),
                (ap.clone(), 0),
                (ca.clone(), 206),
                (sa.clone(), 317),
            ],
        ),
        (
            ca.clone(),
            vec![
                (eu.clone(), 73),
                (us.clone(), 79),
                (ap.clone(), 206),
                (ca.clone(), 0),
                (sa.clone(), 122),
            ],
        ),
        (
            sa.clone(),
            vec![
                (eu.clone(), 177),
                (us.clone(), 174),
                (ap.clone(), 317),
                (ca.clone(), 122),
                (sa.clone(), 0),
            ],
        ),
    ];
    let latencies = latencies
        .into_iter()
        .map(|(region, region_latencies)| {
            (region, region_latencies.into_iter().collect())
        })
        .collect();
    let planet = Planet::from_latencies(latencies);
    (planet, regions)
}

fn gcp_planet() -> (Planet, Vec<Region>) {
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

#[allow(dead_code)]
fn newt(aws: bool) {
    let (planet, regions) = if aws { aws_planet() } else { gcp_planet() };
    println!("{}", planet.distance_matrix(regions.clone()).unwrap());

    let ns = vec![5];
    let clients_per_region = vec![128, 256, 512];
    let pool_sizes = vec![100, 50, 10, 1];
    let conflicts = vec![0, 1, 2, 5, 10, 20];

    ns.into_par_iter().for_each(|n| {
        let regions: Vec<_> = regions.clone().into_iter().take(n).collect();

        let configs = if n == 3 {
            vec![
                // (protocol, (n, f, tiny quorums, clock bump interval, skip
                // fast ack))
                ("Atlas", config!(n, 1, false, None, false, false)),
                // ("EPaxos", config!(n, 1, false, None, false, false)),
                // ("FPaxos", config!(n, 1, false, None, false, false)),
                ("Newt", config!(n, 1, false, None, false, false)),
            ]
        } else if n == 5 {
            vec![
                // (protocol, (n, f, tiny quorums, clock bump interval, skip
                // fast ack))
                // ("Atlas", config!(n, 1, false, None, false, false)),
                // ("Atlas", config!(n, 2, false, None, false, false)),
                // ("EPaxos", config!(n, 0, false, None, false, false)),
                // ("FPaxos", config!(n, 1, false, None, false, false)),
                // ("FPaxos", config!(n, 2, false, None, false, false)),
                // ("Newt", config!(n, 1, false, None, false, false)),
                // ("Newt", config!(n, 2, false, None, false, false)),
                // ("Caesar", config!(n, 2, false, None, false, false)),
                ("Caesar", config!(n, 2, false, None, false, true)),
            ]
        } else {
            panic!("unsupported number of processes {}", n);
        };

        pool_sizes.iter().for_each(|&pool_size| {
            println!("POOL_SIZE: {:?}", pool_size);
            conflicts.iter().for_each(|&conflict_rate| {
                println!("CONFLICTS: {:?}", conflict_rate);
                clients_per_region.iter().for_each(|&clients| {
                    configs.iter().for_each(|&(protocol, mut config)| {
                        // TODO check if the protocol is leader-based, and if
                        // yes, run for all possible
                        // leader configurations

                        // set leader if FPaxos
                        if protocol == "FPaxos" {
                            config.set_leader(1);
                        }

                        // clients workload
                        let shard_count = 1;
                        let key_gen = KeyGen::ConflictPool {
                            conflict_rate,
                            pool_size,
                        };
                        let keys_per_command = 1;
                        let commands_per_client = 200;
                        let payload_size = 0;
                        let workload = Workload::new(
                            shard_count,
                            key_gen,
                            keys_per_command,
                            commands_per_client,
                            payload_size,
                        );

                        // process regions, client regions and planet
                        let process_regions = regions.clone();
                        let client_regions = regions.clone();
                        let planet = planet.clone();

                        let (metrics, client_latencies) = match protocol {
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
                            "Caesar" => run::<CaesarSequential>(
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
                            metrics,
                            client_latencies,
                        );
                    })
                })
            })
        })
    });
}

#[allow(dead_code)]
fn fairest_leader() {
    let (planet, regions) = aws_runs_planet();
    // let (planet, regions) = aws_planet();
    println!("{}", planet.distance_matrix(regions.clone()).unwrap());

    let configs = vec![config!(5, 1, false, None, false, false)];

    let clients_per_region = 1;
    // clients workload
    let shard_count = 1;
    let key_gen = KeyGen::ConflictPool {
        conflict_rate: 2,
        pool_size: 1,
    };
    let keys_per_command = 1;
    let commands_per_client = 500;
    let payload_size = 0;
    let workload = Workload::new(
        shard_count,
        key_gen,
        keys_per_command,
        commands_per_client,
        payload_size,
    );

    for mut config in configs {
        for leader in 1..=config.n() {
            println!("-----------------------------");
            config.set_leader(leader as ProcessId);

            // process regions, client regions and planet
            let process_regions = regions.clone();
            let client_regions = regions.clone();
            let planet = planet.clone();

            let (_, client_latencies) = run::<FPaxos>(
                config,
                workload,
                clients_per_region,
                process_regions,
                client_regions,
                planet,
            );
            // compute clients stats
            let histogram = client_latencies.into_iter().fold(
                Histogram::new(),
                |mut histogram_acc, (region, (_issued_commands, histogram))| {
                    println!(
                        "region = {:<14} | {:?}",
                        region.name(),
                        histogram
                    );
                    // add average latency to histogram
                    histogram_acc
                        .increment(histogram.mean().value().round() as u64);
                    histogram_acc
                },
            );
            println!(
                "LEADER: {} in region {:?} | avg = {:<3} | std = {:<3}",
                leader,
                regions
                    .get(leader - 1)
                    .expect("leader should exist in regions"),
                histogram.mean().value().round() as u64,
                histogram.stddev().value().round() as u64
            );
        }
    }
}

#[allow(dead_code)]
fn equidistant<P: Protocol>(protocol_name: &str) {
    // intra-region distance
    let distance = 200;

    // number of processes and f
    let configs = vec![(3, 1), (5, 2)];

    // total clients
    let total_clients = 1000;

    // clients workload
    let shard_count = 1;
    let key_gen = KeyGen::ConflictPool {
        conflict_rate: 2,
        pool_size: 1,
    };
    let keys_per_command = 1;
    let total_commands = 500;
    let payload_size = 0;
    let workload = Workload::new(
        shard_count,
        key_gen,
        keys_per_command,
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
fn increasing_regions<P: Protocol>(protocol_name: &str) {
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
    let shard_count = 1;
    let key_gen = KeyGen::ConflictPool {
        conflict_rate: 2,
        pool_size: 1,
    };
    let keys_per_command = 1;
    let total_commands = 500;
    let payload_size = 0;
    let workload = Workload::new(
        shard_count,
        key_gen,
        keys_per_command,
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

fn run<P: Protocol>(
    config: Config,
    workload: Workload,
    clients_per_region: usize,
    process_regions: Vec<Region>,
    client_regions: Vec<Region>,
    planet: Planet,
) -> (
    HashMap<ProcessId, (ProtocolMetrics, ExecutorMetrics)>,
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
    let (metrics, _executors_monitors, client_latencies) = runner.run(None);

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

    (metrics, client_latencies)
}

fn handle_run_result(
    protocol_name: &str,
    config: Config,
    clients_per_region: usize,
    metrics: HashMap<ProcessId, (ProtocolMetrics, ExecutorMetrics)>,
    client_latencies: HashMap<Region, (usize, Histogram)>,
) {
    let mut fast_paths = 0;
    let mut slow_paths = 0;
    let mut wait_condition_delay = Histogram::new();
    let mut commit_latency = Histogram::new();
    let mut execution_delay = Histogram::new();

    // show processes stats
    metrics.into_iter().for_each(
        |(process_id, (process_metrics, executor_metrics))| {
            println!("{}:", process_id);
            println!("  process metrics:");
            println!("{:?}", process_metrics);
            println!("  executor metrics:");
            println!("{:?}", executor_metrics);

            let process_fast_paths = process_metrics
                .get_aggregated(ProtocolMetricsKind::FastPath)
                .cloned()
                .unwrap_or_default();
            let process_slow_paths = process_metrics
                .get_aggregated(ProtocolMetricsKind::SlowPath)
                .cloned()
                .unwrap_or_default();
            let process_wait_condition_delay = process_metrics
                .get_collected(ProtocolMetricsKind::WaitConditionDelay);
            let process_commit_latency = process_metrics
                .get_collected(ProtocolMetricsKind::CommitLatency);
            let executor_execution_delay = executor_metrics
                .get_collected(ExecutorMetricsKind::ExecutionDelay);

            fast_paths += process_fast_paths;
            slow_paths += process_slow_paths;
            if let Some(h) = process_wait_condition_delay {
                wait_condition_delay.merge(h);
            }
            if let Some(h) = process_commit_latency {
                commit_latency.merge(h);
            }
            if let Some(h) = executor_execution_delay {
                execution_delay.merge(h);
            }
        },
    );
    // compute the percentage of fast paths
    let total = fast_paths + slow_paths;
    let fp_percentage = (fast_paths as f64 * 100f64) / total as f64;

    // compute clients stats
    let execution_latency = client_latencies.into_iter().fold(
        Histogram::new(),
        |mut histogram_acc, (region, (_issued_commands, histogram))| {
            println!("region = {:<14} | {:?}", region.name(), histogram);
            // merge histograms
            histogram_acc.merge(&histogram);
            histogram_acc
        },
    );

    let name = |protocol_name, wait_condition: bool| {
        if protocol_name == "Caesar" && !wait_condition {
            "CaesarNW"
        } else {
            protocol_name
        }
    };

    let prefix = format!(
        "{:<8} n = {} f = {} c = {:<3}",
        name(protocol_name, config.caesar_wait_condition()),
        config.n(),
        config.f(),
        clients_per_region
    );
    println!(
        "{} | wait condition delay: {:?}",
        prefix, wait_condition_delay
    );
    println!("{} | commit latency      : {:?}", prefix, commit_latency);
    println!("{} | execution latency   : {:?}", prefix, execution_latency);
    println!("{} | execution delay     : {:?}", prefix, execution_delay);
    println!("{} | fast path rate      : {:<7.1}", prefix, fp_percentage);
}
