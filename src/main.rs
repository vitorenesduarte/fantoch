use planet_sim::client::Workload;
use planet_sim::config::Config;
use planet_sim::planet::{Planet, Region};
use planet_sim::protocol::{Newt, Process};
use planet_sim::sim::Runner;

fn main() {
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

    // planet
    let planet = Planet::new("latency/");

    // f
    let f = 1;
    // let tiny_quorums_configs = vec![false, true];
    let tiny_quorums_configs = vec![false];

    // clients workload
    let conflict_rate = 2;
    let total_commands = 500;
    let workload = Workload::new(conflict_rate, total_commands);

    // clients per region
    let clients_per_region = 1000 / 13;
    assert_eq!(clients_per_region, 76);

    for tiny_quorums in tiny_quorums_configs {
        println!("TINY QUORUMS: {}", tiny_quorums);
        for n in vec![3, 5, 7, 9, 11, 13] {
            println!("running simulation for n={}", n);
            println!();
            // config
            let mut config = Config::new(n, f);
            config.set_newt_tiny_quorums(tiny_quorums);

            // function that creates ping pong processes
            let create_process =
                |process_id, region, planet, config| Newt::new(process_id, region, planet, config);

            // process regions
            let process_regions = regions13.clone().into_iter().take(n).collect();

            // client regions
            let client_regions = regions13.clone();

            // create runner
            let mut runner = Runner::new(
                planet.clone(),
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

            let expected_commands_per_region = total_commands * clients_per_region;
            let (mean_sum, p5_sum, p95_sum, p99_sum) = stats
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
                    )
                })
                .fold(
                    (0.0, 0.0, 0.0, 0.0),
                    |(mean_acc, p5_acc, p95_acc, p99_acc), (mean, p5, p95, p99)| {
                        (mean_acc + mean, p5_acc + p5, p95_acc + p95, p99_acc + p99)
                    },
                );
            // TODO averaging of percentiles is just wrong, but we'll do it for now
            let stats_count = stats.len() as f64;
            println!(
                "n={} | avg={} p5={} p95={} p99={}",
                n,
                mean_sum / stats_count,
                p5_sum / stats_count,
                p95_sum / stats_count,
                p99_sum / stats_count
            );
        }
    }
}
