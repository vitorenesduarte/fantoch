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

    // clients workload
    let conflict_rate = 2;
    let total_commands = 500;
    let workload = Workload::new(conflict_rate, total_commands);

    // clients per region
    let clients_per_region = 1000 / 13;
    assert_eq!(clients_per_region, 76);

    for n in vec![3, 5, 7, 9, 11, 13] {
        println!("running simulation for n={}", n);
        println!();
        // config
        let config = Config::new(n, f);

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

        let stats_count = stats.len();
        let expected_commands_per_region = total_commands * clients_per_region;
        let mean = stats
            .iter()
            .map(|(region, (region_issued_commands, region_stats))| {
                if *region_issued_commands != expected_commands_per_region {
                    panic!(
                        "region {:?} has only issued {} out of {} commands",
                        region, region_issued_commands, expected_commands_per_region
                    );
                }
                region_stats.mean().value()
            })
            .sum::<f64>()
            / (stats_count as f64);
        println!("n={} | {}", n, mean as u64);
    }
}
