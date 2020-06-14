mod bench;
mod ping;
mod util;

use color_eyre::Report;
use rusoto_core::Region;

// experiment config
const INSTANCE_TYPE: &str = "c5.2xlarge";
const MAX_SPOT_INSTANCE_REQUEST_WAIT_SECS: u64 = 5 * 60; // 5 minutes
const MAX_INSTANCE_DURATION_HOURS: usize = 1;

// bench-specific config
const BRANCH: &str = "exp";
const OUTPUT_LOG: &str = ".tracer_log";

// ping-specific config
const PING_DURATION_SECS: usize = 30 * 60; // 30 minutes

#[tokio::main]
async fn main() -> Result<(), Report> {
    let args: Vec<String> = std::env::args().collect();
    let (instance_type, set_sorted_processes) = match args.as_slice() {
        [_] => (INSTANCE_TYPE, false),
        [_, instance_type] => (instance_type.as_str(), false),
        [_, instance_type, set_sorted_processes] => {
            let set_sorted_processes = set_sorted_processes
                .parse::<bool>()
                .expect("second argument should be a boolean");
            (instance_type.as_str(), set_sorted_processes)
        }
        _ => {
            panic!("at most 2 arguments are expected");
        }
    };

    // init logging
    tracing_subscriber::fmt::init();

    let server_instance_type = instance_type.to_string();
    let client_instance_type = instance_type.to_string();
    let branch = BRANCH.to_string();
    bench(
        server_instance_type,
        client_instance_type,
        branch,
        set_sorted_processes,
    )
    .await
}

async fn bench(
    server_instance_type: String,
    client_instance_type: String,
    branch: String,
    set_sorted_processes: bool,
) -> Result<(), Report> {
    // let regions = vec![
    //     Region::EuWest1,
    //     Region::ApSoutheast1,
    //     Region::CaCentral1,
    //     Region::ApNortheast1,
    //     Region::UsWest1,
    // ];
    // let ns = vec![5, 3];
    let regions = vec![Region::EuWest1, Region::UsWest1, Region::CaCentral1];
    let ns = vec![3];
    let clients_per_region = vec![8, 32, 128, 256, 512];
    let newt_configs = vec![
        // (tiny, real_time, clock_bump_interval)
        (false, false, 0),
        (false, true, 10),
        (true, false, 0),
        (true, true, 10),
    ];
    let output_log = tokio::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(OUTPUT_LOG)
        .await?;
    bench::bench_experiment(
        server_instance_type.clone(),
        client_instance_type.clone(),
        regions.clone(),
        MAX_SPOT_INSTANCE_REQUEST_WAIT_SECS,
        MAX_INSTANCE_DURATION_HOURS,
        branch.clone(),
        ns,
        clients_per_region,
        newt_configs,
        set_sorted_processes,
        output_log,
    )
    .await
}

#[allow(dead_code)]
async fn ping(instance_type: &str) -> Result<(), Report> {
    // all AWS regions
    let regions = vec![
        Region::AfSouth1,
        Region::ApEast1,
        Region::ApNortheast1,
        // Region::ApNortheast2, special-region
        Region::ApSouth1,
        Region::ApSoutheast1,
        Region::ApSoutheast2,
        Region::CaCentral1,
        Region::EuCentral1,
        Region::EuNorth1,
        Region::EuSouth1,
        Region::EuWest1,
        Region::EuWest2,
        Region::EuWest3,
        Region::MeSouth1,
        Region::SaEast1,
        Region::UsEast1,
        Region::UsEast2,
        Region::UsWest1,
        Region::UsWest2,
    ];

    ping::ping_experiment(
        regions,
        instance_type,
        MAX_SPOT_INSTANCE_REQUEST_WAIT_SECS,
        MAX_INSTANCE_DURATION_HOURS,
        PING_DURATION_SECS,
    )
    .await
}
