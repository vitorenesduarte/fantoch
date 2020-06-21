mod bench;
mod config;
mod ping;
mod util;

use color_eyre::Report;
use fantoch::config::Config;
use rusoto_core::Region;

// experiment config
const INSTANCE_TYPE: &str = "c5.2xlarge";
const MAX_SPOT_INSTANCE_REQUEST_WAIT_SECS: u64 = 5 * 60; // 5 minutes
const MAX_INSTANCE_DURATION_HOURS: usize = 3;

// processes config
const PROTOCOL: &str = "newt_atomic";
const GC_INTERVAL: Option<usize> = Some(50); // every 50
const TRACER_SHOW_INTERVAL: Option<usize> = None;

// bench-specific config
const BRANCH: &str = "executor_metrics";
const OUTPUT_LOG: &str = ".tracer_log";

// ping-specific config
const PING_DURATION_SECS: usize = 30 * 60; // 30 minutes

macro_rules! config {
    ($n:expr, $f:expr, $tiny_quorums:expr, $clock_bump_interval:expr, $skip_fast_ack:expr) => {{
        let mut config = Config::new($n, $f);
        config.set_newt_tiny_quorums($tiny_quorums);
        if let Some(interval) = $clock_bump_interval {
            config.set_newt_clock_bump_interval(interval);
        }
        config.set_skip_fast_ack($skip_fast_ack);
        if let Some(interval) = GC_INTERVAL {
            config.set_gc_interval(interval);
        }
        config
    }};
}

#[tokio::main]
async fn main() -> Result<(), Report> {
    // init logging
    tracing_subscriber::fmt::init();

    let server_instance_type = INSTANCE_TYPE.to_string();
    let client_instance_type = INSTANCE_TYPE.to_string();
    let branch = BRANCH.to_string();
    bench(server_instance_type, client_instance_type, branch).await
}

async fn bench(
    server_instance_type: String,
    client_instance_type: String,
    branch: String,
) -> Result<(), Report> {
    let regions = vec![
        Region::EuWest1,
        Region::UsWest1,
        Region::ApSoutheast1,
        Region::CaCentral1,
        Region::SaEast1,
    ];
    let configs = vec![
        // n, f, tiny quorums, clock bump interval, skip fast ack
        config!(5, 1, false, None, false),
        config!(5, 1, false, Some(10), false),
        config!(5, 1, false, None, true),
        config!(5, 1, false, Some(10), true),
        config!(5, 1, true, None, false),
        config!(5, 1, true, Some(10), false),
        config!(5, 1, true, None, true),
        config!(5, 1, true, Some(10), true),
    ];
    let clients_per_region = vec![4, 32, 256, 512, 1024];

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
        PROTOCOL.to_string(),
        configs,
        TRACER_SHOW_INTERVAL,
        clients_per_region,
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
