mod bench;
mod config;
mod exp;
mod ping;
mod testbed;
mod util;

use color_eyre::Report;
use exp::{Machines, RunMode};
use eyre::WrapErr;
use fantoch::config::Config;
use rusoto_core::Region;
use tsunami::Tsunami;

// aws experiment config
const SERVER_INSTANCE_TYPE: &str = "c5.2xlarge";
const CLIENT_INSTANCE_TYPE: &str = "c5.2xlarge";
const MAX_SPOT_INSTANCE_REQUEST_WAIT_SECS: u64 = 5 * 60; // 5 minutes
const MAX_INSTANCE_DURATION_HOURS: usize = 1;

// run mode
const RUN_MODE: RunMode = RunMode::Release;

// processes config
const PROTOCOL: &str = "newt_atomic";
const GC_INTERVAL: Option<usize> = Some(50); // every 50

const TRACER_SHOW_INTERVAL: Option<usize> = None;

// bench-specific config
const BRANCH: &str = "baremetal";
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

    baremetal_bench(regions, configs, clients_per_region, output_log).await
    // aws_bench(regions, configs, clients_per_region, output_log).await
}

async fn baremetal_bench(
    regions: Vec<Region>,
    configs: Vec<Config>,
    clients_per_region: Vec<usize>,
    output_log: tokio::fs::File,
) -> Result<(), Report> {
    // setup baremetal machines
    let machines =
        testbed::baremetal::setup(regions, BRANCH.to_string(), RUN_MODE)
            .await
            .wrap_err("baremetal spawn")?;

    // run benchmarks
    run_bench(machines, configs, clients_per_region, output_log)
        .await
        .wrap_err("run bench")?;

    Ok(())
}

#[allow(dead_code)]
async fn aws_bench(
    regions: Vec<Region>,
    configs: Vec<Config>,
    clients_per_region: Vec<usize>,
    output_log: tokio::fs::File,
) -> Result<(), Report> {
    let mut launcher: tsunami::providers::aws::Launcher<_> = Default::default();
    let res = do_aws_bench(
        &mut launcher,
        regions,
        configs,
        clients_per_region,
        output_log,
    )
    .await;

    // trap errors to make sure there's time for a debug
    if let Err(e) = &res {
        tracing::warn!("aws bench experiment error: {:?}", e);
    }
    tracing::info!("will wait 5 minutes before terminating spot instances");
    tokio::time::delay_for(tokio::time::Duration::from_secs(300)).await;

    launcher.terminate_all().await?;
    Ok(())
}

async fn do_aws_bench(
    launcher: &mut tsunami::providers::aws::Launcher<
        rusoto_credential::DefaultCredentialsProvider,
    >,
    regions: Vec<Region>,
    configs: Vec<Config>,
    clients_per_region: Vec<usize>,
    output_log: tokio::fs::File,
) -> Result<(), Report> {
    // setup aws machines
    let machines = testbed::aws::setup(
        launcher,
        SERVER_INSTANCE_TYPE.to_string(),
        CLIENT_INSTANCE_TYPE.to_string(),
        regions,
        MAX_SPOT_INSTANCE_REQUEST_WAIT_SECS,
        MAX_INSTANCE_DURATION_HOURS,
        BRANCH.to_string(),
        RUN_MODE,
    )
    .await
    .wrap_err("aws spawn")?;

    // run benchmarks
    run_bench(machines, configs, clients_per_region, output_log)
        .await
        .wrap_err("run bench")?;

    Ok(())
}

async fn run_bench(
    machines: Machines<'_>,
    configs: Vec<Config>,
    clients_per_region: Vec<usize>,
    output_log: tokio::fs::File,
) -> Result<(), Report> {
    bench::bench_experiment(
        machines,
        RUN_MODE,
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
