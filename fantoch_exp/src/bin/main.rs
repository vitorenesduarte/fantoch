use color_eyre::eyre::WrapErr;
use color_eyre::Report;
use fantoch::client::{KeyGen, ShardGen, Workload};
use fantoch::config::Config;
use fantoch::planet::Planet;
use fantoch_exp::exp::Machines;
use fantoch_exp::{FantochFeature, Protocol, RunMode, Testbed};
use rusoto_core::Region;
use std::time::Duration;
use tsunami::Tsunami;

// folder where all results will be stored
const RESULTS_DIR: &str = "../partial_replication";

// aws experiment config
const SERVER_INSTANCE_TYPE: &str = "m5.4xlarge";
// const SERVER_INSTANCE_TYPE: &str = "c5.2xlarge";
const CLIENT_INSTANCE_TYPE: &str = "c5.2xlarge";
const MAX_SPOT_INSTANCE_REQUEST_WAIT_SECS: u64 = 5 * 60; // 5 minutes
const MAX_INSTANCE_DURATION_HOURS: usize = 1;

// run mode
const RUN_MODE: RunMode = RunMode::Release;

// processes config
const GC_INTERVAL: Option<Duration> = Some(Duration::from_millis(50)); // every 50
const TRANSITIVE_CONFLICTS: bool = true;
const TRACER_SHOW_INTERVAL: Option<usize> = None;

// clients config
const COMMANDS_PER_CLIENT: usize = 500; // 500 if WAN, 500_000 if LAN
const PAYLOAD_SIZE: usize = 0; // 0 if no bottleneck, 4096 if paxos bottleneck

// bench-specific config
const BRANCH: &str = "master";
// TODO allow more than one feature
const FEATURE: Option<FantochFeature> = None;
// const FEATURE: Option<FantochFeature> = Some(FantochFeature::Amortize);

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
        config.set_transitive_conflicts(TRANSITIVE_CONFLICTS);
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
        /*
        Region::CaCentral1,
        Region::SaEast1,
        */
    ];
    let n = regions.len();

    /*
    THROUGHPUT
    let configs = vec![
        // (protocol, (n, f, tiny quorums, clock bump interval, skip fast ack))
        (Protocol::NewtAtomic, config!(n, 1, false, None, false)),
        (Protocol::NewtAtomic, config!(n, 2, false, None, false)),
        (Protocol::FPaxos, config!(n, 1, false, None, false)),
        (Protocol::FPaxos, config!(n, 2, false, None, false)),
        (Protocol::AtlasLocked, config!(n, 1, false, None, false)),
        (Protocol::AtlasLocked, config!(n, 2, false, None, false)),
    ];

    let clients_per_region = vec![
        4,
        8,
        16,
        32,
        64,
        128,
        256,
        512,
        1024,
        1024 * 2,
        1024 * 4,
        1024 * 8,
        1024 * 16,
        1024 * 32,
    ];

    let skip = |protocol, _, clients| {
        // skip Atlas with more than 4096 clients
        protocol == Protocol::AtlasLocked && clients > 1024 * 4
    };
    */

    /*
    MULTI_KEY
    let configs = vec![
        // (protocol, (n, f, tiny quorums, clock bump interval, skip fast ack))
        (Protocol::NewtAtomic, config!(n, 1, false, None, false)),
        (Protocol::NewtAtomic, config!(n, 2, false, None, false)),
        (Protocol::NewtLocked, config!(n, 1, false, None, false)),
        (Protocol::NewtLocked, config!(n, 2, false, None, false)),
        (Protocol::NewtFineLocked, config!(n, 1, false, None, false)),
        (Protocol::NewtFineLocked, config!(n, 2, false, None, false)),
    ];

    let clients_per_region =
        vec![256, 1024, 1024 * 4, 1024 * 8, 1024 * 16, 1024 * 32];
    let zipf_key_count = 1_000_000;

    let skip = |_, _, _| false;
    */

    let mut configs = vec![
        // (protocol, (n, f, tiny quorums, clock bump interval, skip fast ack))
        (Protocol::NewtAtomic, config!(n, 1, false, None, false)),
    ];

    let clients_per_region = vec![
        /*
        256,
        1024,
        1024 * 4,
        1024 * 8,
        1024 * 16,
        1024 * 32,
        1024 * 64,
        1024 * 80,
        1024 * 96,
        */
        1024 * 112,
        1024 * 128,
        1024 * 144,
    ];
    let shards_per_command = 1;
    let shard_count = 4;
    let keys_per_shard = 1;
    let zipf_coefficient = 1.0;
    let zipf_key_count = 1_000_000;
    let key_gen = KeyGen::Zipf {
        coefficient: zipf_coefficient,
        key_count: zipf_key_count,
    };

    let skip = |_, _, _| false;

    // set shards in each config
    configs
        .iter_mut()
        .for_each(|(_protocol, config)| config.set_shards(shard_count));

    let mut workloads = Vec::new();
    let workload = Workload::new(
        shards_per_command,
        ShardGen::Random { shard_count },
        keys_per_shard,
        key_gen,
        COMMANDS_PER_CLIENT,
        PAYLOAD_SIZE,
    );
    workloads.push(workload);

    // create AWS planet
    let planet = Some(Planet::from("../latency_aws"));
    // let planet = None; // if delay is not to be injected

    baremetal_bench(
        regions,
        shard_count,
        planet,
        configs,
        clients_per_region,
        workloads,
        skip,
    )
    .await
    /*
    aws_bench(
        regions,
        shard_count,
        configs,
        clients_per_region,
        workloads,
        skip,
    ).await
    */
}

#[allow(dead_code)]
async fn baremetal_bench(
    regions: Vec<Region>,
    shard_count: usize,
    planet: Option<Planet>,
    configs: Vec<(Protocol, Config)>,
    clients_per_region: Vec<usize>,
    workloads: Vec<Workload>,
    skip: impl Fn(Protocol, Config, usize) -> bool,
) -> Result<(), Report>
where
{
    // create launcher
    let mut launchers = fantoch_exp::testbed::baremetal::create_launchers(
        &regions,
        shard_count,
    );

    // compute features
    let features = FEATURE.map(|feature| vec![feature]).unwrap_or_default();

    // setup baremetal machines
    let machines = fantoch_exp::testbed::baremetal::setup(
        &mut launchers,
        regions,
        shard_count,
        BRANCH.to_string(),
        RUN_MODE,
        features.clone(),
    )
    .await
    .wrap_err("baremetal spawn")?;

    // run benchmarks
    run_bench(
        machines,
        features,
        Testbed::Baremetal,
        planet,
        configs,
        clients_per_region,
        workloads,
        skip,
    )
    .await
    .wrap_err("run bench")?;

    Ok(())
}

#[allow(dead_code)]
async fn aws_bench(
    regions: Vec<Region>,
    shard_count: usize,
    configs: Vec<(Protocol, Config)>,
    clients_per_region: Vec<usize>,
    workloads: Vec<Workload>,
    skip: impl Fn(Protocol, Config, usize) -> bool,
) -> Result<(), Report> {
    let mut launcher: tsunami::providers::aws::Launcher<_> = Default::default();
    let res = do_aws_bench(
        &mut launcher,
        regions,
        shard_count,
        configs,
        clients_per_region,
        workloads,
        skip,
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
    shard_count: usize,
    configs: Vec<(Protocol, Config)>,
    clients_per_region: Vec<usize>,
    workloads: Vec<Workload>,
    skip: impl Fn(Protocol, Config, usize) -> bool,
) -> Result<(), Report> {
    // compute features
    let features = FEATURE.map(|feature| vec![feature]).unwrap_or_default();

    // setup aws machines
    let machines = fantoch_exp::testbed::aws::setup(
        launcher,
        regions,
        shard_count,
        SERVER_INSTANCE_TYPE.to_string(),
        CLIENT_INSTANCE_TYPE.to_string(),
        MAX_SPOT_INSTANCE_REQUEST_WAIT_SECS,
        MAX_INSTANCE_DURATION_HOURS,
        BRANCH.to_string(),
        RUN_MODE,
        features.clone(),
    )
    .await
    .wrap_err("aws spawn")?;

    // no need for aws planet
    let planet = None;

    // run benchmarks
    run_bench(
        machines,
        features,
        Testbed::Aws,
        planet,
        configs,
        clients_per_region,
        workloads,
        skip,
    )
    .await
    .wrap_err("run bench")?;

    Ok(())
}

async fn run_bench(
    machines: Machines<'_>,
    features: Vec<FantochFeature>,
    testbed: Testbed,
    planet: Option<Planet>,
    configs: Vec<(Protocol, Config)>,
    clients_per_region: Vec<usize>,
    workloads: Vec<Workload>,
    skip: impl Fn(Protocol, Config, usize) -> bool,
) -> Result<(), Report> {
    fantoch_exp::bench::bench_experiment(
        machines,
        RUN_MODE,
        features,
        testbed,
        planet,
        configs,
        TRACER_SHOW_INTERVAL,
        clients_per_region,
        workloads,
        skip,
        RESULTS_DIR,
    )
    .await
}
