use color_eyre::eyre::WrapErr;
use color_eyre::Report;
use fantoch::client::{KeyGen, ShardGen, Workload};
use fantoch::config::Config;
use fantoch::planet::Planet;
use fantoch_exp::bench::ExperimentTimeouts;
use fantoch_exp::machine::Machines;
use fantoch_exp::progress::TracingProgressBar;
use fantoch_exp::{FantochFeature, Protocol, RunMode, Testbed};
use rusoto_core::Region;
use std::time::Duration;
use tsunami::providers::aws::LaunchMode;
use tsunami::Tsunami;

// folder where all results will be stored
// const RESULTS_DIR: &str = "../graph_executor";
const RESULTS_DIR: &str = "../graph_executor_zipf01_transitive_bulk";

// timeouts
const fn minutes(minutes: u64) -> Duration {
    Duration::from_secs(3600 * minutes)
}
const EXPERIMENT_TIMEOUTS: ExperimentTimeouts = ExperimentTimeouts {
    start: Some(minutes(20)),
    run: Some(minutes(20)),
    stop: Some(minutes(20)),
};

// aws experiment config
const LAUCH_MODE: LaunchMode = LaunchMode::DefinedDuration { hours: 1 };
const SERVER_INSTANCE_TYPE: &str = "m5.4xlarge";
// const SERVER_INSTANCE_TYPE: &str = "c5.2xlarge";
const CLIENT_INSTANCE_TYPE: &str = "c5.2xlarge";
const MAX_SPOT_INSTANCE_REQUEST_WAIT_SECS: u64 = 5 * 60; // 5 minutes

// processes config
const EXECUTOR_CLEANUP_INTERVAL: Duration = Duration::from_millis(5);
const GC_INTERVAL: Option<Duration> = Some(Duration::from_millis(50));
const SEND_DETACHED_INTERVAL: Duration = Duration::from_millis(5);
const TRANSITIVE_CONFLICTS: bool = true;
const TRACER_SHOW_INTERVAL: Option<usize> = None;

// clients config
const COMMANDS_PER_CLIENT: usize = 500; // 500 if WAN, 500_000 if LAN
const PAYLOAD_SIZE: usize = 0; // 0 if no bottleneck, 4096 if paxos bottleneck

// processes and client config
const CPUS: Option<usize> = None;

// fantoch run config
const BRANCH: &str = "parallel_graph_executor_v2";

// release run
const FEATURES: &[FantochFeature] = &[FantochFeature::Jemalloc];
const RUN_MODE: RunMode = RunMode::Release;

// heaptrack run
// const FEATURES: &[FantochFeature] = &[];
// const RUN_MODE: RunMode = RunMode::Heaptrack;

// flamegraph run
// const FEATURES: &[FantochFeature] = &[FantochFeature::Jemalloc];
// const RUN_MODE: RunMode = RunMode::Flamegraph;

// list of protocol binaries to cleanup before running the experiment
const PROTOCOLS_TO_CLEANUP: &[Protocol] =
    &[Protocol::NewtAtomic, Protocol::AtlasLocked];

macro_rules! config {
    ($n:expr, $f:expr, $tiny_quorums:expr, $clock_bump_interval:expr, $skip_fast_ack:expr) => {{
        let mut config = Config::new($n, $f);
        config.set_newt_tiny_quorums($tiny_quorums);
        if let Some(interval) = $clock_bump_interval {
            config.set_newt_clock_bump_interval(interval);
        }
        config.set_skip_fast_ack($skip_fast_ack);
        config.set_executor_cleanup_interval(EXECUTOR_CLEANUP_INTERVAL);
        if let Some(interval) = GC_INTERVAL {
            config.set_gc_interval(interval);
        }
        config.set_newt_detached_send_interval(SEND_DETACHED_INTERVAL);
        config.set_transitive_conflicts(TRANSITIVE_CONFLICTS);
        config
    }};
}

#[tokio::main]
async fn main() -> Result<(), Report> {
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
        (Protocol::AtlasLocked, config!(n, 1, false, None, false)),
        // (Protocol::NewtAtomic, config!(n, 1, false, None, false)),
    ];

    let clients_per_region = vec![
        // 1024 / 4,
        // 1024 / 2,
        // 1024,
        // 1024 * 2,
        1024 * 4, // 1
        1024 * 8, // 1
        1024 * 16,
        1024 * 24, // 1
        1024 * 32,
        1024 * 36, // 1
        1024 * 40, // 1
        1024 * 48,
        1024 * 56, // 1
        1024 * 64,
        1024 * 96,
        1024 * 128,
        1024 * 160,
        1024 * 192,
        1024 * 224,
        1024 * 240,
        1024 * 256,
        1024 * 272,
    ];
    let shards_per_command = 2;
    let shard_count = 5;
    let keys_per_shard = 1;
    let zipf_key_count = 1_000_000;
    let key_gen = KeyGen::Zipf {
        coefficient: 0.1,
        key_count: zipf_key_count,
    };
    // let key_gen = KeyGen::ConflictRate { conflict_rate: 0 };

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

    // init logging
    let progress = TracingProgressBar::init(
        (workloads.len() * clients_per_region.len() * configs.len()) as u64,
    );

    baremetal_bench(
        regions,
        shard_count,
        planet,
        configs,
        clients_per_region,
        workloads,
        skip,
        progress,
    )
    .await
    /*
    local_bench(
        regions,
        shard_count,
        planet,
        configs,
        clients_per_region,
        workloads,
        skip,
        progress,
    )
    .await
    aws_bench(
        regions,
        shard_count,
        configs,
        clients_per_region,
        workloads,
        skip,
        progress,
    ).await
    */
}

#[allow(dead_code)]
async fn local_bench(
    regions: Vec<Region>,
    shard_count: usize,
    planet: Option<Planet>,
    configs: Vec<(Protocol, Config)>,
    clients_per_region: Vec<usize>,
    workloads: Vec<Workload>,
    skip: impl Fn(Protocol, Config, usize) -> bool,
    progress: TracingProgressBar,
) -> Result<(), Report>
where
{
    // setup baremetal machines
    let machines = fantoch_exp::testbed::local::setup(
        regions,
        shard_count,
        BRANCH.to_string(),
        RUN_MODE,
        FEATURES.to_vec(),
    )
    .await
    .wrap_err("local spawn")?;

    // run benchmarks
    run_bench(
        machines,
        Testbed::Local,
        planet,
        configs,
        clients_per_region,
        workloads,
        skip,
        progress,
    )
    .await
    .wrap_err("run bench")?;

    Ok(())
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
    progress: TracingProgressBar,
) -> Result<(), Report>
where
{
    // create launcher
    let mut launchers = fantoch_exp::testbed::baremetal::create_launchers(
        &regions,
        shard_count,
    );

    // setup baremetal machines
    let machines = fantoch_exp::testbed::baremetal::setup(
        &mut launchers,
        regions,
        shard_count,
        BRANCH.to_string(),
        RUN_MODE,
        FEATURES.to_vec(),
    )
    .await
    .wrap_err("baremetal spawn")?;

    // run benchmarks
    run_bench(
        machines,
        Testbed::Baremetal,
        planet,
        configs,
        clients_per_region,
        workloads,
        skip,
        progress,
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
    progress: TracingProgressBar,
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
        progress,
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
    progress: TracingProgressBar,
) -> Result<(), Report> {
    // setup aws machines
    let machines = fantoch_exp::testbed::aws::setup(
        launcher,
        LAUCH_MODE,
        regions,
        shard_count,
        SERVER_INSTANCE_TYPE.to_string(),
        CLIENT_INSTANCE_TYPE.to_string(),
        MAX_SPOT_INSTANCE_REQUEST_WAIT_SECS,
        BRANCH.to_string(),
        RUN_MODE,
        FEATURES.to_vec(),
    )
    .await
    .wrap_err("aws spawn")?;

    // no need for aws planet
    let planet = None;

    // run benchmarks
    run_bench(
        machines,
        Testbed::Aws,
        planet,
        configs,
        clients_per_region,
        workloads,
        skip,
        progress,
    )
    .await
    .wrap_err("run bench")?;

    Ok(())
}

async fn run_bench(
    machines: Machines<'_>,
    testbed: Testbed,
    planet: Option<Planet>,
    configs: Vec<(Protocol, Config)>,
    clients_per_region: Vec<usize>,
    workloads: Vec<Workload>,
    skip: impl Fn(Protocol, Config, usize) -> bool,
    progress: TracingProgressBar,
) -> Result<(), Report> {
    fantoch_exp::bench::bench_experiment(
        machines,
        RUN_MODE,
        FEATURES.to_vec(),
        testbed,
        planet,
        configs,
        TRACER_SHOW_INTERVAL,
        clients_per_region,
        workloads,
        CPUS,
        skip,
        EXPERIMENT_TIMEOUTS,
        PROTOCOLS_TO_CLEANUP.to_vec(),
        progress,
        RESULTS_DIR,
    )
    .await
}
