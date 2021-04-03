use clap::{App, Arg};
use color_eyre::Report;
use fantoch::config::Config;
use fantoch::id::{ProcessId, ShardId};
use fantoch::info;
use fantoch::protocol::Protocol;
use std::net::IpAddr;
use std::time::Duration;

pub const LIST_SEP: &str = ",";

const DEFAULT_SHARDS: usize = 1;
const DEFAULT_SHARD_ID: ShardId = 0;

const DEFAULT_IP: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 3000;
const DEFAULT_CLIENT_PORT: u16 = 4000;

const DEFAULT_EXECUTE_AT_COMMIT: bool = false;
const DEFAULT_EXECUTOR_CLEANUP_INTERVAL: Duration = Duration::from_millis(5);

const DEFAULT_WORKERS: usize = 1;
const DEFAULT_EXECUTORS: usize = 1;
const DEFAULT_MULTIPLEXING: usize = 1;

// tempo's config
const DEFAULT_TEMPO_TINY_QUORUMS: bool = false;
const DEFAULT_TEMPO_DETACHED_SEND_INTERVAL: Duration = Duration::from_millis(5);

// protocol's config
const DEFAULT_SKIP_FAST_ACK: bool = false;

#[global_allocator]
#[cfg(feature = "jemalloc")]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

type ProtocolArgs = (
    ProcessId,
    ShardId,
    Option<Vec<(ProcessId, ShardId)>>,
    IpAddr,
    u16,
    u16,
    Vec<(String, Option<Duration>)>,
    Config,
    bool,
    usize,
    Option<Duration>,
    usize,
    usize,
    usize,
    usize,
    usize,
    Option<String>,
    Option<Duration>,
    Option<String>,
    usize,
    Option<usize>,
);

#[allow(dead_code)]
pub fn run<P>() -> Result<(), Report>
where
    P: Protocol + Send + 'static,
{
    let (args, _guard) = parse_args();
    let (
        process_id,
        shard_id,
        sorted_processes,
        ip,
        port,
        client_port,
        addresses,
        config,
        tcp_nodelay,
        tcp_buffer_size,
        tcp_flush_interval,
        process_channel_buffer_size,
        client_channel_buffer_size,
        workers,
        executors,
        multiplexing,
        execution_log,
        ping_interval,
        metrics_file,
        stack_size,
        cpus,
    ) = args;

    let process = fantoch::run::process::<P, String>(
        process_id,
        shard_id,
        sorted_processes,
        ip,
        port,
        client_port,
        addresses,
        config,
        tcp_nodelay,
        tcp_buffer_size,
        tcp_flush_interval,
        process_channel_buffer_size,
        client_channel_buffer_size,
        workers,
        executors,
        multiplexing,
        execution_log,
        ping_interval,
        metrics_file,
    );

    super::tokio_runtime(stack_size, cpus).block_on(process)
}

fn parse_args() -> (ProtocolArgs, tracing_appender::non_blocking::WorkerGuard) {
    let matches = App::new("process")
        .version("0.1")
        .author("Vitor Enes <vitorenesduarte@gmail.com>")
        .about("Runs an instance of some protocol.")
        .arg(
            Arg::with_name("id")
                .long("id")
                .value_name("ID")
                .help("process identifier")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("shard_id")
                .long("shard_id")
                .value_name("SHARD_ID")
                .help("shard identifier; default: 0")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("sorted_processes")
                .long("sorted")
                .value_name("SORTED_PROCESSES")
                .help("comma-separated list of 'ID-SHARD_ID', where ID is the process id and SHARD-ID the identifier of the shard it belongs to, sorted by distance; if not set, processes will ping each other and try to figure out this list from ping latency; for this, 'ping_interval' should be set")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("ip")
                .long("ip")
                .value_name("IP")
                .help("ip to bind to; default: 127.0.0.1")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("port")
                .long("port")
                .value_name("PORT")
                .help("port to bind to; default: 3000")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("client_port")
                .long("client_port")
                .value_name("CLIENT_PORT")
                .help("client port to bind to; default: 4000")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("addresses")
                .long("addresses")
                .value_name("ADDRESSES")
                .help("comma-separated list of addresses to connect to; if a delay (in milliseconds) is to be injected, the address should be of the form IP:PORT-DELAY; for example, 127.0.0.1:3000-120 injects a delay of 120 milliseconds before sending a message to the process at the 127.0.0.1:3000 address")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("n")
                .long("processes")
                .value_name("PROCESS_NUMBER")
                .help("number of processes")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("f")
                .long("faults")
                .value_name("FAULT_NUMBER")
                .help("number of allowed faults")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("shard_count")
                .long("shard_count")
                .value_name("SHARDS_COUNT")
                .help("number of shards; default: 1")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("execute_at_commit")
                .long("execute_at_commit")
                .value_name("EXECUTE_AT_COMMIT")
                .help("bool indicating whether execution should be skipped; default: false")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("executor_cleanup_interval")
                .long("executor_cleanup_interval")
                .value_name("EXECUTOR_CLEANUP_INTERVAL")
                .help("executor cleanup interval (in milliseconds); default: 5")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("executor_monitor_pending_interval")
                .long("executor_monitor_pending_interval")
                .value_name("EXECUTOR_MONITOR_PENDING_INTERVAL")
                .help("executor monitor pending interval (in milliseconds); if no value if set, pending commands are not monitored")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("gc_interval")
                .long("gc_interval")
                .value_name("GC_INTERVAL")
                .help("garbage collection interval (in milliseconds); if no value if set, stability doesn't run and commands are deleted at commit time")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("leader")
                .long("leader")
                .value_name("LEADER")
                .help("id of the starting leader process in leader-based protocols")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("tempo_tiny_quorums")
                .long("tempo_tiny_quorums")
                .value_name("TEMPO_TINY_QUORUMS")
                .help("boolean indicating whether tempo's tiny quorums are enabled; default: false")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("tempo_clock_bump_interval")
                .long("tempo_clock_bump_interval")
                .value_name("TEMPO_CLOCK_BUMP_INTERVAL")
                .help("number indicating the interval (in milliseconds) between clock bumps; if this value is not set, then clocks are not bumped periodically")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("tempo_detached_send_interval")
                .long("tempo_detached_send_interval")
                .value_name("TEMPO_DETACHED_SEND_INTERVAL")
                .help("number indicating the interval (in milliseconds) between mdetached messages are sent; default: 5")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("skip_fast_ack")
                .long("skip_fast_ack")
                .value_name("SKIP_FAST_ACK")
                .help("boolean indicating whether protocols should try to enable the skip fast ack optimization; default: false")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("tcp_nodelay")
                .long("tcp_nodelay")
                .value_name("TCP_NODELAY")
                .help("TCP_NODELAY; default: true")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("tcp_buffer_size")
                .long("tcp_buffer_size")
                .value_name("TCP_BUFFER_SIZE")
                .help("size of the TCP buffer; default: 8192 (bytes)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("tcp_flush_interval")
                .long("tcp_flush_interval")
                .value_name("TCP_FLUSH_INTERVAL")
                .help("TCP flush interval (in milliseconds); if 0, then flush occurs on every send; default: 0")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("process_channel_buffer_size")
                .long("process_channel_buffer_size")
                .value_name("PROCESS_CHANNEL_BUFFER_SIZE")
                .help(
                    "size of the buffer in each channel used for task communication related to the processes; default: 100",
                )
                .takes_value(true),
        )
        .arg(
            Arg::with_name("client_channel_buffer_size")
                .long("client_channel_buffer_size")
                .value_name("CLIENT_CHANNEL_BUFFER_SIZE")
                .help(
                    "size of the buffer in each channel used for task communication related to the clients; default: 100",
                )
                .takes_value(true),
        )
        .arg(
            Arg::with_name("workers")
                .long("workers")
                .value_name("WORKERS")
                .help("number of protocol workers; default: 1")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("executors")
                .long("executors")
                .value_name("EXECUTORS")
                .help("number of executors; default: 1")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("multiplexing")
                .long("multiplexing")
                .value_name("MULTIPLEXING")
                .help("number of connections between replicas; default: 1")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("execution_log")
                .long("execution_log")
                .value_name("EXECUTION_LOG")
                .help("log file in which execution info should be written to; by default this information is not logged")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("ping_interval")
                .long("ping_interval")
                .value_name("PING_INTERVAL")
                .help("number indicating the interval (in milliseconds) between pings between processes; by default there's no pinging; if set, this value should be > 0")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("metrics_file")
                .long("metrics_file")
                .value_name("METRICS_FILE")
                .help("file in which metrics are (periodically, every 5s) written to; by default metrics are not logged")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("stack_size")
                .long("stack_size")
                .value_name("STACK_SIZE")
                .help("stack size (in bytes) of each tokio thread; default: 2 * 1024 * 1024 (bytes)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("cpus")
                .long("cpus")
                .value_name("CPUS")
                .help("number of cpus to be used by tokio; by default all available cpus are used")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("log_file")
                .long("log_file")
                .value_name("LOG_FILE")
                .help("file to which logs will be written to; if not set, logs will be redirect to the stdout")
                .takes_value(true),
        )
        .get_matches();

    let tracing_directives = None;
    let guard = fantoch::util::init_tracing_subscriber(
        matches.value_of("log_file"),
        tracing_directives,
    );

    // parse arguments
    let process_id = parse_process_id(matches.value_of("id"));
    let shard_id = parse_shard_id(matches.value_of("shard_id"));
    let sorted_processes =
        parse_sorted_processes(matches.value_of("sorted_processes"));
    let ip = parse_ip(matches.value_of("ip"));
    let port = parse_port(matches.value_of("port"));
    let client_port = parse_client_port(matches.value_of("client_port"));
    let addresses = parse_addresses(matches.value_of("addresses"));

    // parse config
    let config = build_config(
        parse_n(matches.value_of("n")),
        parse_f(matches.value_of("f")),
        parse_shard_count(matches.value_of("shard_count")),
        parse_execute_at_commit(matches.value_of("execute_at_commit")),
        parse_executor_cleanup_interval(
            matches.value_of("executor_cleanup_interval"),
        ),
        parse_executor_monitor_pending_interval(
            matches.value_of("executor_monitor_pending_interval"),
        ),
        parse_gc_interval(matches.value_of("gc_interval")),
        parse_leader(matches.value_of("leader")),
        parse_tempo_tiny_quorums(matches.value_of("tempo_tiny_quorums")),
        parse_tempo_clock_bump_interval(
            matches.value_of("tempo_clock_bump_interval"),
        ),
        parse_tempo_detached_send_interval(
            matches.value_of("tempo_detached_send_interval"),
        ),
        parse_skip_fast_ack(matches.value_of("skip_fast_ack")),
    );

    let tcp_nodelay = super::parse_tcp_nodelay(matches.value_of("tcp_nodelay"));
    let tcp_buffer_size =
        super::parse_tcp_buffer_size(matches.value_of("tcp_buffer_size"));
    let tcp_flush_interval =
        super::parse_tcp_flush_interval(matches.value_of("tcp_flush_interval"));

    let process_channel_buffer_size = super::parse_channel_buffer_size(
        matches.value_of("process_channel_buffer_size"),
    );
    let client_channel_buffer_size = super::parse_channel_buffer_size(
        matches.value_of("client_channel_buffer_size"),
    );
    let workers = parse_workers(matches.value_of("workers"));
    let executors = parse_executors(matches.value_of("executors"));
    let multiplexing = parse_multiplexing(matches.value_of("multiplexing"));
    let execution_log = parse_execution_log(matches.value_of("execution_log"));
    let ping_interval = parse_ping_interval(matches.value_of("ping_interval"));
    let metrics_file = parse_metrics_file(matches.value_of("metrics_file"));
    let stack_size = super::parse_stack_size(matches.value_of("stack_size"));
    let cpus = super::parse_cpus(matches.value_of("cpus"));

    info!("process id: {}", process_id);
    info!("sorted processes: {:?}", sorted_processes);
    info!("ip: {:?}", ip);
    info!("port: {}", port);
    info!("client port: {}", client_port);
    info!("addresses: {:?}", addresses);
    info!("config: {:?}", config);
    info!("tcp_nodelay: {:?}", tcp_nodelay);
    info!("tcp buffer size: {:?}", tcp_buffer_size);
    info!("tcp flush interval: {:?}", tcp_flush_interval);
    info!(
        "process channel buffer size: {:?}",
        process_channel_buffer_size
    );
    info!(
        "client channel buffer size: {:?}",
        client_channel_buffer_size
    );
    info!("workers: {:?}", workers);
    info!("executors: {:?}", executors);
    info!("multiplexing: {:?}", multiplexing);
    info!("execution log: {:?}", execution_log);
    info!("ping_interval: {:?}", ping_interval);
    info!("metrics file: {:?}", metrics_file);
    info!("stack size: {:?}", stack_size);

    let args = (
        process_id,
        shard_id,
        sorted_processes,
        ip,
        port,
        client_port,
        addresses,
        config,
        tcp_nodelay,
        tcp_buffer_size,
        tcp_flush_interval,
        process_channel_buffer_size,
        client_channel_buffer_size,
        workers,
        executors,
        multiplexing,
        execution_log,
        ping_interval,
        metrics_file,
        stack_size,
        cpus,
    );
    (args, guard)
}

fn parse_process_id(id: Option<&str>) -> ProcessId {
    parse_id::<ProcessId>(id.expect("process id should be set"))
}

fn parse_shard_id(shard_id: Option<&str>) -> ShardId {
    shard_id
        .map(|id| parse_id::<ShardId>(id))
        .unwrap_or(DEFAULT_SHARD_ID)
}

fn parse_id<I>(id: &str) -> I
where
    I: std::str::FromStr,
    <I as std::str::FromStr>::Err: std::fmt::Debug,
{
    id.parse::<I>().expect("id should be a number")
}

fn parse_sorted_processes(
    ids: Option<&str>,
) -> Option<Vec<(ProcessId, ShardId)>> {
    ids.map(|ids| {
        ids.split(LIST_SEP)
            .map(|entry| {
                let parts: Vec<_> = entry.split('-').collect();
                assert_eq!(parts.len(), 2, "each sorted process entry should have the form 'ID-SHARD_ID'");
                let id = parse_id::<ProcessId>(parts[0]);
                let shard_id = parse_id::<ShardId>(parts[1]);
                (id, shard_id)
            })
            .collect()
    })
}

fn parse_ip(ip: Option<&str>) -> IpAddr {
    ip.unwrap_or(DEFAULT_IP)
        .parse::<IpAddr>()
        .expect("ip should be a valid ip address")
}

fn parse_port(port: Option<&str>) -> u16 {
    port.map(|port| port.parse::<u16>().expect("port should be a number"))
        .unwrap_or(DEFAULT_PORT)
}

fn parse_client_port(port: Option<&str>) -> u16 {
    port.map(|port| {
        port.parse::<u16>().expect("client port should be a number")
    })
    .unwrap_or(DEFAULT_CLIENT_PORT)
}

fn parse_addresses(addresses: Option<&str>) -> Vec<(String, Option<Duration>)> {
    addresses
        .expect("addresses should be set")
        .split(LIST_SEP)
        .map(|address| {
            let parts: Vec<_> = address.split('-').collect();
            let address = parts[0].to_string();
            match parts.len() {
                1 => {
                    // in this case, no delay was set
                    (address, None)
                }
                2 => {
                    let millis = parts[1]
                        .parse::<u64>()
                        .expect("address delay should be a number");
                    let delay = Duration::from_millis(millis);
                    (address, Some(delay))
                }
                _ => {
                    panic!("invalid address: {:?}", address);
                }
            }
        })
        .collect()
}

pub fn build_config(
    n: usize,
    f: usize,
    shard_count: usize,
    execute_at_commit: bool,
    executor_cleanup_interval: Duration,
    executor_monitor_pending_interval: Option<Duration>,
    gc_interval: Option<Duration>,
    leader: Option<ProcessId>,
    tempo_tiny_quorums: bool,
    tempo_clock_bump_interval: Option<Duration>,
    tempo_detached_send_interval: Duration,
    skip_fast_ack: bool,
) -> Config {
    // create config
    let mut config = Config::new(n, f);
    config.set_shard_count(shard_count);
    config.set_execute_at_commit(execute_at_commit);
    config.set_executor_cleanup_interval(executor_cleanup_interval);
    if let Some(interval) = executor_monitor_pending_interval {
        config.set_executor_monitor_pending_interval(interval);
    }
    if let Some(interval) = gc_interval {
        config.set_gc_interval(interval);
    }
    // set leader if we have one
    if let Some(leader) = leader {
        config.set_leader(leader);
    }
    // set tempo's config
    config.set_tempo_tiny_quorums(tempo_tiny_quorums);
    if let Some(interval) = tempo_clock_bump_interval {
        config.set_tempo_clock_bump_interval(interval);
    }
    config.set_tempo_detached_send_interval(tempo_detached_send_interval);
    // set protocol's config
    config.set_skip_fast_ack(skip_fast_ack);
    config
}

pub fn parse_n(n: Option<&str>) -> usize {
    n.expect("n should be set")
        .parse::<usize>()
        .expect("n should be a number")
}

pub fn parse_f(f: Option<&str>) -> usize {
    f.expect("f should be set")
        .parse::<usize>()
        .expect("f should be a number")
}

pub fn parse_shard_count(shards: Option<&str>) -> usize {
    shards
        .map(|shards| {
            shards.parse::<usize>().expect("shards should be a number")
        })
        .unwrap_or(DEFAULT_SHARDS)
}

pub fn parse_execute_at_commit(execute_at_commit: Option<&str>) -> bool {
    execute_at_commit
        .map(|execute_at_commit| {
            execute_at_commit
                .parse::<bool>()
                .expect("execute_at_commit should be a bool")
        })
        .unwrap_or(DEFAULT_EXECUTE_AT_COMMIT)
}

pub fn parse_executor_cleanup_interval(interval: Option<&str>) -> Duration {
    interval
        .map(|interval| {
            let ms = interval
                .parse::<u64>()
                .expect("executor_cleanup_interval should be a number");
            Duration::from_millis(ms)
        })
        .unwrap_or(DEFAULT_EXECUTOR_CLEANUP_INTERVAL)
}

pub fn parse_executor_monitor_pending_interval(
    interval: Option<&str>,
) -> Option<Duration> {
    interval.map(|interval| {
        let ms = interval
            .parse::<u64>()
            .expect("executor_monitor_pending_interval should be a number");
        Duration::from_millis(ms)
    })
}

pub fn parse_gc_interval(gc_interval: Option<&str>) -> Option<Duration> {
    gc_interval.map(|gc_interval| {
        let ms = gc_interval
            .parse::<u64>()
            .expect("gc_interval should be a number");
        Duration::from_millis(ms)
    })
}

fn parse_leader(leader: Option<&str>) -> Option<ProcessId> {
    leader.map(|leader| parse_id(leader))
}

fn parse_tempo_tiny_quorums(tempo_tiny_quorums: Option<&str>) -> bool {
    tempo_tiny_quorums
        .map(|tempo_tiny_quorums| {
            tempo_tiny_quorums
                .parse::<bool>()
                .expect("tempo_tiny_quorums should be a bool")
        })
        .unwrap_or(DEFAULT_TEMPO_TINY_QUORUMS)
}

fn parse_tempo_clock_bump_interval(interval: Option<&str>) -> Option<Duration> {
    interval.map(|interval| {
        let ms = interval
            .parse::<u64>()
            .expect("tempo_clock_bump_interval should be a number");
        Duration::from_millis(ms)
    })
}

fn parse_tempo_detached_send_interval(interval: Option<&str>) -> Duration {
    interval
        .map(|interval| {
            let ms = interval
                .parse::<u64>()
                .expect("tempo_detached_send_interval should be a number");
            Duration::from_millis(ms)
        })
        .unwrap_or(DEFAULT_TEMPO_DETACHED_SEND_INTERVAL)
}

pub fn parse_skip_fast_ack(skip_fast_ack: Option<&str>) -> bool {
    skip_fast_ack
        .map(|skip_fast_ack| {
            skip_fast_ack
                .parse::<bool>()
                .expect("skip_fast_ack should be a boolean")
        })
        .unwrap_or(DEFAULT_SKIP_FAST_ACK)
}

fn parse_workers(workers: Option<&str>) -> usize {
    workers
        .map(|workers| {
            workers
                .parse::<usize>()
                .expect("workers should be a number")
        })
        .unwrap_or(DEFAULT_WORKERS)
}

fn parse_executors(executors: Option<&str>) -> usize {
    executors
        .map(|executors| {
            executors
                .parse::<usize>()
                .expect("executors should be a number")
        })
        .unwrap_or(DEFAULT_EXECUTORS)
}

fn parse_multiplexing(multiplexing: Option<&str>) -> usize {
    multiplexing
        .map(|multiplexing| {
            multiplexing
                .parse::<usize>()
                .expect("multiplexing should be a number")
        })
        .unwrap_or(DEFAULT_MULTIPLEXING)
}

pub fn parse_execution_log(execution_log: Option<&str>) -> Option<String> {
    execution_log.map(String::from)
}

fn parse_ping_interval(interval: Option<&str>) -> Option<Duration> {
    interval.map(|interval| {
        let millis = interval
            .parse::<u64>()
            .expect("ping_interval should be a number");
        Duration::from_millis(millis)
    })
}

pub fn parse_metrics_file(metrics_file: Option<&str>) -> Option<String> {
    metrics_file.map(String::from)
}
