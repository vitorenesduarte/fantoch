use clap::{App, Arg};
use fantoch::config::Config;
use fantoch::id::ProcessId;
use fantoch::protocol::Protocol;
use std::error::Error;
use std::net::IpAddr;

const LIST_SEP: &str = ",";
const DEFAULT_IP: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 3000;
const DEFAULT_CLIENT_PORT: u16 = 4000;
const DEFAULT_TCP_NODELAY: bool = true;
const DEFAULT_WORKERS: usize = 1;
const DEFAULT_EXECUTORS: usize = 1;
const DEFAULT_MULTIPLEXING: usize = 1;

// newt's config
const DEFAULT_NEWT_TINY_QUORUMS: bool = false;
const DEFAULT_NEWT_REAL_TIME: bool = false;
const DEFAULT_NEWT_CLOCK_BUMP_INTERVAL: usize = 10;

// protocol's config
const DEFAULT_SKIP_FAST_ACK: bool = false;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[allow(dead_code)]
pub fn run<P>() -> Result<(), Box<dyn Error>>
where
    P: Protocol + Send + 'static,
{
    let (
        process_id,
        sorted_processes,
        ip,
        port,
        client_port,
        addresses,
        config,
        tcp_nodelay,
        tcp_buffer_size,
        tcp_flush_interval,
        channel_buffer_size,
        multiplexing,
        execution_log,
        tracer_show_interval,
        ping_interval,
    ) = parse_args();

    let process = fantoch::run::process::<P, String>(
        process_id,
        sorted_processes,
        ip,
        port,
        client_port,
        addresses,
        config,
        tcp_nodelay,
        tcp_buffer_size,
        tcp_flush_interval,
        channel_buffer_size,
        multiplexing,
        execution_log,
        tracer_show_interval,
        ping_interval,
    );
    super::tokio_runtime().block_on(process)
}

fn parse_args() -> (
    ProcessId,
    Option<Vec<ProcessId>>,
    IpAddr,
    u16,
    u16,
    Vec<(String, Option<usize>)>,
    Config,
    bool,
    usize,
    Option<usize>,
    usize,
    usize,
    Option<String>,
    Option<usize>,
    Option<usize>,
) {
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
            Arg::with_name("sorted_processes")
                .long("sorted")
                .value_name("SORTED_PROCESSES")
                .help("comma-separated list of process identifiers sorted by distance; if not set, processes will ping each other and try to figure out this list from ping latency; for this, 'ping_interval' should be set")
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
                .value_name("ADDR")
                .help("comma-separated list of addresses to connect to; if a delay (in milliseconds) is to be injected, the address should be of the form ADDRESS-DELAY; for example, 127.0.0.1:300-120 injects a delay of 120 milliseconds before sending a message to the process at the 127.0.0.1:3000 address")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("n")
                .long("processes")
                .value_name("PROCESS_NUMBER")
                .help("total number of processes")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("f")
                .long("faults")
                .value_name("FAULT_NUMBER")
                .help("total number of allowed faults")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("transitive_conflicts")
                .long("transitive_conflicts")
                .value_name("TRANSITIVE_CONFLICTS")
                .help("bool indicating whether we can assume that the conflict relation is transitive; default: false")
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
            Arg::with_name("leader")
                .long("leader")
                .value_name("LEADER")
                .help("id of the starting leader process in leader-based protocols")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("tcp_nodelay")
                .long("tcp_nodelay")
                .value_name("TCP_NODELAY")
                .help("TCP_NODELAY; defaul: true")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("tcp_buffer_size")
                .long("tcp_buffer_size")
                .value_name("TCP_BUFFER_SIZE")
                .help("size of the TCP buffer; default: 8192 (8KBs)")
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
            Arg::with_name("gc_interval")
                .long("gc_interval")
                .value_name("GC_INTERVAL")
                .help("garbage collection interval (in milliseconds); if no value if set, stability doesn't run and commands are deleted at commit time")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("channel_buffer_size")
                .long("channel_buffer_size")
                .value_name("CHANNEL_BUFFER_SIZE")
                .help(
                    "size of the buffer in each channel used for task communication; default: 100",
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
            Arg::with_name("tracer_show_interval")
                .long("tracer_show_interval")
                .value_name("TRACER_SHOW_INTERVAL")
                .help("number indicating the interval (in milliseconds) between tracing information being show; by default there's no tracing; if set, this value should be > 0")
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
            Arg::with_name("newt_tiny_quorums")
                .long("newt_tiny_quorums")
                .value_name("NEWT_TINY_QUORUMS")
                .help("boolean indicating whether newt's tiny quorums are enabled; default: false")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("newt_real_time")
                .long("newt_real_time")
                .value_name("NEWT_REAL_TIME")
                .help("boolean indicating whether newt should use real time to bump its clocks; default: false")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("newt_clock_bump_interval")
                .long("newt_clock_bump_interval")
                .value_name("NEWT_CLOCK_BUMP_INTERVAL")
                .help("number indicating the interval (in milliseconds) between clock bump; this value is only used if 'newt_real_time = true'; default: 10")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("skip_fast_ack")
                .long("skip_fast_ack")
                .value_name("SKIP_FAST_ACK")
                .help("boolean indicating whether protocols should try to enable the skip fast ack optimization; default: false")
                .takes_value(true),
        )
        .get_matches();

    // parse arguments
    let process_id = parse_process_id(matches.value_of("id"));
    let sorted_processes =
        parse_sorted_processes(matches.value_of("sorted_processes"));
    let ip = parse_ip(matches.value_of("ip"));
    let port = parse_port(matches.value_of("port"));
    let client_port = parse_client_port(matches.value_of("client_port"));
    let addresses = parse_addresses(matches.value_of("addresses"));
    let mut config = super::parse_config(
        matches.value_of("n"),
        matches.value_of("f"),
        matches.value_of("transitive_conflicts"),
        matches.value_of("execute_at_commit"),
        matches.value_of("gc_interval"),
    );
    let leader = parse_leader(matches.value_of("leader"));
    let tcp_nodelay = super::parse_tcp_nodelay(matches.value_of("tcp_nodelay"));
    let tcp_buffer_size =
        super::parse_tcp_buffer_size(matches.value_of("tcp_buffer_size"));
    let tcp_flush_interval =
        super::parse_tcp_flush_interval(matches.value_of("tcp_flush_interval"));

    let channel_buffer_size = super::parse_channel_buffer_size(
        matches.value_of("channel_buffer_size"),
    );
    let workers = parse_workers(matches.value_of("workers"));
    let executors = parse_executors(matches.value_of("executors"));
    let multiplexing = parse_multiplexing(matches.value_of("multiplexing"));
    let execution_log =
        super::parse_execution_log(matches.value_of("execution_log"));
    let tracer_show_interval =
        parse_tracer_show_interval(matches.value_of("tracer_show_interval"));
    let ping_interval = parse_ping_interval(matches.value_of("ping_interval"));
    let (newt_tiny_quorums, newt_real_time, newt_clock_bump_interval) =
        parse_newt_config(
            matches.value_of("newt_tiny_quorums"),
            matches.value_of("newt_real_time"),
            matches.value_of("newt_clock_bump_interval"),
        );
    let skip_fast_ack = parse_skip_fast_ack(matches.value_of("skip_fast_ack"));

    // update config:
    // - set leader if we have one
    // - set the number of workers and executors
    if let Some(leader) = leader {
        config.set_leader(leader);
    }
    config.set_workers(workers);
    config.set_executors(executors);

    // set newt's config
    config.set_newt_tiny_quorums(newt_tiny_quorums);
    config.set_newt_real_time(newt_real_time);
    config.set_newt_clock_bump_interval(newt_clock_bump_interval);

    // set protocol's config
    config.set_skip_fast_ack(skip_fast_ack);

    println!("process id: {}", process_id);
    println!("sorted processes: {:?}", sorted_processes);
    println!("ip: {:?}", ip);
    println!("port: {}", port);
    println!("client port: {}", client_port);
    println!("addresses: {:?}", addresses);
    println!("config: {:?}", config);
    println!("tcp_nodelay: {:?}", tcp_nodelay);
    println!("tcp buffer size: {:?}", tcp_buffer_size);
    println!("tcp flush interval: {:?}", tcp_flush_interval);
    println!("channel buffer size: {:?}", channel_buffer_size);
    println!("multiplexing: {:?}", multiplexing);
    println!("execution log: {:?}", execution_log);
    println!("trace_show_interval: {:?}", tracer_show_interval);
    println!("ping_interval: {:?}", ping_interval);

    // check that the number of sorted processes equals `n` (if it was set)
    if let Some(sorted_processes) = sorted_processes.as_ref() {
        assert_eq!(sorted_processes.len(), config.n());
    }

    // check that the number of addresses equals `n - 1`
    assert_eq!(addresses.len(), config.n() - 1);

    (
        process_id,
        sorted_processes,
        ip,
        port,
        client_port,
        addresses,
        config,
        tcp_nodelay,
        tcp_buffer_size,
        tcp_flush_interval,
        channel_buffer_size,
        multiplexing,
        execution_log,
        tracer_show_interval,
        ping_interval,
    )
}

fn parse_process_id(id: Option<&str>) -> ProcessId {
    parse_id(id.expect("process id should be set"))
}

fn parse_leader(leader: Option<&str>) -> Option<ProcessId> {
    leader.map(|leader| parse_id(leader))
}

fn parse_sorted_processes(ids: Option<&str>) -> Option<Vec<ProcessId>> {
    ids.map(|ids| ids.split(LIST_SEP).map(|id| parse_id(id)).collect())
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

fn parse_addresses(addresses: Option<&str>) -> Vec<(String, Option<usize>)> {
    addresses
        .expect("addresses should be set")
        .split(LIST_SEP)
        .map(|address| {
            let parts: Vec<_> = address.split("-").collect();
            let address = parts[0].to_string();
            match parts.len() {
                1 => {
                    // in this case, no delay was set
                    (address, None)
                }
                2 => {
                    let delay = parts[1]
                        .parse::<usize>()
                        .expect("address delay should be a number");
                    (address, Some(delay))
                }
                _ => {
                    panic!("invalid address: {:?}", address);
                }
            }
        })
        .collect()
}

fn parse_id(id: &str) -> ProcessId {
    id.parse::<ProcessId>()
        .expect("process id should be a number")
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

fn parse_tracer_show_interval(
    tracer_show_interval: Option<&str>,
) -> Option<usize> {
    tracer_show_interval.map(|tracer_show_interval| {
        tracer_show_interval
            .parse::<usize>()
            .expect("tracer_show_interval should be a number")
    })
}

fn parse_ping_interval(ping_interval: Option<&str>) -> Option<usize> {
    ping_interval.map(|ping_interval| {
        ping_interval
            .parse::<usize>()
            .expect("ping_interval should be a number")
    })
}

fn parse_newt_config(
    newt_tiny_quorums: Option<&str>,
    newt_real_time: Option<&str>,
    newt_clock_bump_interval: Option<&str>,
) -> (bool, bool, usize) {
    let newt_tiny_quorums = newt_tiny_quorums
        .map(|newt_tiny_quorums| {
            newt_tiny_quorums
                .parse::<bool>()
                .expect("newt_tiny_quorums should be a bool")
        })
        .unwrap_or(DEFAULT_NEWT_TINY_QUORUMS);
    let newt_real_time = newt_real_time
        .map(|newt_real_time| {
            newt_real_time
                .parse::<bool>()
                .expect("newt_real_time should be a bool")
        })
        .unwrap_or(DEFAULT_NEWT_REAL_TIME);
    let newt_clock_bump_interval = newt_clock_bump_interval
        .map(|newt_clock_bump_interval| {
            newt_clock_bump_interval
                .parse::<usize>()
                .expect("newt_clock_bump_interval should be a number")
        })
        .unwrap_or(DEFAULT_NEWT_CLOCK_BUMP_INTERVAL);
    (newt_tiny_quorums, newt_real_time, newt_clock_bump_interval)
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
