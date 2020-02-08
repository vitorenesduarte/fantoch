use clap::{App, Arg};
use planet_sim::config::Config;
use planet_sim::id::ProcessId;
use std::net::IpAddr;

const LIST_SEP: &str = ",";
const DEFAULT_IP: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 3000;
const DEFAULT_CLIENT_PORT: u16 = 4000;
const DEFAULT_TCP_NODELAY: bool = true;
const DEFAULT_WORKERS: usize = 1;
const DEFAULT_EXECUTORS: usize = 1;
const DEFAULT_MULTIPLEXING: usize = 1;

pub fn parse_args() -> (
    ProcessId,
    Vec<ProcessId>,
    IpAddr,
    u16,
    u16,
    Vec<String>,
    Config,
    bool,
    usize,
    Option<usize>,
    usize,
    usize,
    Option<String>,
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
                .help("comma-separated list of process identifiers sorted by distance")
                .required(true)
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
                .help("comma-separated list of addresses to connect to")
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
                .help("TCP flush interval (in microseconds); if 0, then flush occurs on every send; default: 0")
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

    // update config:
    // - set leader if we have one
    // - set the number of workers and executors
    if let Some(leader) = leader {
        config.set_leader(leader);
    }
    config.set_workers(workers);
    config.set_executors(executors);

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

    // check that the number of sorted processes equals `n`
    assert_eq!(sorted_processes.len(), config.n());

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
    )
}

fn parse_process_id(id: Option<&str>) -> ProcessId {
    parse_id(id.expect("process id should be set"))
}

fn parse_leader(leader: Option<&str>) -> Option<ProcessId> {
    leader.map(|leader| parse_id(leader))
}

fn parse_sorted_processes(ids: Option<&str>) -> Vec<ProcessId> {
    ids.expect("sorted processes should be set")
        .split(LIST_SEP)
        .map(|id| parse_id(id))
        .collect()
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

fn parse_addresses(addresses: Option<&str>) -> Vec<String> {
    addresses
        .expect("addresses should be set")
        .split(LIST_SEP)
        .map(|address| address.to_string())
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
