mod common;

use clap::{Command, Arg};
use color_eyre::Report;
use fantoch::client::{KeyGen, Workload};
use fantoch::id::ClientId;
use fantoch::info;
use std::time::Duration;

const RANGE_SEP: &str = "-";
const DEFAULT_KEYS_PER_COMMAND: usize = 1;
const DEFAULT_SHARD_COUNT: usize = 1;
const DEFAULT_KEY_GEN: KeyGen = KeyGen::ConflictPool {
    conflict_rate: 100,
    pool_size: 1,
};
const DEFAULT_COMMANDS_PER_CLIENT: usize = 1000;
const DEFAULT_READ_ONLY_PERCENTAGE: usize = 0;
const DEFAULT_PAYLOAD_SIZE: usize = 100;
const DEFAULT_BATCH_MAX_SIZE: usize = 1;
const DEFAULT_BATCH_MAX_DELAY: Duration = Duration::from_millis(5);

type ClientArgs = (
    Vec<ClientId>,
    Vec<String>,
    Option<Duration>,
    Workload,
    usize,
    Duration,
    bool,
    usize,
    Option<usize>,
    Option<String>,
    usize,
    Option<usize>,
);

fn main() -> Result<(), Report> {
    let (args, _guard) = parse_args();
    let (
        ids,
        addresses,
        interval,
        workload,
        batch_max_size,
        batch_max_delay,
        tcp_nodelay,
        channel_buffer_size,
        status_frequency,
        metrics_file,
        stack_size,
        cpus,
    ) = args;

    common::tokio_runtime(stack_size, cpus).block_on(fantoch::run::client(
        ids,
        addresses,
        interval,
        workload,
        batch_max_size,
        batch_max_delay,
        tcp_nodelay,
        channel_buffer_size,
        status_frequency,
        metrics_file,
    ))
}

fn parse_args() -> (ClientArgs, tracing_appender::non_blocking::WorkerGuard) {
    let matches = Command::new("client")
        .version("0.1")
        .author("Vitor Enes <vitorenesduarte@gmail.com>")
        .about("Runs a client that will connect to some instance of a protocol.")
        .arg(
            Arg::new("ids")
                .long("ids")
                .value_name("ID_RANGE")
                .help("a range of client identifiers represented as START-END; as many client as the number of identifers will be created")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::new("addresses")
                .long("addresses")
                .value_name("ADDRESSES")
                .help("comma-separated list of addresses to connect to (in the form IP:PORT e.g. 127.0.0.1:3000)")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::new("interval")
                .long("interval")
                .value_name("INTERVAL")
                .help("if this value is set, an open-loop client will be created (by default is closed-loop) and the value set is used as the interval (in milliseconds) between submitted commands")
                .takes_value(true),
        )
        .arg(
            Arg::new("shard_count")
                .long("shard_count")
                .value_name("SHARD_COUNT")
                .help("number of shards accessed in the system; default: 1")
                .takes_value(true),
        )
        .arg(
            Arg::new("key_gen")
                .long("key_gen")
                .value_name("KEY_GEN")
                .help("representation of a key generator; possible values 'conflict_pool,100,1' where 100 is the conflict rate and 1 the pool size, or 'zipf,1.3,10000' where 1.3 is the zipf coefficient (which should be non-zero) and 10000 the number of keys (per shard) in the distribution; default: 'conflict_rate,100,1'")
                .takes_value(true),
        )
        .arg(
            Arg::new("keys_per_command")
                .long("keys_per_command")
                .value_name("KEYS_PER_COMMAND")
                .help("number of keys accessed by each command to be issued by each client; default: 1")
                .takes_value(true),
        )
        .arg(
            Arg::new("commands_per_client")
                .long("commands_per_client")
                .value_name("COMMANDS_PER_CLIENT")
                .help("number of commands to be issued by each client; default: 1000")
                .takes_value(true),
        )
        .arg(
            Arg::new("read_only_percentage")
                .long("read_only_percentage")
                .value_name("READ_ONLY_PERCENTAGE")
                .help("percentage of read-only commands; default: 0")
                .takes_value(true),
        )
        .arg(
            Arg::new("payload_size")
                .long("payload_size")
                .value_name("PAYLOAD_SIZE")
                .help("size of the command payload; default: 100 (bytes)")
                .takes_value(true),
        )
        .arg(
            Arg::new("batch_max_size")
                .long("batch_max_size")
                .value_name("BATCH_MAX_SIZE")
                .help("max size of the batch; default: 1 (i.e., no batching)")
                .takes_value(true),
        )
        .arg(
            Arg::new("batch_max_delay")
                .long("batch_max_delay")
                .value_name("BATCH_MAX_DELAY")
                .help("max delay of a batch; default: 5 (milliseconds)")
                .takes_value(true),
        )
        .arg(
            Arg::new("tcp_nodelay")
                .long("tcp_nodelay")
                .value_name("TCP_NODELAY")
                .help("set TCP_NODELAY; default: true")
                .takes_value(true),
        )
        .arg(
            Arg::new("channel_buffer_size")
                .long("channel_buffer_size")
                .value_name("CHANNEL_BUFFER_SIZE")
                .help("set the size of the buffer in each channel used for task communication; default: 10000")
                .takes_value(true),
        )
        .arg(
            Arg::new("status_frequency")
                .long("status_frequency")
                .value_name("STATUS_FREQUENCY")
                .help("frequency of status messages; if set with 1, a status message will be shown for each completed command; default: no status messages are shown")
                .takes_value(true),
        )
        .arg(
            Arg::new("metrics_file")
                .long("metrics_file")
                .value_name("METRICS_FILE")
                .help("file in which metrics are written to; by default metrics are not logged")
                .takes_value(true),
        )
        .arg(
            Arg::new("stack_size")
                .long("stack_size")
                .value_name("STACK_SIZE")
                .help("stack size (in bytes) of each tokio thread; default: 2 * 1024 * 1024 (bytes)")
                .takes_value(true),
        )
        .arg(
            Arg::new("cpus")
                .long("cpus")
                .value_name("CPUS")
                .help("number of cpus to be used by tokio; by default all available cpus are used")
                .takes_value(true),
        )
        .arg(
            Arg::new("log_file")
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
    let ids = parse_id_range(matches.value_of("ids"));
    let addresses = parse_addresses(matches.value_of("addresses"));
    let interval = parse_interval(matches.value_of("interval"));
    let workload = parse_workload(
        matches.value_of("shard_count"),
        matches.value_of("key_gen"),
        matches.value_of("keys_per_command"),
        matches.value_of("commands_per_client"),
        matches.value_of("read_only_percentage"),
        matches.value_of("payload_size"),
    );

    let batch_max_size =
        parse_batch_max_size(matches.value_of("batch_max_size"));
    let batch_max_delay =
        parse_batch_max_delay(matches.value_of("batch_max_delay"));

    let tcp_nodelay =
        common::parse_tcp_nodelay(matches.value_of("tcp_nodelay"));
    let channel_buffer_size = common::parse_channel_buffer_size(
        matches.value_of("channel_buffer_size"),
    );
    let status_frequency =
        parse_status_frequency(matches.value_of("status_frequency"));
    let metrics_file = parse_metrics_file(matches.value_of("metrics_file"));
    let stack_size = common::parse_stack_size(matches.value_of("stack_size"));
    let cpus = common::parse_cpus(matches.value_of("cpus"));

    info!("ids: {}-{}", ids.first().unwrap(), ids.last().unwrap());
    info!("client number: {}", ids.len());
    info!("addresses: {:?}", addresses);
    info!("workload: {:?}", workload);
    info!("batch_max_size: {:?}", batch_max_size);
    info!("batch_max_delay: {:?}", batch_max_delay);
    info!("tcp_nodelay: {:?}", tcp_nodelay);
    info!("channel buffer size: {:?}", channel_buffer_size);
    info!("status frequency: {:?}", status_frequency);
    info!("metrics file: {:?}", metrics_file);
    info!("stack size: {:?}", stack_size);

    let args = (
        ids,
        addresses,
        interval,
        workload,
        batch_max_size,
        batch_max_delay,
        tcp_nodelay,
        channel_buffer_size,
        status_frequency,
        metrics_file,
        stack_size,
        cpus,
    );
    (args, guard)
}

fn parse_id_range(id_range: Option<&str>) -> Vec<ClientId> {
    let bounds: Vec<_> = id_range
        .expect("id range should be set")
        .split(RANGE_SEP)
        .map(|bound| {
            bound
                .parse::<ClientId>()
                .expect("range bound should be a number")
        })
        .collect();
    // check that we only have two bounds: start and end
    if bounds.len() == 2 {
        let start = bounds[0];
        let end = bounds[1];
        (start..=end).collect()
    } else {
        panic!("invalid id range (there should only be a lower bound and an uppper bound)")
    }
}

fn parse_addresses(addresses: Option<&str>) -> Vec<String> {
    addresses
        .expect("addresses should be set")
        .split(common::protocol::LIST_SEP)
        .map(|address| address.to_string())
        .collect()
}

fn parse_millis_duration(millis: Option<&str>) -> Option<Duration> {
    millis.map(|millis| {
        let millis = millis.parse::<u64>().expect("millis should be a number");
        Duration::from_millis(millis)
    })
}

fn parse_interval(interval: Option<&str>) -> Option<Duration> {
    parse_millis_duration(interval)
}

fn parse_workload(
    shard_count: Option<&str>,
    key_gen: Option<&str>,
    keys_per_command: Option<&str>,
    commands_per_client: Option<&str>,
    read_only_percentage: Option<&str>,
    payload_size: Option<&str>,
) -> Workload {
    let shard_count = parse_shard_count(shard_count);
    let key_gen = parse_key_gen(key_gen);
    let keys_per_command = parse_keys_per_command(keys_per_command);
    let commands_per_client = parse_commands_per_client(commands_per_client);
    let read_only_percentage = parse_read_only_percentage(read_only_percentage);
    let payload_size = parse_payload_size(payload_size);
    let mut workload = Workload::new(
        shard_count,
        key_gen,
        keys_per_command,
        commands_per_client,
        payload_size,
    );
    workload.set_read_only_percentage(read_only_percentage);
    workload
}

fn parse_keys_per_command(number: Option<&str>) -> usize {
    number
        .map(|number| {
            number
                .parse::<usize>()
                .expect("keys per command should be a number")
        })
        .unwrap_or(DEFAULT_KEYS_PER_COMMAND)
}

fn parse_shard_count(number: Option<&str>) -> usize {
    number
        .map(|number| {
            number
                .parse::<usize>()
                .expect("shard count should be a number")
        })
        .unwrap_or(DEFAULT_SHARD_COUNT)
}

fn parse_key_gen(key_gen: Option<&str>) -> KeyGen {
    key_gen
        .map(|key_gen| {
            let parts: Vec<_>= key_gen.split(',').collect();
            match parts.len() {
                2 | 3 => (),
                _ => panic!("invalid specification of key generator: {:?}", key_gen)
            };
            match parts[0] {
                "conflict_pool" => {
                    if parts.len() != 3 {
                        panic!("conflict_pool key generator takes two arguments");
                    }
                    let conflict_rate = parts[1]
                        .parse::<usize>()
                        .expect("conflict rate should be a number");
                    let pool_size = parts[2]
                        .parse::<usize>()
                        .expect("pool size should be a number");
                    KeyGen::ConflictPool { conflict_rate, pool_size }
                }
                "zipf" => {
                    if parts.len() != 3 {
                        panic!("zipf key generator takes two arguments");
                    }
                    let coefficient = parts[1]
                        .parse::<f64>()
                        .expect("zipf coefficient should be a float");
                    let keys_per_shard = parts[2]
                        .parse::<usize>()
                        .expect("number of keys (per shard) in the zipf distribution should be a number");
                        KeyGen::Zipf {
                            coefficient, total_keys_per_shard: keys_per_shard
                        }
                }
                kgen => panic!("invalid key generator type: {}", kgen),
            }
        })
        .unwrap_or(DEFAULT_KEY_GEN)
}

fn parse_commands_per_client(number: Option<&str>) -> usize {
    number
        .map(|number| {
            number
                .parse::<usize>()
                .expect("commands per client should be a number")
        })
        .unwrap_or(DEFAULT_COMMANDS_PER_CLIENT)
}

fn parse_read_only_percentage(number: Option<&str>) -> usize {
    number
        .map(|number| {
            number
                .parse::<usize>()
                .expect("read only percentage should be a number")
        })
        .unwrap_or(DEFAULT_READ_ONLY_PERCENTAGE)
}

fn parse_payload_size(number: Option<&str>) -> usize {
    number
        .map(|number| {
            number
                .parse::<usize>()
                .expect("payload size should be a number")
        })
        .unwrap_or(DEFAULT_PAYLOAD_SIZE)
}

fn parse_batch_max_size(number: Option<&str>) -> usize {
    number
        .map(|number| {
            number
                .parse::<usize>()
                .expect("batch max size should be a number")
        })
        .unwrap_or(DEFAULT_BATCH_MAX_SIZE)
}

fn parse_batch_max_delay(duration: Option<&str>) -> Duration {
    parse_millis_duration(duration).unwrap_or(DEFAULT_BATCH_MAX_DELAY)
}

fn parse_status_frequency(status_frequency: Option<&str>) -> Option<usize> {
    status_frequency.map(|status_frequency| {
        status_frequency
            .parse::<usize>()
            .expect("status frequency should be a number")
    })
}

pub fn parse_metrics_file(metrics_file: Option<&str>) -> Option<String> {
    metrics_file.map(String::from)
}
