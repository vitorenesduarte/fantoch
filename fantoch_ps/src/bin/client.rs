mod common;

use clap::{App, Arg};
use fantoch::client::{KeyGen, Workload};
use fantoch::id::ClientId;
use std::error::Error;
use std::time::Duration;

const RANGE_SEP: &str = "-";
const DEFAULT_KEY_GEN: KeyGen = KeyGen::ConflictRate { conflict_rate: 100 };
const DEFAULT_KEYS_PER_COMMAND: usize = 1;
const DEFAULT_COMMANDS_PER_CLIENT: usize = 1000;
const DEFAULT_PAYLOAD_SIZE: usize = 100;

type ClientArgs = (
    Vec<ClientId>,
    String,
    Option<Duration>,
    Workload,
    bool,
    usize,
    Option<String>,
);

fn main() -> Result<(), Box<dyn Error>> {
    let (
        ids,
        address,
        interval,
        workload,
        tcp_nodelay,
        channel_buffer_size,
        metrics_file,
    ) = parse_args();

    common::tokio_runtime().block_on(fantoch::run::client(
        ids,
        address,
        interval,
        workload,
        tcp_nodelay,
        channel_buffer_size,
        metrics_file,
    ))
}

fn parse_args() -> ClientArgs {
    let matches = App::new("client")
        .version("0.1")
        .author("Vitor Enes <vitorenesduarte@gmail.com>")
        .about("Runs a client that will connect to some instance of a protocol.")
        .arg(
            Arg::with_name("ids")
                .long("ids")
                .value_name("ID_RANGE")
                .help("a range of client identifiers represented as START-END; as many client as the number of identifers will be created")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("address")
                .long("address")
                .value_name("ADDR")
                .help("address of the protocol instance to connect to (in the form IP:PORT e.g. 127.0.0.1:3000)")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("interval")
                .long("interval")
                .value_name("INTERVAL")
                .help("if this value is set, an open-loop client will be created (by default is closed-loop) and the value set is used as the interval (in milliseconds) between submitted commands")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("key_gen")
                .long("key_gen")
                .value_name("KEY_GEN")
                .help("representation of a key generator; possible values 'conflict_rate,100' where 100 is the conflict rate, or 'zipf,1.3,10000' where 1.3 is the zipf coefficient (which should be non-zero) and 10000 the number of keys in the distribution; default: 'conflict_rate,100'")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("keys_per_command")
                .long("keys_per_command")
                .value_name("KEYS_PER_COMMAND")
                .help("number of keys in each command to be issued by each client; default: 1")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("commands_per_client")
                .long("commands_per_client")
                .value_name("COMMANDS_PER_CLIENT")
                .help("number of commands to be issued by each client; default: 1000")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("payload_size")
                .long("payload_size")
                .value_name("PAYLOAD_SIZE")
                .help("size of the command payload; default: 100 (bytes)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("tcp_nodelay")
                .long("tcp_nodelay")
                .value_name("TCP_NODELAY")
                .help("set TCP_NODELAY; default: true")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("channel_buffer_size")
                .long("channel_buffer_size")
                .value_name("CHANNEL_BUFFER_SIZE")
                .help("set the size of the buffer in each channel used for task communication; default: 10000")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("metrics_file")
                .long("metrics_file")
                .value_name("METRICS_FILE")
                .help("file in which metrics are written to; by default metrics are not logged")
                .takes_value(true),
        )
        .get_matches();

    // parse arguments
    let ids = parse_id_range(matches.value_of("ids"));
    let address = parse_address(matches.value_of("address"));
    let interval = parse_interval(matches.value_of("interval"));
    let workload = parse_workload(
        matches.value_of("key_gen"),
        matches.value_of("keys_per_command"),
        matches.value_of("commands_per_client"),
        matches.value_of("payload_size"),
    );
    let tcp_nodelay =
        common::parse_tcp_nodelay(matches.value_of("tcp_nodelay"));
    let channel_buffer_size = common::parse_channel_buffer_size(
        matches.value_of("channel_buffer_size"),
    );
    let metrics_file = parse_metrics_file(matches.value_of("metrics_file"));

    println!("ids: {}-{}", ids.first().unwrap(), ids.last().unwrap());
    println!("client number: {}", ids.len());
    println!("process address: {}", address);
    println!("workload: {:?}", workload);
    println!("tcp_nodelay: {:?}", tcp_nodelay);
    println!("channel buffer size: {:?}", channel_buffer_size);
    println!("metrics file: {:?}", metrics_file);

    (
        ids,
        address,
        interval,
        workload,
        tcp_nodelay,
        channel_buffer_size,
        metrics_file,
    )
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

fn parse_address(addresses: Option<&str>) -> String {
    addresses.expect("address should be set").to_string()
}

fn parse_interval(interval: Option<&str>) -> Option<Duration> {
    interval.map(|interval| {
        let ms = interval
            .parse::<u64>()
            .expect("interval should be a number");
        Duration::from_millis(ms)
    })
}

fn parse_workload(
    key_gen: Option<&str>,
    keys_per_command: Option<&str>,
    commands_per_client: Option<&str>,
    payload_size: Option<&str>,
) -> Workload {
    let key_gen = parse_key_gen(key_gen);
    let keys_per_command = parse_keys_per_command(keys_per_command);
    let commands_per_client = parse_commands_per_client(commands_per_client);
    let payload_size = parse_payload_size(payload_size);
    Workload::new(key_gen, keys_per_command, commands_per_client, payload_size)
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
                "conflict_rate" => {
                    if parts.len() != 2 {
                        panic!("conflict_rate key generator takes a single argument");
                    }
                    let conflict_rate = parts[1]
                        .parse::<usize>()
                        .expect("conflict rate should be a number");
                    KeyGen::ConflictRate { conflict_rate }
                }
                "zipf" => {
                    if parts.len() != 3 {
                        panic!("zipf key generator takes two arguments");
                    }
                    let coefficient = parts[1]
                        .parse::<f64>()
                        .expect("zipf coefficient should be a float");
                    let key_count = parts[2]
                        .parse::<usize>()
                        .expect("number of keys in the zipf distribution should be a number");
                        KeyGen::Zipf {
                            coefficient, key_count
                        }
                }
                kgen => panic!("invalid key generator type: {}", kgen),
            }
        })
        .unwrap_or(DEFAULT_KEY_GEN)
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

fn parse_commands_per_client(number: Option<&str>) -> usize {
    number
        .map(|number| {
            number
                .parse::<usize>()
                .expect("commands per client should be a number")
        })
        .unwrap_or(DEFAULT_COMMANDS_PER_CLIENT)
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

pub fn parse_metrics_file(metrics_file: Option<&str>) -> Option<String> {
    metrics_file.map(String::from)
}
