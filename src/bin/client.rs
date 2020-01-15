use clap::{App, Arg};
use planet_sim::client::Workload;
use planet_sim::id::ClientId;
use std::error::Error;

const RANGE_SEP: &str = "-";
const DEFAULT_CONFLICT_RATE: usize = 100;
const DEFAULT_COMMANDS_PER_CLIENT: usize = 1000;
const DEFAULT_TCP_NODELAY: bool = true;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (ids, address, interval, workload, tcp_nodelay) = parse_args();
    planet_sim::run::client(ids, address, interval, workload, tcp_nodelay).await?;
    Ok(())
}

fn parse_args() -> (Vec<ClientId>, String, Option<u64>, Workload, bool) {
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
                .help("address of the protocol instance to connect to")
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
            Arg::with_name("conflict_rate")
                .long("conflict_rate")
                .value_name("CONFLICT_RATE")
                .help("number between 0 and 100 representing how contended the workload should be; default: 100")
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
            Arg::with_name("tcp_nodelay")
                .long("tcp_nodelay")
                .value_name("TCP_NODELAY")
                .help("set TCP_NODELAY; defaul: true")
                .takes_value(true),
        )
        .get_matches();

    // parse arguments
    let ids = parse_id_range(matches.value_of("ids"));
    let address = parse_address(matches.value_of("address"));
    let interval = parse_interval(matches.value_of("interval"));
    let workload = parse_workload(
        matches.value_of("conflict_rate"),
        matches.value_of("commands_per_client"),
    );
    let tcp_nodelay = parse_tcp_nodelay(matches.value_of("tcp_nodelay"));

    println!("ids: {:?}", ids);
    println!("client number: {}", ids.len());
    println!("process address: {}", address);
    println!("workload: {:?}", workload);
    println!("tcp_nodelay: {:?}", tcp_nodelay);

    (ids, address, interval, workload, tcp_nodelay)
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

fn parse_interval(interval: Option<&str>) -> Option<u64> {
    interval.map(|interval| {
        interval
            .parse::<u64>()
            .expect("interval should be a number")
    })
}

fn parse_workload(conflict_rate: Option<&str>, commands_per_client: Option<&str>) -> Workload {
    let conflict_rate = parse_conflict_rate(conflict_rate);
    let commands_per_client = parse_commands_per_client(commands_per_client);
    Workload::new(conflict_rate, commands_per_client)
}

fn parse_conflict_rate(number: Option<&str>) -> usize {
    number
        .map(|number| {
            number
                .parse::<usize>()
                .expect("conflict rate should be a number")
        })
        .unwrap_or(DEFAULT_CONFLICT_RATE)
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

fn parse_tcp_nodelay(tcp_nodelay: Option<&str>) -> bool {
    tcp_nodelay
        .map(|tcp_nodelay| {
            tcp_nodelay
                .parse::<bool>()
                .expect("tcp_nodelay should be a boolean")
        })
        .unwrap_or(DEFAULT_TCP_NODELAY)
}
