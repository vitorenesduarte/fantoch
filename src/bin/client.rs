use clap::{App, Arg};
use planet_sim::client::Workload;
use planet_sim::id::ClientId;
use std::error::Error;

const DEFAULT_CLIENT_NUMBER: usize = 1;
const DEFAULT_CONFLICT_RATE: usize = 100;
const DEFAULT_COMMANDS_PER_CLIENT: usize = 1000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (client_id, address, client_number, interval, workload) = parse_args();
    planet_sim::run::client(client_id, address, client_number, interval, workload).await?;
    Ok(())
}

fn parse_args() -> (ClientId, String, usize, Option<u64>, Workload) {
    let matches = App::new("client")
        .version("0.1")
        .author("Vitor Enes <vitorenesduarte@gmail.com>")
        .about("Runs a client that will connect to some instance of a protocol.")
        .arg(
            Arg::with_name("id")
                .long("id")
                .value_name("ID")
                .help("client identifier")
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
            Arg::with_name("client_number")
                .long("client_number")
                .value_name("CLIENT_NUMBER")
                .help("number of clients")
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
                .help("number between 0 and 100 representing how contended the workload should be")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("commands_per_client")
                .long("commands_per_client")
                .value_name("COMMANDS_PER_CLIENT")
                .help("number of commands to be issued by each client")
                .takes_value(true),
        )
        .get_matches();

    // parse arguments
    let client_id = parse_id(matches.value_of("id"));
    let address = parse_address(matches.value_of("address"));
    let client_number = parse_client_number(matches.value_of("client_number"));
    let interval = parse_interval(matches.value_of("interval"));
    let workload = parse_workload(
        matches.value_of("conflict_rate"),
        matches.value_of("commands_per_client"),
    );

    println!("client id: {}", client_id);
    println!("process address: {}", address);
    println!("client number: {}", client_number);
    println!("workload: {:?}", workload);

    (client_id, address, client_number, interval, workload)
}

fn parse_id(id: Option<&str>) -> ClientId {
    id.expect("id should be set")
        .parse::<ClientId>()
        .expect("client id should be a number")
}

fn parse_address(addresses: Option<&str>) -> String {
    addresses.expect("address should be set").to_string()
}

fn parse_client_number(number: Option<&str>) -> usize {
    number
        .map(|number| {
            number
                .parse::<usize>()
                .expect("client number should be a number")
        })
        .unwrap_or(DEFAULT_CLIENT_NUMBER)
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
