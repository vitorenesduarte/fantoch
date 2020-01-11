use clap::{App, Arg};
use planet_sim::id::ProcessId;
use planet_sim::protocol::Atlas;
use std::error::Error;

const DEFAULT_PORT: u16 = 3717;
const ADDRESSES_SEP: &str = ",";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (process_id, port, addresses, client_port) = parse_args();

    println!("process id: {}", process_id);
    println!("port: {}", port);
    println!("addresses: {:?}", addresses);
    println!("client port: {}", client_port);
    // planet_sim::run::run::<Atlas, String>(port, addresses, client_port, process_id).await
    Ok(())
}

fn parse_args() -> (ProcessId, u16, Vec<String>, u16) {
    let matches = App::new("prun")
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
            Arg::with_name("port")
                .short("p")
                .long("port")
                .value_name("PORT")
                .help("port to bind to")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("addresses")
                .short("a")
                .long("addresses")
                .value_name("ADDR")
                .help("comma-separated list of addresses to connect to")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("client_port")
                .short("cp")
                .long("client_port")
                .value_name("CLIENT_PORT")
                .help("client port to bind to")
                .takes_value(true),
        )
        .get_matches();

    // parse arguments
    let id = parse_id(matches.value_of("id"));
    let port = parse_port(matches.value_of("port"));
    let addresses = parse_addresses(matches.value_of("addresses"));
    let client_port = parse_port(matches.value_of("client_port"));
    (id, port, addresses, client_port)
}

fn parse_id(id: Option<&str>) -> ProcessId {
    id.expect("id should be set")
        .parse::<ProcessId>()
        .expect("process id should be a number")
}

fn parse_port(port: Option<&str>) -> u16 {
    port.map(|port| port.parse::<u16>().expect("port should be a number"))
        .unwrap_or(DEFAULT_PORT)
}

fn parse_addresses(addresses: Option<&str>) -> Vec<String> {
    addresses
        .expect("addresses should be set")
        .split(ADDRESSES_SEP)
        .map(|address| address.to_string())
        .collect()
}
