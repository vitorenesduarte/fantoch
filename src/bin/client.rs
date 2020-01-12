use clap::{App, Arg};
use planet_sim::id::ClientId;
use std::error::Error;

const DEFAULT_CLIENT_NUMBER: usize = 1;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (client_id, address, client_number) = parse_args();

    println!("client id: {}", client_id);
    println!("process address: {}", address);
    println!("client number: {}", client_number);
    planet_sim::run::client(client_id, address, client_number).await?;
    Ok(())
}

fn parse_args() -> (ClientId, String, usize) {
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
                .short("a")
                .long("address")
                .value_name("ADDR")
                .help("address of the protocol instance to connect to")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("number")
                .short("n")
                .long("number")
                .value_name("CLIENT_NUMBER")
                .help("number of clients")
                .takes_value(true),
        )
        .get_matches();

    // parse arguments
    let id = parse_id(matches.value_of("id"));
    let address = parse_address(matches.value_of("address"));
    let client_number = parse_client_number(matches.value_of("number"));
    (id, address, client_number)
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
