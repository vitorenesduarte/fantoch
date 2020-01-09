use clap::{App, Arg};
use planet_sim::run;
use std::error::Error;

const DEFAULT_PORT: u16 = 3717;
const ADDRESSES_SEP: &str = ",";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let matches = App::new("prun")
        .version("0.1")
        .author("Vitor Enes <vitorenesduarte@gmail.com>")
        .about("Runs an instance of some protocol.")
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
        .get_matches();

    // get port
    let port = parse_port(matches.value_of("port"));

    // get addresses
    let addresses = parse_addresses(matches.value_of("addresses"));

    println!("port: {}", port);
    println!("addresses: {:?}", addresses);

    // connect to all
    let (incoming, outgoing) = run::net::connect_to_all(port, addresses).await?;

    println!("in: {:?}", incoming);
    println!("out: {:?}", outgoing);

    Ok(())
}

fn parse_port(port: Option<&str>) -> u16 {
    port.map(|port| port.parse::<u16>().expect("port should be a number"))
        .unwrap_or(DEFAULT_PORT)
}

fn parse_addresses(addresses: Option<&str>) -> Vec<&str> {
    addresses
        .expect("addresses should be set")
        .split(ADDRESSES_SEP)
        .collect()
}
