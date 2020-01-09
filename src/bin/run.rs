use clap::{App, Arg};
use planet_sim::id::ProcessId;
use planet_sim::run;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;

const DEFAULT_PORT: u16 = 3717;
const ADDRESSES_SEP: &str = ",";

#[derive(Serialize, Deserialize)]
struct Hi(ProcessId);

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (process_id, port, addresses) = parse_args();

    println!("port: {}", port);
    println!("addresses: {:?}", addresses);

    // connect to all
    let (connections, mut connections_say_hi) = run::net::connect_to_all(port, addresses).await?;

    // say hi to all processes
    let hi = Hi(process_id);
    for connection in connections_say_hi.iter_mut() {
        connection.send(&hi).await;
    }
    println!("said hi to all processes");

    // create mapping from process id to connection
    let mut id_to_connection = HashMap::new();
    for mut connection in connections {
        if let Some(Hi(from)) = connection.recv().await {
            // save entry and check it has not been inserted before
            let res = id_to_connection.insert(from, connection);
            assert!(res.is_none());
        } else {
            println!("error receiving hi");
            // TODO how can we make errors?
            return Ok(());
        }
    }

    println!("received hi to processes: {:?}", id_to_connection.keys());
    Ok(())
}

fn parse_args() -> (ProcessId, u16, Vec<String>) {
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
        .get_matches();

    // parse arguments
    let id = parse_id(matches.value_of("id"));
    let port = parse_port(matches.value_of("port"));
    let addresses = parse_addresses(matches.value_of("addresses"));
    (id, port, addresses)
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
