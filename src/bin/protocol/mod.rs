use clap::{App, Arg};
use planet_sim::config::Config;
use planet_sim::id::ProcessId;

const LIST_SEP: &str = ",";

pub fn parse_args() -> (ProcessId, Vec<ProcessId>, u16, Vec<String>, u16, Config) {
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
            Arg::with_name("port")
                .long("port")
                .value_name("PORT")
                .help("port to bind to")
                .required(true)
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
            Arg::with_name("client_port")
                .long("client_port")
                .value_name("CLIENT_PORT")
                .help("client port to bind to")
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
        .get_matches();

    // parse arguments
    let process_id = parse_process_id(matches.value_of("id"));
    let sorted_processes = parse_sorted_processes(matches.value_of("sorted_processes"));
    let port = parse_port(matches.value_of("port"));
    let addresses = parse_addresses(matches.value_of("addresses"));
    let client_port = parse_port(matches.value_of("client_port"));
    let config = parse_config(matches.value_of("n"), matches.value_of("f"));

    println!("process id: {}", process_id);
    println!("sorted processes: {:?}", sorted_processes);
    println!("port: {}", port);
    println!("addresses: {:?}", addresses);
    println!("client port: {}", client_port);
    println!("config: {:?}", config);

    // check that the number of sorted processes equals `n`
    assert_eq!(sorted_processes.len(), config.n());

    // check that the number of addresses equals `n - 1`
    assert_eq!(addresses.len(), config.n() - 1);

    (
        process_id,
        sorted_processes,
        port,
        addresses,
        client_port,
        config,
    )
}

fn parse_process_id(id: Option<&str>) -> ProcessId {
    parse_id(id.expect("process id should be set"))
}

fn parse_sorted_processes(ids: Option<&str>) -> Vec<ProcessId> {
    ids.expect("sorted processes should be set")
        .split(LIST_SEP)
        .map(|id| parse_id(id))
        .collect()
}

fn parse_port(port: Option<&str>) -> u16 {
    port.expect("port should be set")
        .parse::<u16>()
        .expect("port should be a number")
}

fn parse_addresses(addresses: Option<&str>) -> Vec<String> {
    addresses
        .expect("addresses should be set")
        .split(LIST_SEP)
        .map(|address| address.to_string())
        .collect()
}

fn parse_config(n: Option<&str>, f: Option<&str>) -> Config {
    let n = n
        .expect("n should be set")
        .parse::<usize>()
        .expect("n should be a number");
    let f = f
        .expect("f should be set")
        .parse::<usize>()
        .expect("f should be a number");
    Config::new(n, f)
}

fn parse_id(id: &str) -> ProcessId {
    id.parse::<ProcessId>()
        .expect("process id should be a number")
}
