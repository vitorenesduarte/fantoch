mod common;

use planet_sim::protocol::{Atlas, Protocol};
use std::error::Error;

// TODO can we generate all the protocol binaries with a macro?

fn main() -> Result<(), Box<dyn Error>> {
    let (
        process_id,
        sorted_processes,
        ip,
        port,
        client_port,
        addresses,
        config,
        tcp_nodelay,
        socket_buffer_size,
        channel_buffer_size,
        workers,
        executors,
        multiplexing,
    ) = common::protocol::parse_args();
    let process = Atlas::new(process_id, config);

    // get number of cpus
    let cpus = num_cpus::get();
    println!("cpus: {}", cpus);

    // create tokio runtime
    let mut runtime = tokio::runtime::Builder::new()
        .threaded_scheduler()
        .core_threads(cpus)
        .thread_name("runner")
        .build()
        .expect("tokio runtime build should work");

    runtime.block_on(planet_sim::run::process(
        process,
        process_id,
        sorted_processes,
        ip,
        port,
        client_port,
        addresses,
        config,
        tcp_nodelay,
        socket_buffer_size,
        channel_buffer_size,
        workers,
        executors,
        multiplexing,
    ))
}
