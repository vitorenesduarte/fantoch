mod common;

use planet_sim::protocol::{Protocol, SequentialEPaxos};
use std::error::Error;

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
        tcp_buffer_size,
        tcp_flush_interval,
        channel_buffer_size,
        multiplexing,
        execution_log,
    ) = common::protocol::parse_args();

    // check that no leader was defined
    if config.leader().is_some() {
        panic!("can't define a leader in leaderless protocol");
    }

    // create process
    let process = SequentialEPaxos::new(process_id, config);

    common::tokio_runtime().block_on(planet_sim::run::process(
        process,
        process_id,
        sorted_processes,
        ip,
        port,
        client_port,
        addresses,
        config,
        tcp_nodelay,
        tcp_buffer_size,
        tcp_flush_interval,
        channel_buffer_size,
        multiplexing,
        execution_log,
    ))
}
