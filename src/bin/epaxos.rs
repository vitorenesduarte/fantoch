mod common;

use planet_sim::protocol::{EPaxos, Protocol};
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
    ) = common::protocol::parse_args();
    let process = EPaxos::new(process_id, config);

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
    ))
}
