mod common;

use planet_sim::protocol::{Atlas, Protocol};
use std::error::Error;

// TODO can we generate all the protocol binaries with a macro?

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
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
    ) = common::protocol::parse_args();
    let process = Atlas::new(process_id, config);
    planet_sim::run::process(
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
    )
    .await?;
    Ok(())
}
