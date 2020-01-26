mod common;

use planet_sim::protocol::{Basic, Protocol};
use std::error::Error;

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
        multiplexing,
    ) = common::protocol::parse_args();
    let process = Basic::new(process_id, config);
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
        multiplexing,
    )
    .await?;
    Ok(())
}
