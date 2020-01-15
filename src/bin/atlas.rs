mod common;

use planet_sim::protocol::{Atlas, Protocol};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (process_id, sorted_processes, ip, port, client_port, addresses, config, tcp_nodelay) =
        common::protocol::parse_args();
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
    )
    .await?;
    Ok(())
}
