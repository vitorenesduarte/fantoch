mod protocol;

use planet_sim::protocol::{EPaxos, Protocol};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (process_id, sorted_processes, port, addresses, client_port, config) =
        protocol::parse_args();
    let process = EPaxos::new(process_id, config);
    planet_sim::run::process(
        process,
        process_id,
        sorted_processes,
        port,
        addresses,
        client_port,
    )
    .await?;
    Ok(())
}
