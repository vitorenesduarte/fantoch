mod protocol;

use planet_sim::protocol::{Atlas, Protocol};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (process_id, port, addresses, client_port, config) = protocol::parse_args();
    let process = Atlas::new(process_id, config);
    planet_sim::run::process(process, process_id, port, addresses, client_port).await?;
    Ok(())
}
