// This module contains the definition of...
pub mod net;

use crate::id::ProcessId;
use crate::protocol::Process;
use std::error::Error;
use std::fmt::Debug;
use std::marker::PhantomData;
use tokio::net::ToSocketAddrs;

struct Runner<P> {
    phantom: PhantomData<P>,
}

pub async fn run<P, A>(
    port: u16,
    addresses: Vec<A>,
    process_id: ProcessId,
) -> Result<(), Box<dyn Error>>
where
    P: Process + 'static, // TODO what does this 'static do?
    A: ToSocketAddrs + Debug + Clone,
{
    let (from_readers, to_writers) =
        net::connect_to_all::<P, A>(process_id, port, addresses).await?;
    println!("connected to processes: {:?}", to_writers.keys());
    Ok(())
}
