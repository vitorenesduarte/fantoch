// This module contains the definition of...
pub mod task;

// This module contains the definition of...
pub mod net;

use crate::command::{Command, CommandResult};
use crate::id::{ClientId, ProcessId};
use crate::protocol::Process;
use std::error::Error;
use std::fmt::Debug;
use tokio::net::ToSocketAddrs;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug)]
pub enum FromClient {
    // clients can register
    Register(ClientId, UnboundedSender<CommandResult>),
    // or submit new commands
    Submit(Command),
}

pub async fn run<P, A>(
    process_id: ProcessId,
    port: u16,
    addresses: Vec<A>,
    client_port: u16,
) -> Result<(), Box<dyn Error>>
where
    P: Process + 'static, // TODO what does this 'static do?
    A: ToSocketAddrs + Debug + Clone,
{
    // check ports are different
    assert!(port != client_port);

    let (from_readers, to_writers, from_clients) =
        net::init::<P, A>(process_id, port, addresses, client_port).await?;

    loop {
        // match future::select(from_readers.recv(), from_clients.recv()) {
        //     Either::Left(new_msg) => {
        //         println!("new msg: {:?}", new_msg);
        //     }
        //     Either::Right(new_submit) => {
        //         println!("new submit: {:?}", new_submit);
        //     }
        // }
    }
}
