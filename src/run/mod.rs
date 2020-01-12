// This module contains the definition of...
pub mod task;

// This module contains the definition of...
pub mod net;

use crate::command::{Command, CommandResult};
use crate::id::{ClientId, ProcessId};
use crate::protocol::{Process, ToSend};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt::Debug;
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

const LOCALHOST: &str = "127.0.0.1";
const CONNECT_RETRIES: usize = 100;

#[derive(Debug)]
pub enum FromClient {
    // clients can register
    Register(ClientId, UnboundedSender<CommandResult>),
    // or submit new commands
    Submit(Command),
}

pub async fn process<A, P>(
    process_id: ProcessId,
    port: u16,
    addresses: Vec<A>,
    client_port: u16,
) -> Result<(), Box<dyn Error>>
where
    A: ToSocketAddrs + Debug + Clone,
    P: Process + 'static, // TODO what does this 'static do?
{
    // check ports are different
    assert!(port != client_port);

    // start process listener
    let listener = net::listen((LOCALHOST, port)).await?;

    // connect to all processes
    let (from_readers, to_writer) = net::process::connect_to_all::<A, P::Message>(
        process_id,
        listener,
        addresses,
        CONNECT_RETRIES,
    )
    .await?;

    // start client listener
    let listener = net::listen((LOCALHOST, client_port)).await?;
    let from_clients = net::client::start_listener(listener);

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

pub async fn client<A>(
    client_id: ClientId,
    address: A,
    client_number: usize,
) -> Result<(), Box<dyn Error>>
where
    A: ToSocketAddrs,
{
    let connection = net::connect(address).await?;
    loop {}
}
