// This module contains the definition of...
pub mod task;

// This module contains the definition of...
pub mod net;

use crate::client::{Client, Workload};
use crate::command::{Command, CommandResult};
use crate::id::{ClientId, ProcessId};
use crate::protocol::{Process, ToSend};
use crate::time::RunTime;
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
    // TODO there's a single client for now
    let mut connection = net::connect(address).await?;

    // create system time
    let time = RunTime;

    // TODO make workload configurable
    let conflict_rate = 10;
    let total_commands = 100;
    let workload = Workload::new(conflict_rate, total_commands);

    // create client
    let mut client = Client::new(client_id, workload);

    // say hi
    let process_id = net::client::say_hi(&mut connection, client_id).await;

    // set process id (although this won't be used)
    client.skip_discover(process_id);

    if let Some((_, cmd)) = client.start(&time) {
        // submit first command
        connection.send(cmd).await;
        loop {
            if let Some(cmd_result) = connection.recv().await {
                if let Some((_, cmd)) = client.handle(cmd_result, &time) {
                    connection.send(cmd).await;
                } else {
                    // all commands have been generated
                    println!("client {} ended", client_id);
                    println!("total commands: {}", client.issued_commands());
                    println!("{:?}", client.latency_histogram());
                    return Ok(());
                }
            } else {
                panic!("couldn't receive command result from process");
            }
        }
    } else {
        panic!("client couldn't be started");
    }
}
