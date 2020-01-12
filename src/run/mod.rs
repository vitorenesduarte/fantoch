// This module contains the definition of...
pub mod task;

// This module contains the definition of...
pub mod net;

use crate::client::{Client, Workload};
use crate::command::{Command, CommandResult};
use crate::id::{ClientId, ProcessId};
use crate::protocol::Process;
use crate::time::RunTime;
use futures::future::FutureExt;
use futures::select;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::Debug;
use tokio::net::ToSocketAddrs;
use tokio::sync::mpsc::UnboundedSender;

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
    let (mut from_readers, to_writer) = net::process::connect_to_all::<A, P::Message>(
        process_id,
        listener,
        addresses,
        CONNECT_RETRIES,
    )
    .await?;

    // start client listener
    let listener = net::listen((LOCALHOST, client_port)).await?;
    let mut from_clients = net::client::start_listener(process_id, listener);

    // mapping from client id to its channel
    let mut clients = HashMap::new();

    loop {
        select! {
            msg = from_readers.recv().fuse() => {
                println!("reader message: {:?}", msg);
                if let Some(msg) = msg {
                    // TODO handle in process
                } else {
                    println!("[server] error while receiving new process message from readers");
                }
            }
            from_client = from_clients.recv().fuse() => {
                println!("from client: {:?}", from_client);
                if let Some(from_client) = from_client {
                    handle_from_client(from_client, &mut clients)
                } else {
                    println!("[server] error while receiving new command from clients");
                }
            }
        }
    }
}

fn handle_from_client(
    from_client: FromClient,
    clients: &mut HashMap<ClientId, UnboundedSender<CommandResult>>,
) {
    match from_client {
        FromClient::Register(client_id, tx) => {
            let res = clients.insert(client_id, tx);
            assert!(res.is_none());
        }
        FromClient::Submit(cmd) => {
            // TODO handle in process; for now create fake command result
            // get client id
            let client_id = cmd.rifl().source();
            // find its channel
            let tx = clients
                .get_mut(&client_id)
                .expect("client should register before submitting any commands");
            // fake command result
            let cmd_result = CommandResult::new(cmd.rifl(), cmd.key_count());
            if let Err(e) = tx.send(cmd_result) {
                println!(
                    "[server] error while sending command result to client: {:?}",
                    e
                );
            }
        }
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
    let process_id = net::client::client_say_hi(client_id, &mut connection).await;

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
