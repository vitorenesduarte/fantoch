// This module contains the definition of...
pub mod task;

use crate::client::{Client, Workload};
use crate::command::{Command, CommandResult};
use crate::id::{ClientId, ProcessId};
use crate::protocol::{Protocol, ToSend};
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
    // unregister
    Unregister(ClientId),
    // or submit new commands
    Submit(Command),
}

pub async fn process<A, P>(
    mut process: P,
    process_id: ProcessId,
    sorted_processes: Vec<ProcessId>,
    port: u16,
    addresses: Vec<A>,
    client_port: u16,
) -> Result<(), Box<dyn Error>>
where
    A: ToSocketAddrs + Debug + Clone,
    P: Protocol + 'static, // TODO what does this 'static do?
{
    // discover processes
    process.discover(sorted_processes);

    // check ports are different
    assert!(port != client_port);

    // start process listener
    let listener = task::listen((LOCALHOST, port)).await?;

    // connect to all processes
    let (mut from_readers, to_writer) = task::process::connect_to_all::<A, P::Message>(
        process_id,
        listener,
        addresses,
        CONNECT_RETRIES,
    )
    .await?;

    // start client listener
    let listener = task::listen((LOCALHOST, client_port)).await?;
    let mut from_clients = task::client::start_listener(process_id, listener);

    // mapping from client id to its channel
    let mut clients = HashMap::new();

    loop {
        select! {
            msg = from_readers.recv().fuse() => {
                println!("reader message: {:?}", msg);
                if let Some((from, msg)) = msg {
                    handle_from_processes(process_id, from, msg, &mut process, &to_writer)
                } else {
                    println!("[server] error while receiving new process message from readers");
                }
            }
            from_client = from_clients.recv().fuse() => {
                println!("from client: {:?}", from_client);
                if let Some(from_client) = from_client {
                    handle_from_client(from_client, &mut clients, &mut process, &to_writer)
                } else {
                    println!("[server] error while receiving new command from clients");
                }
            }
        }
    }
}

fn handle_from_processes<P>(
    process_id: ProcessId,
    from: ProcessId,
    msg: P::Message,
    process: &mut P,
    to_writer: &UnboundedSender<ToSend<P::Message>>,
) where
    P: Protocol,
{
    // handle message in process
    if let Some(to_send) = process.handle(from, msg) {
        // handle msg locally if self in `to_send.target`
        if to_send.target.contains(&process_id) {
            handle_from_processes(
                process_id,
                process_id,
                to_send.msg.clone(),
                process,
                to_writer,
            );
        }
        if let Err(e) = to_writer.send(to_send) {
            println!("[server] error while sending to broadcast writer: {:?}", e);
        }
    }

    // check if there's new execution info for the executor
}
// // find its channel
// let tx = clients
//     .get_mut(&client_id)
//     .expect("client should register before submitting any commands");
// // fake command result
// let cmd_result = CommandResult::new(cmd.rifl(), cmd.key_count());
// if let Err(e) = tx.send(cmd_result) {
//     println!(
//         "[server] error while sending command result to client: {:?}",
//         e
//     );
// }

fn handle_from_client<P>(
    from_client: FromClient,
    clients: &mut HashMap<ClientId, UnboundedSender<CommandResult>>,
    process: &mut P,
    to_writer: &UnboundedSender<ToSend<P::Message>>,
) where
    P: Protocol,
{
    match from_client {
        FromClient::Submit(cmd) => {
            // get client id
            let client_id = cmd.rifl().source();

            // TODO register in executor

            // submit command in process
            let to_send = process.submit(cmd);
            if let Err(e) = to_writer.send(to_send) {
                println!("[server] error while sending to broadcast writer: {:?}", e);
            }
        }
        FromClient::Register(client_id, tx) => {
            println!("[server] client {} registered", client_id);
            let res = clients.insert(client_id, tx);
            assert!(res.is_none());
        }
        FromClient::Unregister(client_id) => {
            println!("[server] client {} unregistered", client_id);
            let res = clients.remove(&client_id);
            assert!(res.is_some());
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
    let mut connection = task::connect(address).await?;

    // create system time
    let time = RunTime;

    // TODO make workload configurable
    let conflict_rate = 10;
    let total_commands = 100;
    let workload = Workload::new(conflict_rate, total_commands);

    // create client
    let mut client = Client::new(client_id, workload);

    // say hi
    let process_id = task::client::client_say_hi(client_id, &mut connection).await;

    // discover process (although this won't be used)
    client.discover(vec![process_id]);

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
