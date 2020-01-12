// This module contains the definition of...
pub mod task;

use crate::client::{Client, Workload};
use crate::command::{Command, CommandResult};
use crate::config::Config;
use crate::executor::Executor;
use crate::id::{ClientId, ProcessId};
use crate::protocol::{Protocol, ToSend};
use crate::time::RunTime;
use futures::future::FutureExt;
use futures::select;
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

pub enum FromExecutor {
    Submit(Command),
}

pub async fn process<A, P>(
    mut process: P,
    process_id: ProcessId,
    sorted_processes: Vec<ProcessId>,
    port: u16,
    addresses: Vec<A>,
    client_port: u16,
    config: Config,
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
    let from_clients = task::client::start_listener(process_id, listener);

    // start executor
    let (mut from_executor, to_executor) = task::process::start_executor::<P>(config, from_clients);

    loop {
        select! {
            msg = from_readers.recv().fuse() => {
                println!("reader message: {:?}", msg);
                if let Some((from, msg)) = msg {
                    handle_from_processes(process_id, from, msg, &mut process, &to_writer, &to_executor)
                } else {
                    println!("[server] error while receiving new process message from readers");
                }
            }
            cmd = from_executor.recv().fuse() => {
                println!("from executor: {:?}", from_executor);
                if let Some(cmd) = cmd {
                    handle_from_client(process_id, cmd, &mut process, &to_writer, &to_executor)
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
    to_executor: &UnboundedSender<Vec<<P::Executor as Executor>::ExecutionInfo>>,
) where
    P: Protocol,
{
    // handle message in process
    let to_send = process.handle(from, msg);
    send_to_writer(process_id, to_send, process, to_writer, to_executor);

    // check if there's new execution info for the executor
    let execution_info = process.to_executor();
    if !execution_info.is_empty() {
        if let Err(e) = to_executor.send(execution_info) {
            println!("[server] error while sending to executor: {:?}", e);
        }
    }
}

fn handle_from_client<P>(
    process_id: ProcessId,
    cmd: Command,
    process: &mut P,
    to_writer: &UnboundedSender<ToSend<P::Message>>,
    to_executor: &UnboundedSender<Vec<<P::Executor as Executor>::ExecutionInfo>>,
) where
    P: Protocol,
{
    // submit command in process
    let to_send = process.submit(cmd);
    send_to_writer(process_id, Some(to_send), process, to_writer, to_executor);
}

fn send_to_writer<P>(
    process_id: ProcessId,
    to_send: Option<ToSend<P::Message>>,
    process: &mut P,
    to_writer: &UnboundedSender<ToSend<P::Message>>,
    to_executor: &UnboundedSender<Vec<<P::Executor as Executor>::ExecutionInfo>>,
) where
    P: Protocol,
{
    if let Some(to_send) = to_send {
        // handle msg locally if self in `to_send.target`
        if to_send.target.contains(&process_id) {
            handle_from_processes(
                process_id,
                process_id,
                to_send.msg.clone(),
                process,
                to_writer,
                to_executor,
            );
        }
        if let Err(e) = to_writer.send(to_send) {
            println!("[server] error while sending to broadcast writer: {:?}", e);
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
