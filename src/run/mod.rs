// This module contains the prelude.
mod prelude;

// This module contains the definition of...
mod task;

use crate::client::{Client, Workload};
use crate::command::Command;
use crate::config::Config;
use crate::id::{ClientId, ProcessId};
use crate::log;
use crate::protocol::{Protocol, ToSend};
use crate::time::RunTime;
use futures::future::FutureExt;
use futures::select;
use prelude::*;
use std::error::Error;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::net::ToSocketAddrs;
use tokio::sync::Semaphore;

const LOCALHOST: &str = "127.0.0.1";
const CONNECT_RETRIES: usize = 100;

pub async fn process<A, P>(
    process: P,
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
    // this is for callers that don't care about the connected notification
    let semaphore = Arc::new(Semaphore::new(0));
    process_with_notify::<A, P>(
        process,
        process_id,
        sorted_processes,
        port,
        addresses,
        client_port,
        config,
        semaphore,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn process_with_notify<A, P>(
    mut process: P,
    process_id: ProcessId,
    sorted_processes: Vec<ProcessId>,
    port: u16,
    addresses: Vec<A>,
    client_port: u16,
    config: Config,
    connected: Arc<Semaphore>,
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

    // notify parent that we're connected
    connected.add_permits(1);

    loop {
        select! {
            msg = from_readers.recv().fuse() => {
                log!("[server] reader message: {:?}", msg);
                if let Some((from, msg)) = msg {
                    handle_from_processes(process_id, from, msg, &mut process, &to_writer, &to_executor)
                } else {
                    println!("[server] error while receiving new process message from readers");
                }
            }
            cmd = from_executor.recv().fuse() => {
                log!("[server] from executor: {:?}", cmd);
                if let Some(cmd) = cmd {
                    handle_from_client(process_id, cmd, &mut process, &to_writer, &to_executor)
                } else {
                    println!("[server] error while receiving new command from executor");
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
    to_writer: &BroadcastWriterSender<P::Message>,
    to_executor: &ExecutionInfoSender<P>,
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
    to_writer: &BroadcastWriterSender<P::Message>,
    to_executor: &ExecutionInfoSender<P>,
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
    to_writer: &BroadcastWriterSender<P::Message>,
    to_executor: &ExecutionInfoSender<P>,
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
    workload: Workload,
) -> Result<(), Box<dyn Error>>
where
    A: ToSocketAddrs,
{
    // TODO there's a single client for now
    let mut connection = task::connect(address).await?;

    // create system time
    let time = RunTime;

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
    }
    println!("client {} done", client_id);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Newt;
    use tokio::task;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_semaphore() {
        // create semaphore
        let semaphore = Arc::new(Semaphore::new(0));

        let task_semaphore = semaphore.clone();
        tokio::spawn(async move {
            println!("[task] will sleep for 5 seconds");
            tokio::time::delay_for(Duration::from_secs(5)).await;
            println!("[task] semaphore released!");
            task_semaphore.add_permits(1);
        });

        println!("[main] will block on the semaphore");
        let _ = semaphore.acquire().await;
        println!("[main] semaphore acquired!");
    }

    #[tokio::test]
    async fn test_run() {
        // test with newt
        // create local task set
        let local = task::LocalSet::new();

        // run test in local task set
        local
            .run_until(async {
                match run::<Newt>().await {
                    Ok(()) => {}
                    Err(e) => panic!("run failed: {:?}", e),
                }
            })
            .await;
    }

    async fn run<P>() -> Result<(), Box<dyn Error>>
    where
        P: Protocol + 'static,
    {
        // create config
        let n = 3;
        let f = 1;
        let config = Config::new(n, f);

        // create processes
        let process_1 = P::new(1, config);
        let process_2 = P::new(2, config);
        let process_3 = P::new(3, config);

        // create semaphores
        let semaphore_1 = Arc::new(Semaphore::new(0));
        let semaphore_2 = Arc::new(Semaphore::new(0));
        let semaphore_3 = Arc::new(Semaphore::new(0));

        // spawn processes
        task::spawn_local(process_with_notify::<String, P>(
            process_1,
            1,
            vec![1, 2, 3],
            3001,
            vec![
                String::from("localhost:3002"),
                String::from("localhost:3003"),
            ],
            4001,
            config,
            semaphore_1.clone(),
        ));
        task::spawn_local(process_with_notify::<String, P>(
            process_2,
            2,
            vec![2, 3, 1],
            3002,
            vec![
                String::from("localhost:3001"),
                String::from("localhost:3003"),
            ],
            4002,
            config,
            semaphore_2.clone(),
        ));
        task::spawn_local(process_with_notify::<String, P>(
            process_3,
            3,
            vec![3, 1, 2],
            3003,
            vec![
                String::from("localhost:3001"),
                String::from("localhost:3002"),
            ],
            4003,
            config,
            semaphore_3.clone(),
        ));

        // wait that all processes are connected
        println!("[main] waiting that processes are connected");
        let _ = semaphore_1.acquire().await;
        let _ = semaphore_2.acquire().await;
        let _ = semaphore_3.acquire().await;
        println!("[main] processes are connected");

        // create workload
        let conflict_rate = 100;
        let total_commands = 1000;
        let workload = Workload::new(conflict_rate, total_commands);

        // spawn clients
        let client_1_handle =
            task::spawn_local(client(1, String::from("localhost:4001"), 1, workload));
        let client_2_handle =
            task::spawn_local(client(2, String::from("localhost:4002"), 1, workload));
        let client_3_handle =
            task::spawn_local(client(3, String::from("localhost:4003"), 1, workload));

        // wait for the 3 clients
        let _ = client_1_handle.await.expect("client 1 should finish");
        let _ = client_2_handle.await.expect("client 2 should finish");
        let _ = client_3_handle.await.expect("client 3 should finish");
        Ok(())
    }
}
