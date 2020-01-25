use super::connection::Connection;
use crate::config::Config;
use crate::executor::Executor;
use crate::id::{ClientId, ProcessId};
use crate::log;
use crate::protocol::Protocol;
use crate::run::forward::ReaderToWorkers;
use crate::run::prelude::*;
use crate::run::task;
use futures::future::FutureExt;
use futures::select;
use std::collections::HashMap;
use std::fmt::Debug;
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::time::Duration;

pub async fn connect_to_all<A, P>(
    process_id: ProcessId,
    listener: TcpListener,
    addresses: Vec<A>,
    to_workers: ReaderToWorkers<P>,
    connect_retries: usize,
    tcp_nodelay: bool,
    socket_buffer_size: usize,
    channel_buffer_size: usize,
) -> RunResult<HashMap<ProcessId, WriterSender<P>>>
where
    A: ToSocketAddrs + Debug,
    P: Protocol + 'static,
{
    // spawn listener
    let mut rx = task::spawn_producer(channel_buffer_size, |tx| {
        super::listener_task(listener, tcp_nodelay, socket_buffer_size, tx)
    });

    // number of addresses
    let n = addresses.len();

    // create list of in and out connections:
    // - even though TCP is full-duplex, due to the current tokio non-parallel-tcp-socket-read-write
    //   limitation, we going to use in streams for reading and out streams for writing, which can
    //   be done in parallel
    let mut outgoing = Vec::with_capacity(n);
    let mut incoming = Vec::with_capacity(n);

    // connect to all addresses (outgoing)
    for address in addresses {
        let mut tries = 0;
        loop {
            match super::connect(&address, tcp_nodelay, socket_buffer_size).await {
                Ok(connection) => {
                    // save connection if connected successfully
                    outgoing.push(connection);
                    break;
                }
                Err(e) => {
                    // if not, try again if we shouldn't give up (due to too many attempts)
                    tries += 1;
                    if tries < connect_retries {
                        println!("failed to connect to {:?}: {}", address, e);
                        println!(
                            "will try again in 1 second ({} out of {})",
                            tries, connect_retries,
                        );
                        tokio::time::delay_for(Duration::from_secs(1)).await;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
    }

    // receive from listener all connected (incoming)
    for _ in 0..n {
        let connection = rx
            .recv()
            .await
            .expect("should receive connection from listener");
        incoming.push(connection);
    }

    let to_writers = handshake::<P>(
        process_id,
        to_workers,
        channel_buffer_size,
        incoming,
        outgoing,
    )
    .await;
    Ok(to_writers)
}

async fn handshake<P>(
    process_id: ProcessId,
    to_workers: ReaderToWorkers<P>,
    channel_buffer_size: usize,
    mut connections_0: Vec<Connection>,
    mut connections_1: Vec<Connection>,
) -> HashMap<ProcessId, WriterSender<P>>
where
    P: Protocol + 'static,
{
    // say hi to all on both connections
    say_hi(process_id, &mut connections_0).await;
    say_hi(process_id, &mut connections_1).await;
    println!("said hi to all processes");

    // receive hi from all on both connections
    let id_to_connection_0 = receive_hi(connections_0).await;
    let id_to_connection_1 = receive_hi(connections_1).await;
    println!(
        "received hi from all processes: {:?} | {:?}",
        id_to_connection_0.keys(),
        id_to_connection_1.keys()
    );

    // start readers and writers
    start_readers::<P>(to_workers, id_to_connection_0);
    start_writers::<P>(channel_buffer_size, id_to_connection_1)
}

async fn say_hi(process_id: ProcessId, connections: &mut Vec<Connection>) {
    let hi = ProcessHi(process_id);
    // send hi on each connection
    for connection in connections.iter_mut() {
        connection.send(&hi).await;
    }
}

async fn receive_hi(connections: Vec<Connection>) -> HashMap<ProcessId, Connection> {
    let mut id_to_connection = HashMap::with_capacity(connections.len());

    // receive hi from each connection
    for mut connection in connections {
        if let Some(ProcessHi(from)) = connection.recv().await {
            // save entry and check it has not been inserted before
            let res = id_to_connection.insert(from, connection);
            assert!(res.is_none());
        } else {
            panic!("error receiving hi");
        }
    }
    id_to_connection
}

/// Starts a reader task per connection received. A `ToWorkers` is passed to each reader so that
/// these can forward immediately to the correct worker process.
fn start_readers<P>(to_workers: ReaderToWorkers<P>, connections: HashMap<ProcessId, Connection>)
where
    P: Protocol + 'static,
{
    for (process_id, connection) in connections {
        let to_workers_clone = to_workers.clone();
        task::spawn(reader_task::<P>(to_workers_clone, process_id, connection));
    }
}

fn start_writers<P>(
    channel_buffer_size: usize,
    connections: HashMap<ProcessId, Connection>,
) -> HashMap<ProcessId, WriterSender<P>>
where
    P: Protocol + 'static,
{
    // mapping from process id to channel broadcast writer should write to
    let mut writers = HashMap::with_capacity(connections.len());

    // start on writer task per connection
    for (process_id, connection) in connections {
        // create channel where parent should write to
        let tx = task::spawn_consumer(channel_buffer_size, |rx| writer_task::<P>(connection, rx));
        writers.insert(process_id, tx);
    }

    writers
}

/// Reader task.
async fn reader_task<P>(
    mut to_workers: ReaderToWorkers<P>,
    process_id: ProcessId,
    mut connection: Connection,
) where
    P: Protocol + 'static,
{
    loop {
        match connection.recv().await {
            Some(msg) => {
                if let Err(e) = to_workers.forward((process_id, msg)).await {
                    println!(
                        "[reader] error notifying process task with new msg: {:?}",
                        e
                    );
                }
            }
            None => {
                println!("[reader] error receiving message from connection");
            }
        }
    }
}

/// Writer task.
async fn writer_task<P>(mut connection: Connection, mut parent: WriterReceiver<P>)
where
    P: Protocol + 'static,
{
    loop {
        if let Some(msg) = parent.recv().await {
            connection.send(msg).await;
        } else {
            println!("[writer] error receiving message from parent");
        }
    }
}

/// Starts the executor.
pub fn start_executor<P>(
    config: Config,
    channel_buffer_size: usize,
    from_clients: ClientReceiver,
) -> (CommandReceiver, ExecutionInfoSender<P>)
where
    P: Protocol + 'static,
{
    task::spawn_producer_and_consumer(channel_buffer_size, |tx, rx| {
        executor_task::<P>(config, tx, rx, from_clients)
    })
}

async fn executor_task<P>(
    config: Config,
    mut to_parent: CommandSender,
    mut from_parent: ExecutionInfoReceiver<P>,
    mut from_clients: ClientReceiver,
) where
    P: Protocol,
{
    // create executor
    let mut executor = P::Executor::new(config);

    // mapping from client id to its channel
    let mut clients = HashMap::new();

    loop {
        select! {
            execution_info = from_parent.recv().fuse() => {
                log!("[executor] from parent: {:?}", execution_info);
                if let Some(execution_info) = execution_info {
                    handle_execution_info::<P>(execution_info, &mut executor, &mut clients).await;
                } else {
                    println!("[executor] error while receiving execution info from parent");
                }
            }
            from_client = from_clients.recv().fuse() => {
                log!("[executor] from client: {:?}", from_client);
                if let Some(from_client) = from_client {
                    handle_from_client::<P>(from_client, &mut executor, &mut clients, &mut to_parent).await;
                } else {
                    println!("[executor] error while receiving new command from clients");
                }
            }
        }
    }
}

async fn handle_execution_info<P>(
    to_executor: Vec<<P::Executor as Executor>::ExecutionInfo>,
    executor: &mut P::Executor,
    clients: &mut HashMap<ClientId, CommandResultSender>,
) where
    P: Protocol,
{
    // get new commands ready
    let ready: Vec<_> = to_executor
        .into_iter()
        .flat_map(|info| executor.handle(info))
        .map(|result| result.unwrap_ready())
        .collect();

    for cmd_result in ready {
        // get client id
        let client_id = cmd_result.rifl().source();
        // get client channel
        let tx = clients
            .get_mut(&client_id)
            .expect("command result should belong to a registered client");

        // send command result to client
        if let Err(e) = tx.send(cmd_result).await {
            println!(
                "[executor] error while sending to command result to client {}: {:?}",
                client_id, e
            );
        }
    }
}

async fn handle_from_client<P>(
    from_client: FromClient,
    executor: &mut P::Executor,
    clients: &mut HashMap<ClientId, CommandResultSender>,
    to_parent: &mut CommandSender,
) where
    P: Protocol,
{
    match from_client {
        FromClient::Submit(cmd) => {
            // register in executor
            executor.register(cmd.rifl(), cmd.key_count());

            // send to command to parent
            if let Err(e) = to_parent.send(cmd).await {
                println!("[executor] error while sending to parent: {:?}", e);
            }
        }
        FromClient::Register(client_id, tx) => {
            println!("[executor] client {} registered", client_id);
            let res = clients.insert(client_id, tx);
            assert!(res.is_none());
        }
        FromClient::Unregister(client_id) => {
            println!("[executor] client {} unregistered", client_id);
            let res = clients.remove(&client_id);
            assert!(res.is_some());
        }
    }
}
