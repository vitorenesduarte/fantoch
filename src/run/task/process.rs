use super::{connection::Connection, ProcessHi};
use crate::command::{Command, CommandResult};
use crate::config::Config;
use crate::executor::Executor;
use crate::id::{ClientId, ProcessId};
use crate::log;
use crate::protocol::{Protocol, ToSend};
use crate::run::task;
use crate::run::FromClient;
use bytes::Bytes;
use futures::future::FutureExt;
use futures::select;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::Debug;
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::time::Duration;

pub async fn connect_to_all<A, V>(
    process_id: ProcessId,
    listener: TcpListener,
    addresses: Vec<A>,
    connect_retries: usize,
) -> Result<
    (
        UnboundedReceiver<(ProcessId, V)>,
        UnboundedSender<ToSend<V>>,
    ),
    Box<dyn Error>,
>
where
    A: ToSocketAddrs + Debug,
    V: Debug + Serialize + DeserializeOwned + Send + 'static,
{
    // spawn listener
    let mut rx = task::spawn_producer(|tx| super::listener_task(listener, tx));

    // number of addresses
    let n = addresses.len();

    // create list of in and out connections:
    // - even though TCP is full-duplex, due to the current tokio parallel-tcp-socket-read-write
    //   limitation, we going to use in streams for reading and out streams for writing, which can
    //   be done in parallel
    let mut outgoing = Vec::with_capacity(n);
    let mut incoming = Vec::with_capacity(n);

    // connect to all addresses (and get the writers)
    for address in addresses {
        let mut tries = 0;
        loop {
            match super::connect(&address).await {
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

    // receive from listener all connected (the readers)
    for _ in 0..n {
        let connection = rx
            .recv()
            .await
            .expect("should receive connection from listener");
        incoming.push(connection);
    }

    Ok(handshake::<V>(process_id, incoming, outgoing).await)
}

async fn handshake<V>(
    process_id: ProcessId,
    mut connections_0: Vec<Connection>,
    mut connections_1: Vec<Connection>,
) -> (
    UnboundedReceiver<(ProcessId, V)>,
    UnboundedSender<ToSend<V>>,
)
where
    V: Debug + Serialize + DeserializeOwned + Send + 'static,
{
    // say hi to all
    say_hi(process_id, &mut connections_0).await;
    say_hi(process_id, &mut connections_1).await;
    println!("said hi to all processes");

    // receive hi from all
    let id_to_connection_0 = receive_hi(connections_0).await;
    let id_to_connection_1 = receive_hi(connections_1).await;
    println!(
        "received hi from all processes: {:?} | {:?}",
        id_to_connection_0.keys(),
        id_to_connection_1.keys()
    );

    (
        start_readers::<V>(id_to_connection_0),
        start_broadcast_writer::<V>(process_id, id_to_connection_1),
    )
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

/// Starts a reader task per connection received and returns an unbounded channel to which
/// readers will write to.
fn start_readers<V>(
    connections: HashMap<ProcessId, Connection>,
) -> UnboundedReceiver<(ProcessId, V)>
where
    V: Debug + DeserializeOwned + Send + 'static,
{
    task::spawn_producers(connections, |(process_id, connection), tx| {
        reader_task::<V>(process_id, connection, tx)
    })
}

fn start_broadcast_writer<V>(
    process_id: ProcessId,
    connections: HashMap<ProcessId, Connection>,
) -> UnboundedSender<ToSend<V>>
where
    V: Serialize + Send + 'static,
{
    // mapping from process id to channel broadcast writer should write to
    let mut writers = HashMap::new();

    // start on writer task per connection
    for (process_id, connection) in connections {
        // create channel where parent should write to
        let tx = task::spawn_consumer(|rx| writer_task(connection, rx));
        writers.insert(process_id, tx);
    }

    // spawn broadcast writer
    task::spawn_consumer(|rx| broadcast_writer_task::<V>(process_id, writers, rx))
}

/// Reader task.
async fn reader_task<V>(
    process_id: ProcessId,
    mut connection: Connection,
    parent: UnboundedSender<(ProcessId, V)>,
) where
    V: Debug + DeserializeOwned + Send + 'static,
{
    loop {
        match connection.recv().await {
            Some(msg) => {
                if let Err(e) = parent.send((process_id, msg)) {
                    println!("[reader] error notifying parent task with new msg: {:?}", e);
                }
            }
            None => {
                println!("[reader] error receiving message from connection");
            }
        }
    }
}

/// Broadcast Writer task.
async fn broadcast_writer_task<V>(
    process_id: ProcessId,
    mut writers: HashMap<ProcessId, UnboundedSender<Bytes>>,
    mut parent: UnboundedReceiver<ToSend<V>>,
) where
    V: Serialize + Send + 'static,
{
    loop {
        if let Some(ToSend { target, msg, .. }) = parent.recv().await {
            // serialize message
            let bytes = Connection::serialize(&msg);
            for id in target {
                // only send if id different from self
                if id != process_id {
                    // find writer
                    let writer = writers
                        .get_mut(&id)
                        .expect("[broadcast_writer] identifier in target should have a writer");
                    if let Err(e) = writer.send(bytes.clone()) {
                        println!(
                            "[broadcast_writer] error sending bytes to writer {:?}: {:?}",
                            id, e
                        );
                    }
                }
            }
        } else {
            println!("[broadcast_writer] error receiving message from parent");
        }
    }
}

/// Writer task.
async fn writer_task(mut connection: Connection, mut parent: UnboundedReceiver<Bytes>) {
    loop {
        if let Some(bytes) = parent.recv().await {
            connection.send_serialized(bytes).await;
        } else {
            println!("[writer] error receiving message from parent");
        }
    }
}

/// Starts the executor.
pub fn start_executor<P>(
    config: Config,
    from_clients: UnboundedReceiver<FromClient>,
) -> (
    UnboundedReceiver<Command>,
    UnboundedSender<Vec<<P::Executor as Executor>::ExecutionInfo>>,
)
where
    P: Protocol + 'static,
{
    task::spawn_producer_and_consumer(|tx, rx| executor_task::<P>(config, tx, rx, from_clients))
}

async fn executor_task<P>(
    config: Config,
    to_parent: UnboundedSender<Command>,
    mut from_parent: UnboundedReceiver<Vec<<P::Executor as Executor>::ExecutionInfo>>,
    mut from_clients: UnboundedReceiver<FromClient>,
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
                    handle_execution_info::<P>(execution_info, &mut executor, &mut clients);
                } else {
                    println!("[executor] error while receiving execution info from parent");
                }
            }
            from_client = from_clients.recv().fuse() => {
                log!("[executor] from client: {:?}", from_client);
                if let Some(from_client) = from_client {
                    handle_from_client::<P>(from_client, &mut executor, &mut clients, &to_parent);
                } else {
                    println!("[executor] error while receiving new command from clients");
                }
            }
        }
    }
}

fn handle_execution_info<P>(
    execution_info: Vec<<P::Executor as Executor>::ExecutionInfo>,
    executor: &mut P::Executor,
    clients: &mut HashMap<ClientId, UnboundedSender<CommandResult>>,
) where
    P: Protocol,
{
    // get new commands ready
    let ready = executor.handle(execution_info);

    for cmd_result in ready {
        // get client id
        let client_id = cmd_result.rifl().source();
        // get client channel
        let tx = clients
            .get(&client_id)
            .expect("command result should belong to a registered client");

        // send command result to client
        if let Err(e) = tx.send(cmd_result) {
            println!(
                "[executor] error while sending to command result to client {}: {:?}",
                client_id, e
            );
        }
    }
}

fn handle_from_client<P>(
    from_client: FromClient,
    executor: &mut P::Executor,
    clients: &mut HashMap<ClientId, UnboundedSender<CommandResult>>,
    to_parent: &UnboundedSender<Command>,
) where
    P: Protocol,
{
    match from_client {
        FromClient::Submit(cmd) => {
            // register in executor
            executor.register(&cmd);

            // send to command to parent
            if let Err(e) = to_parent.send(cmd) {
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
