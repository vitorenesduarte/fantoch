use super::connection::Connection;
use crate::command::Command;
use crate::id::{Dot, ProcessId};
use crate::log;
use crate::protocol::{Protocol, ToSend};
use crate::run::prelude::*;
use crate::run::task;
use rand::Rng;
use std::collections::HashMap;
use std::fmt::Debug;
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::task::JoinHandle;
use tokio::time::{self, Duration};

pub async fn connect_to_all<A, P>(
    process_id: ProcessId,
    listener: TcpListener,
    addresses: Vec<A>,
    to_workers: ReaderToWorkers<P>,
    connect_retries: usize,
    tcp_nodelay: bool,
    tcp_buffer_size: usize,
    tcp_flush_interval: Option<usize>,
    channel_buffer_size: usize,
    multiplexing: usize,
) -> RunResult<HashMap<ProcessId, Vec<WriterSender<P>>>>
where
    A: ToSocketAddrs + Debug,
    P: Protocol + 'static,
{
    // spawn listener
    let mut rx = task::spawn_producer(channel_buffer_size, |tx| {
        super::listener_task(listener, tcp_nodelay, tcp_buffer_size, tx)
    });

    // number of addresses
    let n = addresses.len();

    // create list of in and out connections:
    // - even though TCP is full-duplex, due to the current tokio non-parallel-tcp-socket-read-write
    //   limitation, we going to use in streams for reading and out streams for writing, which can
    //   be done in parallel
    let mut outgoing = Vec::with_capacity(n * multiplexing);
    let mut incoming = Vec::with_capacity(n * multiplexing);

    // connect to all addresses (outgoing)
    for address in addresses {
        // create `multiplexing` connections per address
        for _ in 0..multiplexing {
            let connection =
                super::connect(&address, tcp_nodelay, tcp_buffer_size, connect_retries).await?;
            // save connection if connected successfully
            outgoing.push(connection);
        }
    }

    // receive from listener all connected (incoming)
    for _ in 0..(n * multiplexing) {
        let connection = rx
            .recv()
            .await
            .expect("should receive connection from listener");
        incoming.push(connection);
    }

    let to_writers = handshake::<P>(
        process_id,
        n,
        to_workers,
        tcp_flush_interval,
        channel_buffer_size,
        incoming,
        outgoing,
    )
    .await;
    Ok(to_writers)
}

async fn handshake<P>(
    process_id: ProcessId,
    n: usize,
    to_workers: ReaderToWorkers<P>,
    tcp_flush_interval: Option<usize>,
    channel_buffer_size: usize,
    mut connections_0: Vec<Connection>,
    mut connections_1: Vec<Connection>,
) -> HashMap<ProcessId, Vec<WriterSender<P>>>
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

    // start readers and writers
    start_readers::<P>(to_workers, id_to_connection_0);
    start_writers::<P>(
        n,
        tcp_flush_interval,
        channel_buffer_size,
        id_to_connection_1,
    )
}

async fn say_hi(process_id: ProcessId, connections: &mut Vec<Connection>) {
    let hi = ProcessHi(process_id);
    // send hi on each connection
    for connection in connections.iter_mut() {
        connection.send(&hi).await;
    }
}

async fn receive_hi(connections: Vec<Connection>) -> Vec<(ProcessId, Connection)> {
    let mut id_to_connection = Vec::with_capacity(connections.len());

    // receive hi from each connection
    for mut connection in connections {
        if let Some(ProcessHi(from)) = connection.recv().await {
            // save entry and check it has not been inserted before
            id_to_connection.push((from, connection));
        } else {
            panic!("error receiving hi");
        }
    }
    id_to_connection
}

/// Starts a reader task per connection received. A `ToWorkers` is passed to each reader so that
/// these can forward immediately to the correct worker process.
fn start_readers<P>(to_workers: ReaderToWorkers<P>, connections: Vec<(ProcessId, Connection)>)
where
    P: Protocol + 'static,
{
    for (process_id, connection) in connections {
        let to_workers_clone = to_workers.clone();
        task::spawn(reader_task::<P>(to_workers_clone, process_id, connection));
    }
}

fn start_writers<P>(
    n: usize,
    tcp_flush_interval: Option<usize>,
    channel_buffer_size: usize,
    connections: Vec<(ProcessId, Connection)>,
) -> HashMap<ProcessId, Vec<WriterSender<P>>>
where
    P: Protocol + 'static,
{
    // mapping from process id to channel broadcast writer should write to
    let mut writers = HashMap::with_capacity(n);

    // start on writer task per connection
    for (process_id, connection) in connections {
        // create channel where parent should write to
        let tx = task::spawn_consumer(channel_buffer_size, |rx| {
            writer_task::<P>(tcp_flush_interval, connection, rx)
        });
        writers.entry(process_id).or_insert_with(Vec::new).push(tx);
    }

    // check `writers` size
    assert_eq!(writers.len(), n);
    writers
}

/// Reader task.
async fn reader_task<P>(
    mut reader_to_workers: ReaderToWorkers<P>,
    process_id: ProcessId,
    mut connection: Connection,
) where
    P: Protocol + 'static,
{
    loop {
        match connection.recv().await {
            Some(msg) => {
                if let Err(e) = reader_to_workers.forward((process_id, msg)).await {
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
async fn writer_task<P>(
    tcp_flush_interval: Option<usize>,
    mut connection: Connection,
    mut parent: WriterReceiver<P>,
) where
    P: Protocol + 'static,
{
    // if flush interval higher than 0, then flush periodically; otherwise, flush on every write
    if let Some(tcp_flush_interval) = tcp_flush_interval {
        // create interval
        let mut interval = time::interval(Duration::from_micros(tcp_flush_interval as u64));
        loop {
            tokio::select! {
                msg = parent.recv() => {
                    if let Some(msg) = msg {
                        connection.write(msg).await;
                    } else {
                        println!("[writer] error receiving message from parent");
                    }
                }
                _ = interval.tick() => {
                    // flush socket
                    connection.flush().await;
                }
            }
        }
    } else {
        loop {
            if let Some(msg) = parent.recv().await {
                connection.send(msg).await;
            } else {
                println!("[writer] error receiving message from parent");
            }
        }
    }
}

/// Starts process workers.
pub fn start_processes<P>(
    process: P,
    process_id: ProcessId,
    reader_to_workers_rxs: Vec<ReaderReceiver<P>>,
    client_to_workers_rxs: Vec<SubmitReceiver>,
    to_writers: HashMap<ProcessId, Vec<WriterSender<P>>>,
    worker_to_executors: WorkerToExecutors<P>,
) -> Vec<JoinHandle<()>>
where
    P: Protocol + Send + 'static,
{
    // zip rxs'
    let incoming = reader_to_workers_rxs
        .into_iter()
        .zip(client_to_workers_rxs.into_iter());

    // create executor workers
    incoming
        .map(|(from_readers, from_clients)| {
            task::spawn(process_task::<P>(
                process.clone(),
                process_id,
                from_readers,
                from_clients,
                to_writers.clone(),
                worker_to_executors.clone(),
            ))
        })
        .collect()
}

async fn process_task<P>(
    mut process: P,
    process_id: ProcessId,
    mut from_readers: ReaderReceiver<P>,
    mut from_clients: SubmitReceiver,
    mut to_writers: HashMap<ProcessId, Vec<WriterSender<P>>>,
    mut worker_to_executors: WorkerToExecutors<P>,
) where
    P: Protocol + 'static,
{
    loop {
        tokio::select! {
            msg = from_readers.recv() => {
                log!("[server] reader message: {:?}", msg);
                if let Some((from, msg)) = msg {
                    handle_from_processes(process_id, from, msg, &mut process, &mut to_writers, &mut worker_to_executors).await
                } else {
                    println!("[server] error while receiving new process message from readers");
                }
            }
            cmd = from_clients.recv() => {
                log!("[server] from clients: {:?}", cmd);
                if let Some((dot, cmd)) = cmd {
                    handle_from_client(process_id, dot, cmd, &mut process, &mut to_writers).await
                } else {
                    println!("[server] error while receiving new command from executor");
                }
            }
        }
    }
}

async fn handle_from_processes<P>(
    process_id: ProcessId,
    from: ProcessId,
    msg: P::Message,
    process: &mut P,
    to_writers: &mut HashMap<ProcessId, Vec<WriterSender<P>>>,
    worker_to_executors: &mut WorkerToExecutors<P>,
) where
    P: Protocol + 'static,
{
    // handle message in process
    if let Some(to_send) = process.handle(from, msg) {
        handle_to_send(process_id, to_send, process, to_writers).await;
    }

    // check if there's new execution info for the executor
    for execution_info in process.to_executor() {
        if let Err(e) = worker_to_executors.forward(execution_info).await {
            println!(
                "[server] error while sending new execution info to executor: {:?}",
                e
            );
        }
    }
}

async fn handle_to_send<P>(
    process_id: ProcessId,
    to_send: ToSend<P::Message>,
    process: &mut P,
    to_writers: &mut HashMap<ProcessId, Vec<WriterSender<P>>>,
) where
    P: Protocol + 'static,
{
    // unpack to send
    let ToSend { target, msg, .. } = to_send;
    for destination in target {
        if destination == process_id {
            // handle msg locally if self in `to_send.target`
            handle_message_from_self::<P>(process_id, msg.clone(), process)
        } else {
            // send message to correct writer
            send_to_writer::<P>(destination, msg.clone(), to_writers).await
        }
    }
}

fn handle_message_from_self<P>(process_id: ProcessId, msg: P::Message, process: &mut P)
where
    P: Protocol + 'static,
{
    // make sure that, if there's something to be sent, it is to self, i.e. messages from self to
    // self shouldn't generate messages TODO can we avoid cloning here?
    if let Some(ToSend { target, msg, .. }) = process.handle(process_id, msg) {
        assert!(target.len() == 1);
        assert!(target.contains(&process_id));
        // handling this message shouldn't generate a new message
        let nothing = process.handle(process_id, msg);
        assert!(nothing.is_none());
    }
}

async fn send_to_writer<P>(
    to: ProcessId,
    msg: P::Message,
    to_writers: &mut HashMap<ProcessId, Vec<WriterSender<P>>>,
) where
    P: Protocol + 'static,
{
    // find all writers
    let writers = to_writers
        .get_mut(&to)
        .expect("[server] identifier in target should have a writer");

    // pick a random one
    let writer_index = rand::thread_rng().gen_range(0, writers.len());

    if let Err(e) = writers[writer_index].send(msg).await {
        println!("[server] error while sending to broadcast writer: {:?}", e);
    }
}

async fn handle_from_client<P>(
    process_id: ProcessId,
    dot: Dot,
    cmd: Command,
    process: &mut P,
    to_writers: &mut HashMap<ProcessId, Vec<WriterSender<P>>>,
) where
    P: Protocol + 'static,
{
    // submit command in process
    let to_send = process.submit(Some(dot), cmd);
    handle_to_send(process_id, to_send, process, to_writers).await;
}
