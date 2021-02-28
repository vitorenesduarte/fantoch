// This module contains executor's implementation.
pub mod executor;

// This module contains execution logger's implementation.
mod execution_logger;

// This module contains process's implementation.
pub mod process;

// This module contains client's implementation.
pub mod client;

// This module contains periodic's implementation.
pub mod periodic;

// This module contains ping's implementation.
pub mod ping;

// This module contains delay's implementation.
pub mod delay;

// This module contains periodic metrics's implementation.
pub mod metrics_logger;

use crate::config::Config;
use crate::id::{ProcessId, ShardId};
use crate::protocol::Protocol;
use crate::run::chan;
use crate::run::prelude::*;
use crate::run::rw::Connection;
use crate::run::task;
use crate::HashMap;
use crate::{trace, warn};
use color_eyre::Report;
use std::fmt::Debug;
use std::net::IpAddr;
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::time::{self, Duration};

pub async fn connect_to_all<A, P>(
    process_id: ProcessId,
    shard_id: ShardId,
    config: Config,
    listener: TcpListener,
    addresses: Vec<(A, Option<Duration>)>,
    to_workers: ReaderToWorkers<P>,
    to_executors: ToExecutors<P>,
    connect_retries: usize,
    tcp_nodelay: bool,
    tcp_buffer_size: usize,
    tcp_flush_interval: Option<Duration>,
    channel_buffer_size: usize,
    multiplexing: usize,
) -> Result<
    (
        HashMap<ProcessId, (ShardId, IpAddr, Option<Duration>)>,
        HashMap<ProcessId, Vec<WriterSender<P>>>,
    ),
    Report,
>
where
    A: ToSocketAddrs + Debug,
    P: Protocol + 'static,
{
    // check that (n-1 + shards-1) addresses were set
    let total = config.n() - 1 + config.shard_count() - 1;
    assert_eq!(
        addresses.len(),
        total,
        "addresses count should be (n-1 + shards-1)"
    );

    // compute the number of expected connections
    let total_connections = total * multiplexing;

    // spawn listener
    let mut from_listener = task::spawn_producer(channel_buffer_size, |tx| {
        task::listener_task(listener, tcp_nodelay, tcp_buffer_size, tx)
    });

    // create list of in and out connections:
    // - even though TCP is full-duplex, due to the current tokio
    //   non-parallel-tcp-socket-read-write limitation, we going to use in
    //   streams for reading and out streams for writing, which can be done in
    //   parallel
    let mut outgoing = Vec::with_capacity(total_connections);
    let mut incoming = Vec::with_capacity(total_connections);

    // connect to all addresses (outgoing)
    for (address, delay) in addresses {
        // create `multiplexing` connections per address
        for _ in 0..multiplexing {
            let mut connection = task::connect(
                &address,
                tcp_nodelay,
                tcp_buffer_size,
                connect_retries,
            )
            .await?;
            // maybe set delay
            if let Some(delay) = delay {
                connection.set_delay(delay);
            }
            // save connection if connected successfully
            outgoing.push(connection);
        }
    }

    // receive from listener all connected (incoming)
    for _ in 0..total_connections {
        let connection = from_listener
            .recv()
            .await
            .expect("should receive connection from listener");
        incoming.push(connection);
    }

    let res = handshake::<P>(
        process_id,
        shard_id,
        to_workers,
        to_executors,
        tcp_flush_interval,
        channel_buffer_size,
        incoming,
        outgoing,
    )
    .await;
    Ok(res)
}

async fn handshake<P>(
    process_id: ProcessId,
    shard_id: ShardId,
    to_workers: ReaderToWorkers<P>,
    to_executors: ToExecutors<P>,
    tcp_flush_interval: Option<Duration>,
    channel_buffer_size: usize,
    mut connections_0: Vec<Connection>,
    mut connections_1: Vec<Connection>,
) -> (
    HashMap<ProcessId, (ShardId, IpAddr, Option<Duration>)>,
    HashMap<ProcessId, Vec<WriterSender<P>>>,
)
where
    P: Protocol + 'static,
{
    // say hi to all on both connections
    say_hi(process_id, shard_id, &mut connections_0).await;
    say_hi(process_id, shard_id, &mut connections_1).await;
    trace!("said hi to all processes");

    // receive hi from all on both connections
    let id_to_connection_0 = receive_hi(connections_0).await;
    let id_to_connection_1 = receive_hi(connections_1).await;

    // start readers and writers
    start_readers::<P>(to_workers, to_executors, id_to_connection_0);
    start_writers::<P>(
        shard_id,
        tcp_flush_interval,
        channel_buffer_size,
        id_to_connection_1,
    )
    .await
}

async fn say_hi(
    process_id: ProcessId,
    shard_id: ShardId,
    connections: &mut Vec<Connection>,
) {
    let hi = ProcessHi {
        process_id,
        shard_id,
    };
    // send hi on each connection
    for connection in connections.iter_mut() {
        if let Err(e) = connection.send(&hi).await {
            warn!("error while sending hi to connection: {:?}", e)
        }
    }
}

async fn receive_hi(
    connections: Vec<Connection>,
) -> Vec<(ProcessId, ShardId, Connection)> {
    let mut id_to_connection = Vec::with_capacity(connections.len());

    // receive hi from each connection
    for mut connection in connections {
        if let Some(ProcessHi {
            process_id,
            shard_id,
        }) = connection.recv().await
        {
            id_to_connection.push((process_id, shard_id, connection));
        } else {
            panic!("error receiving hi");
        }
    }
    id_to_connection
}

/// Starts a reader task per connection received. A `ReaderToWorkers` is passed
/// to each reader so that these can forward immediately to the correct worker
/// process.
fn start_readers<P>(
    to_workers: ReaderToWorkers<P>,
    to_executors: ToExecutors<P>,
    connections: Vec<(ProcessId, ShardId, Connection)>,
) where
    P: Protocol + 'static,
{
    for (process_id, shard_id, connection) in connections {
        task::spawn(reader_task::<P>(
            to_workers.clone(),
            to_executors.clone(),
            process_id,
            shard_id,
            connection,
        ));
    }
}

async fn start_writers<P>(
    shard_id: ShardId,
    tcp_flush_interval: Option<Duration>,
    channel_buffer_size: usize,
    connections: Vec<(ProcessId, ShardId, Connection)>,
) -> (
    HashMap<ProcessId, (ShardId, IpAddr, Option<Duration>)>,
    HashMap<ProcessId, Vec<WriterSender<P>>>,
)
where
    P: Protocol + 'static,
{
    let mut ips = HashMap::with_capacity(connections.len());
    // mapping from process id to channel broadcast writer should write to
    let mut writers = HashMap::with_capacity(connections.len());

    // start on writer task per connection
    for (peer_id, peer_shard_id, connection) in connections {
        // save shard id, ip and connection delay
        let ip = connection
            .ip_addr()
            .expect("ip address should be set for outgoing connection");
        let delay = connection.delay();
        ips.insert(peer_id, (peer_shard_id, ip, delay));

        // get connection delay
        let connection_delay = connection.delay();

        // get list set of writers to this process and create writer channels
        let txs = writers.entry(peer_id).or_insert_with(Vec::new);
        let (mut writer_tx, writer_rx) = chan::channel(channel_buffer_size);

        // name the channel accordingly
        writer_tx.set_name(format!(
            "to_writer_{}_process_{}",
            txs.len(),
            peer_id
        ));

        // don't use a flush interval if this peer is in my region: a peer is in
        // my region if it has a different shard id
        let tcp_flush_interval = if peer_shard_id != shard_id {
            None
        } else {
            tcp_flush_interval
        };

        // spawn the writer task
        task::spawn(writer_task::<P>(
            tcp_flush_interval,
            connection,
            writer_rx,
        ));

        let tx = if let Some(delay) = connection_delay {
            // if connection has a delay, spawn a delay task for this writer
            let (mut delay_tx, delay_rx) = chan::channel(channel_buffer_size);

            // name the channel accordingly
            delay_tx.set_name(format!(
                "to_delay_{}_process_{}",
                txs.len(),
                peer_id
            ));

            // spawn delay task
            task::spawn(delay::delay_task(delay_rx, writer_tx, delay));

            // in this case, messages are first forward to the delay task, which
            // then forwards them to the writer task
            delay_tx
        } else {
            // if there's no connection delay, then send the messages directly
            // to the writer task
            writer_tx
        };

        // and add a new writer channel
        txs.push(tx);
    }

    (ips, writers)
}

/// Reader task.
async fn reader_task<P>(
    mut reader_to_workers: ReaderToWorkers<P>,
    mut to_executors: ToExecutors<P>,
    process_id: ProcessId,
    shard_id: ShardId,
    mut connection: Connection,
) where
    P: Protocol + 'static,
{
    loop {
        match connection.recv::<POEMessage<P>>().await {
            Some(msg) => match msg {
                POEMessage::Protocol(msg) => {
                    let forward = reader_to_workers
                        .forward((process_id, shard_id, msg))
                        .await;
                    if let Err(e) = forward {
                        warn!("[reader] error notifying process task with new msg: {:?}",e);
                    }
                }
                POEMessage::Executor(execution_info) => {
                    trace!("[reader] to executor {:?}", execution_info);
                    // notify executor
                    if let Err(e) = to_executors.forward(execution_info).await {
                        warn!("[reader] error while notifying executor with new execution info: {:?}", e);
                    }
                }
            },
            None => {
                warn!("[reader] error receiving message from connection");
                break;
            }
        }
    }
}

/// Writer task.
async fn writer_task<P>(
    tcp_flush_interval: Option<Duration>,
    mut connection: Connection,
    mut parent: WriterReceiver<P>,
) where
    P: Protocol + 'static,
{
    // track whether there's been a flush error on this connection
    let mut flush_error = false;
    // if flush interval higher than 0, then flush periodically; otherwise,
    // flush on every write
    if let Some(tcp_flush_interval) = tcp_flush_interval {
        // create interval
        let mut interval = time::interval(tcp_flush_interval);
        loop {
            tokio::select! {
                msg = parent.recv() => {
                    if let Some(msg) = msg {
                        // connection write *doesn't* flush
                        if let Err(e) = connection.write(&*msg).await {
                            warn!("[writer] error writing message in connection: {:?}", e);
                        }
                    } else {
                        warn!("[writer] error receiving message from parent");
                        break;
                    }
                }
                _ = interval.tick() => {
                    // flush socket
                    if let Err(e) = connection.flush().await {
                        // make sure we only log the error once
                        if !flush_error {
                            warn!("[writer] error flushing connection: {:?}", e);
                            flush_error = true;
                        }
                    }
                }
            }
        }
    } else {
        loop {
            if let Some(msg) = parent.recv().await {
                // connection write *does* flush
                if let Err(e) = connection.send(&*msg).await {
                    warn!(
                        "[writer] error sending message to connection: {:?}",
                        e
                    );
                }
            } else {
                warn!("[writer] error receiving message from parent");
                break;
            }
        }
    }
    warn!("[writer] exiting after failure");
}
