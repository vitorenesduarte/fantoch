// Implementation of the read-write task;
mod rw;

// Implementation of `ShardsPending`.
mod pending;

// Definition of `Batch`.
mod batch;

// Implementation of a batcher.
mod batcher;

// Implementation of an unbatcher.
mod unbatcher;

use crate::client::{Client, ClientData, Workload};
use crate::command::{Command, CommandResult};
use crate::hash_map::HashMap;
use crate::id::{ClientId, ProcessId, Rifl, ShardId};
use crate::run::chan::{self, ChannelReceiver, ChannelSender};
use crate::run::prelude::*;
use crate::run::rw::Connection;
use crate::run::task;
use crate::time::{RunTime, SysTime};
use crate::HashSet;
use crate::{info, trace, warn};
use color_eyre::Report;
use futures::stream::{FuturesUnordered, StreamExt};
use std::fmt::Debug;
use std::time::Duration;
use tokio::net::ToSocketAddrs;

const MAX_CLIENT_CONNECTIONS: usize = 32;

pub async fn client<A>(
    ids: Vec<ClientId>,
    addresses: Vec<A>,
    interval: Option<Duration>,
    workload: Workload,
    batch_max_size: usize,
    batch_max_delay: Duration,
    connect_retries: usize,
    tcp_nodelay: bool,
    channel_buffer_size: usize,
    status_frequency: Option<usize>,
    metrics_file: Option<String>,
) -> Result<(), Report>
where
    A: ToSocketAddrs + Clone + Debug + Send + 'static + Sync,
{
    // create client pool
    let mut pool = Vec::with_capacity(MAX_CLIENT_CONNECTIONS);
    // init each entry
    pool.resize_with(MAX_CLIENT_CONNECTIONS, Vec::new);

    // assign each client to a client worker
    ids.into_iter().enumerate().for_each(|(index, client_id)| {
        let index = index % MAX_CLIENT_CONNECTIONS;
        pool[index].push(client_id);
    });

    // start each client worker in pool
    let handles = pool.into_iter().filter_map(|client_ids| {
        // only start a client for this pool index if any client id was assigned
        // to it
        if !client_ids.is_empty() {
            // start the open loop client if some interval was provided
            let handle = if let Some(interval) = interval {
                task::spawn(open_loop_client::<A>(
                    client_ids,
                    addresses.clone(),
                    interval,
                    workload,
                    batch_max_size,
                    batch_max_delay,
                    connect_retries,
                    tcp_nodelay,
                    channel_buffer_size,
                    status_frequency,
                ))
            } else {
                task::spawn(closed_loop_client::<A>(
                    client_ids,
                    addresses.clone(),
                    workload,
                    batch_max_size,
                    batch_max_delay,
                    connect_retries,
                    tcp_nodelay,
                    channel_buffer_size,
                    status_frequency,
                ))
            };
            Some(handle)
        } else {
            None
        }
    });

    // wait for all clients to complete and aggregate their metrics
    let mut data = ClientData::new();

    let mut handles = handles.collect::<FuturesUnordered<_>>();
    while let Some(join_result) = handles.next().await {
        let clients = join_result?.expect("client should run correctly");
        for client in clients {
            info!("client {} ended", client.id());
            data.merge(client.data());
            info!("metrics from {} collected", client.id());
        }
    }

    if let Some(file) = metrics_file {
        info!("will write client data to {}", file);
        task::util::serialize_and_compress(&data, &file)?;
    }

    info!("all clients ended");
    Ok(())
}

async fn closed_loop_client<A>(
    client_ids: Vec<ClientId>,
    addresses: Vec<A>,
    workload: Workload,
    batch_max_size: usize,
    batch_max_delay: Duration,
    connect_retries: usize,
    tcp_nodelay: bool,
    channel_buffer_size: usize,
    status_frequency: Option<usize>,
) -> Option<Vec<Client>>
where
    A: ToSocketAddrs + Clone + Debug + Send + 'static + Sync,
{
    // create system time
    let time = RunTime;

    // setup client
    let (mut clients, mut unbatcher_rx, mut batcher_tx) = client_setup(
        client_ids,
        addresses,
        workload,
        batch_max_size,
        batch_max_delay,
        connect_retries,
        tcp_nodelay,
        channel_buffer_size,
        status_frequency,
    )
    .await?;

    // track which clients are finished (i.e. all their commands have completed)
    let mut finished = HashSet::with_capacity(clients.len());
    // track which clients are workload finished
    let mut workload_finished = HashSet::with_capacity(clients.len());

    // generate the first message of each client
    for client in clients.values_mut() {
        cmd_send(client, &time, &mut batcher_tx, &mut workload_finished).await;
    }

    // wait for results and generate/submit new commands while there are
    // commands to be generated
    while finished.len() < clients.len() {
        // and wait for next result
        let from_unbatcher = unbatcher_rx.recv().await;
        let ready_clients =
            cmd_recv(&mut clients, &time, from_unbatcher, &mut finished);
        for client_id in ready_clients {
            let client = clients
                .get_mut(&client_id)
                .expect("[client] ready client should exist");
            // if client hasn't finished, issue a new command
            cmd_send(client, &time, &mut batcher_tx, &mut workload_finished)
                .await;
        }
    }
    assert_eq!(workload_finished.len(), finished.len());

    // return clients
    Some(
        clients
            .into_iter()
            .map(|(_client_id, client)| client)
            .collect(),
    )
}

async fn open_loop_client<A>(
    client_ids: Vec<ClientId>,
    addresses: Vec<A>,
    interval: Duration,
    workload: Workload,
    batch_max_size: usize,
    batch_max_delay: Duration,
    connect_retries: usize,
    tcp_nodelay: bool,
    channel_buffer_size: usize,
    status_frequency: Option<usize>,
) -> Option<Vec<Client>>
where
    A: ToSocketAddrs + Clone + Debug + Send + 'static + Sync,
{
    // create system time
    let time = RunTime;

    // setup client
    let (mut clients, mut unbatcher_rx, mut batcher_tx) = client_setup(
        client_ids,
        addresses,
        workload,
        batch_max_size,
        batch_max_delay,
        connect_retries,
        tcp_nodelay,
        channel_buffer_size,
        status_frequency,
    )
    .await?;

    // create interval
    let mut interval = tokio::time::interval(interval);

    // track which clients are finished (i.e. all their commands have completed)
    let mut finished = HashSet::with_capacity(clients.len());
    // track which clients are workload finished
    let mut workload_finished = HashSet::with_capacity(clients.len());

    while finished.len() < clients.len() {
        tokio::select! {
            from_unbatcher = unbatcher_rx.recv() => {
                cmd_recv(
                    &mut clients,
                    &time,
                    from_unbatcher,
                    &mut finished,
                );
            }
            _ = interval.tick() => {
                // submit new command on every tick for each connected client
                // (if there are still commands to be generated)
                for (client_id, client) in clients.iter_mut(){
                    // if the client hasn't finished, try to issue a new command
                    if !workload_finished.contains(client_id) {
                        cmd_send(client, &time, &mut batcher_tx, &mut workload_finished).await;
                    }
                }
            }
        }
    }
    assert_eq!(workload_finished.len(), finished.len());

    // return clients
    Some(
        clients
            .into_iter()
            .map(|(_client_id, client)| client)
            .collect(),
    )
}

async fn client_setup<A>(
    client_ids: Vec<ClientId>,
    addresses: Vec<A>,
    workload: Workload,
    batch_max_size: usize,
    batch_max_delay: Duration,
    client_retries: usize,
    tcp_nodelay: bool,
    channel_buffer_size: usize,
    status_frequency: Option<usize>,
) -> Option<(
    HashMap<ClientId, Client>,
    ChannelReceiver<Vec<Rifl>>,
    ChannelSender<(ShardId, Command)>,
)>
where
    A: ToSocketAddrs + Clone + Debug + Send + 'static + Sync,
{
    let mut shard_to_process = HashMap::with_capacity(addresses.len());
    let mut connections = Vec::with_capacity(addresses.len());

    // connect to each address (one per shard)
    let tcp_buffer_size = 0;
    for address in addresses {
        let connect = task::connect(
            address,
            tcp_nodelay,
            tcp_buffer_size,
            client_retries,
        );
        let mut connection = match connect.await {
            Ok(connection) => connection,
            Err(e) => {
                // TODO panicking here as not sure how to make error handling
                // send + 'static (required by tokio::spawn) and
                // still be able to use the ? operator
                panic!(
                    "[client] error connecting at clients {:?}: {:?}",
                    client_ids, e
                );
            }
        };

        // say hi
        let (process_id, shard_id) =
            client_say_hi(client_ids.clone(), &mut connection).await?;

        // update set of processes to be discovered by the client
        assert!(shard_to_process.insert(shard_id, process_id).is_none(), "client shouldn't try to connect to the same shard more than once, only to the closest one");

        // update list of connected processes
        connections.push((process_id, connection));
    }

    // start client read-write task
    let (read, mut process_to_writer) = rw::start_client_rw_tasks(
        &client_ids,
        channel_buffer_size,
        connections,
    );

    // create mapping from shard id to client read-write task
    let shard_to_write = shard_to_process
        .into_iter()
        .map(|(shard_id, process_id)| {
            let writer = process_to_writer
                .remove(&process_id)
                .expect("a rw-task should exist for each process id");
            (shard_id, writer)
        })
        .collect();
    assert!(
        process_to_writer.is_empty(),
        "all rw-tasks should be associated with some shard"
    );

    // create clients
    let clients = client_ids
        .iter()
        .map(|&client_id| {
            let client = Client::new(client_id, workload, status_frequency);
            // no need to discover as the `unbatcher` will do the job of
            // selecting the closest process
            (client_id, client)
        })
        .collect();

    spawn_batcher_and_unbatcher(
        client_ids,
        batch_max_size,
        batch_max_delay,
        clients,
        channel_buffer_size,
        read,
        shard_to_write,
    )
    .await
}

async fn spawn_batcher_and_unbatcher(
    client_ids: Vec<ClientId>,
    batch_max_size: usize,
    batch_max_delay: Duration,
    clients: HashMap<ClientId, Client>,
    channel_buffer_size: usize,
    read: ChannelReceiver<CommandResult>,
    shard_to_writer: HashMap<ShardId, ChannelSender<ClientToServer>>,
) -> Option<(
    HashMap<ClientId, Client>,
    ChannelReceiver<Vec<Rifl>>,
    ChannelSender<(ShardId, Command)>,
)> {
    let (mut batcher_tx, batcher_rx) = chan::channel(channel_buffer_size);
    batcher_tx
        .set_name(format!("to_batcher_{}", super::util::ids_repr(&client_ids)));
    let (mut to_unbatcher_tx, to_unbatcher_rx) =
        chan::channel(channel_buffer_size);
    to_unbatcher_tx.set_name(format!(
        "to_unbatcher_{}",
        super::util::ids_repr(&client_ids)
    ));
    let (mut to_client_tx, to_client_rx) = chan::channel(channel_buffer_size);
    to_client_tx
        .set_name(format!("to_client_{}", super::util::ids_repr(&client_ids)));

    // spawn batcher
    task::spawn(batcher::batcher(
        batcher_rx,
        to_unbatcher_tx,
        batch_max_size,
        batch_max_delay,
    ));

    // spawn unbatcher
    task::spawn(unbatcher::unbatcher(
        to_unbatcher_rx,
        to_client_tx,
        read,
        shard_to_writer,
    ));

    // return clients and their means to communicate with the service
    Some((clients, to_client_rx, batcher_tx))
}

/// Generate the next command, returning a boolean representing whether a new
/// command was generated or not.
async fn cmd_send(
    client: &mut Client,
    time: &dyn SysTime,
    to_batcher: &mut ChannelSender<(ShardId, Command)>,
    workload_finished: &mut HashSet<ClientId>,
) {
    if let Some(next) = client.cmd_send(time) {
        if let Err(e) = to_batcher.send(next).await {
            warn!("[client] error forwarding batch: {:?}", e);
        }
    } else {
        // record that this client has finished its workload
        assert!(client.workload_finished());
        assert!(workload_finished.insert(client.id()));
    }
}

/// Handles new ready rifls. Returns the client ids of clients with a new
/// command finished.
fn cmd_recv(
    clients: &mut HashMap<ClientId, Client>,
    time: &dyn SysTime,
    from_unbatcher: Option<Vec<Rifl>>,
    finished: &mut HashSet<ClientId>,
) -> Vec<ClientId> {
    if let Some(rifls) = from_unbatcher {
        do_cmd_recv(clients, time, rifls, finished)
    } else {
        panic!("[client] error while receiving message from client read-write task");
    }
}

fn do_cmd_recv(
    clients: &mut HashMap<ClientId, Client>,
    time: &dyn SysTime,
    rifls: Vec<Rifl>,
    finished: &mut HashSet<ClientId>,
) -> Vec<ClientId> {
    rifls
        .into_iter()
        .map(move |rifl| {
            // find client that sent this command
            let client_id = rifl.source();
            let client = clients
                .get_mut(&client_id)
                .expect("[client] command result should belong to a client");

            // handle command results
            client.cmd_recv(rifl, time);

            // check if client is finished
            if client.finished() {
                // record that this client is finished
                info!("client {:?} exited loop", client_id);
                assert!(finished.insert(client_id));
            }
            client_id
        })
        .collect()
}

async fn client_say_hi(
    client_ids: Vec<ClientId>,
    connection: &mut Connection,
) -> Option<(ProcessId, ShardId)> {
    trace!("[client] will say hi with ids {:?}", client_ids);
    // say hi
    let hi = ClientHi(client_ids.clone());
    if let Err(e) = connection.send(&hi).await {
        warn!("[client] error while sending hi: {:?}", e);
    }

    // receive hi back
    if let Some(ProcessHi {
        process_id,
        shard_id,
    }) = connection.recv().await
    {
        trace!(
            "[client] clients {:?} received hi from process {} with shard id {}",
            client_ids,
            process_id,
            shard_id
        );
        Some((process_id, shard_id))
    } else {
        warn!("[client] clients {:?} couldn't receive process id from connected process", client_ids);
        None
    }
}
