use crate::client::{Client, ClientData, Workload};
use crate::command::{Command, CommandResult};
use crate::hash_map::{Entry, HashMap};
use crate::id::{ClientId, ProcessId, Rifl, ShardId};
use crate::run::chan;
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

async fn batcher() {}

async fn closed_loop_client<A>(
    client_ids: Vec<ClientId>,
    addresses: Vec<A>,
    workload: Workload,
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
    let (mut clients, mut reader, mut process_to_writer) = client_setup(
        client_ids,
        addresses,
        workload,
        connect_retries,
        tcp_nodelay,
        channel_buffer_size,
        status_frequency,
    )
    .await?;

    // create pending
    let mut pending = ShardsPending::new();

    // track which clients are finished (i.e. all their commands have completed)
    let mut finished = HashSet::with_capacity(clients.len());
    // track which clients are workload finished
    let mut workload_finished = HashSet::with_capacity(clients.len());

    // generate the first message of each client
    for (_client_id, client) in clients.iter_mut() {
        next_cmd(
            client,
            &time,
            &mut process_to_writer,
            &mut pending,
            &mut workload_finished,
        )
        .await;
    }

    // wait for results and generate/submit new commands while there are
    // commands to be generated
    while finished.len() < clients.len() {
        // and wait for next result
        let from_server = reader.recv().await;
        let client = handle_cmd_result(
            &mut clients,
            &time,
            from_server,
            &mut pending,
            &mut finished,
        );
        if let Some(client) = client {
            // if client hasn't finished, issue a new command
            next_cmd(
                client,
                &time,
                &mut process_to_writer,
                &mut pending,
                &mut workload_finished,
            )
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
    client_retries: usize,
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
    let (mut clients, mut reader, mut process_to_writer) = client_setup(
        client_ids,
        addresses,
        workload,
        client_retries,
        tcp_nodelay,
        channel_buffer_size,
        status_frequency,
    )
    .await?;

    // create pending
    let mut pending = ShardsPending::new();

    // create interval
    let mut interval = tokio::time::interval(interval);

    // track which clients are finished (i.e. all their commands have completed)
    let mut finished = HashSet::with_capacity(clients.len());
    // track which clients are workload finished
    let mut workload_finished = HashSet::with_capacity(clients.len());

    while finished.len() < clients.len() {
        tokio::select! {
            from_server = reader.recv() => {
                handle_cmd_result(&mut clients, &time, from_server, &mut pending, &mut finished);
            }
            _ = interval.tick() => {
                // submit new command on every tick for each connected client (if there are still commands to be generated)
                for (client_id, client) in clients.iter_mut(){
                    // if the client hasn't finished, try to issue a new command
                    if !workload_finished.contains(client_id) {
                        next_cmd(client, &time, &mut process_to_writer, &mut pending, &mut workload_finished).await;
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
    client_retries: usize,
    tcp_nodelay: bool,
    channel_buffer_size: usize,
    status_frequency: Option<usize>,
) -> Option<(
    HashMap<ClientId, Client>,
    ServerToClientReceiver,
    HashMap<ProcessId, ClientToServerSender>,
)>
where
    A: ToSocketAddrs + Clone + Debug + Send + 'static + Sync,
{
    let mut to_discover = HashMap::with_capacity(addresses.len());
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
        assert!(to_discover.insert(shard_id, process_id).is_none(), "client shouldn't try to connect to the same shard more than once, only to the closest one");

        // update list of connected processes
        connections.push((process_id, connection));
    }

    // start client read-write task
    let (read, process_to_write) =
        start_client_rw_tasks(&client_ids, channel_buffer_size, connections);

    // create clients
    let clients = client_ids
        .into_iter()
        .map(|client_id| {
            let mut client = Client::new(client_id, workload, status_frequency);
            // discover processes
            client.connect(to_discover.clone());
            (client_id, client)
        })
        .collect();

    // return clients and their means to communicate with the service
    Some((clients, read, process_to_write))
}

/// Generate the next command, returning a boolean representing whether a new
/// command was generated or not.
async fn next_cmd(
    client: &mut Client,
    time: &dyn SysTime,
    process_to_writer: &mut HashMap<ProcessId, ClientToServerSender>,
    pending: &mut ShardsPending,
    workload_finished: &mut HashSet<ClientId>,
) {
    if let Some((target_shard, cmd)) = client.next_cmd(time) {
        // register command in pending (which will aggregate several
        // `CommandResult`s if the command acesses more than one shard)
        pending.register(&cmd);

        // 1. register the command in all shards but the target shard
        for shard in cmd.shards().filter(|shard| **shard != target_shard) {
            let msg = ClientToServer::Register(cmd.clone());
            send_to_shard(&client, process_to_writer, shard, msg).await
        }

        // 2. submit the command to the target shard
        let msg = ClientToServer::Submit(cmd);
        send_to_shard(&client, process_to_writer, &target_shard, msg).await
    } else {
        // record that this client has finished its workload
        assert!(workload_finished.insert(client.id()));
    }
}

async fn send_to_shard(
    client: &Client,
    process_to_writer: &mut HashMap<ProcessId, ClientToServerSender>,
    shard_id: &ShardId,
    msg: ClientToServer,
) {
    // find closest process on this shard
    let process_id = client.shard_process(shard_id);
    // find process writer
    let writer = process_to_writer
        .get_mut(&process_id)
        .expect("[client] dind't find writer for target process");
    if let Err(e) = writer.send(msg).await {
        warn!("[client] error while sending message to client read-write task: {:?}", e);
    }
}

/// Handles a command result. Returns the client if a new COMMAND COMPLETED and
/// the client did NOT FINISH.
fn handle_cmd_result<'a>(
    clients: &'a mut HashMap<ClientId, Client>,
    time: &dyn SysTime,
    from_server: Option<CommandResult>,
    pending: &mut ShardsPending,
    finished: &mut HashSet<ClientId>,
) -> Option<&'a mut Client> {
    if let Some(cmd_result) = from_server {
        do_handle_cmd_result(clients, time, cmd_result, pending, finished)
    } else {
        panic!("[client] error while receiving message from client read-write task");
    }
}

fn do_handle_cmd_result<'a>(
    clients: &'a mut HashMap<ClientId, Client>,
    time: &dyn SysTime,
    cmd_result: CommandResult,
    pending: &mut ShardsPending,
    finished: &mut HashSet<ClientId>,
) -> Option<&'a mut Client> {
    if let Some(rifl) = pending.add(cmd_result) {
        let client_id = rifl.source();
        let client = clients
            .get_mut(&client_id)
            .expect("[client] command result should belong to a client");

        // handle command results and check if client is finished
        if client.cmd_finished(rifl, time) {
            // record that this client is finished
            info!("client {:?} exited loop", client_id);
            assert!(finished.insert(client_id));
            None
        } else {
            Some(client)
        }
    } else {
        // no new command completed, so return non
        None
    }
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

fn start_client_rw_tasks(
    client_ids: &Vec<ClientId>,
    channel_buffer_size: usize,
    connections: Vec<(ProcessId, Connection)>,
) -> (
    ServerToClientReceiver,
    HashMap<ProcessId, ClientToServerSender>,
) {
    // create server-to-client channels: although we keep one connection per
    // shard, we'll have all rw tasks will write to the same channel; this means
    // the client will read from a single channel (and potentially receive
    // messages from any of the shards)
    let (mut s2c_tx, s2c_rx) = chan::channel(channel_buffer_size);
    s2c_tx.set_name(format!(
        "server_to_client_{}",
        super::util::ids_repr(&client_ids)
    ));

    let mut process_to_tx = HashMap::with_capacity(connections.len());
    for (process_id, connection) in connections {
        // create client-to-server channels: since clients may send operations
        // to different shards, we create one client-to-rw channel per rw task
        let (mut c2s_tx, c2s_rx) = chan::channel(channel_buffer_size);
        c2s_tx.set_name(format!(
            "client_to_server_{}_{}",
            process_id,
            super::util::ids_repr(&client_ids)
        ));

        // spawn rw task
        task::spawn(client_rw_task(connection, s2c_tx.clone(), c2s_rx));
        process_to_tx.insert(process_id, c2s_tx);
    }
    (s2c_rx, process_to_tx)
}

async fn client_rw_task(
    mut connection: Connection,
    mut to_parent: ServerToClientSender,
    mut from_parent: ClientToServerReceiver,
) {
    loop {
        tokio::select! {
            to_client = connection.recv() => {
                trace!("[client_rw] to client: {:?}", to_client);
                if let Some(to_client) = to_client {
                    if let Err(e) = to_parent.send(to_client).await {
                        warn!("[client_rw] error while sending message from server to parent: {:?}", e);
                    }
                } else {
                    warn!("[client_rw] error while receiving message from server to parent");
                    break;
                }
            }
            to_server = from_parent.recv() => {
                trace!("[client_rw] from client: {:?}", to_server);
                if let Some(to_server) = to_server {
                    if let Err(e) = connection.send(&to_server).await {
                        warn!("[client_rw] error while sending message to server: {:?}", e);
                    }
                } else {
                    warn!("[client_rw] error while receiving message from parent to server");
                    // in this case it means that the parent (the client) is done, and so we can exit the loop
                    break;
                }
            }
        }
    }
}

struct Expected {
    shard_count: usize,
    total_key_count: usize,
}

struct ShardsPending {
    pending: HashMap<Rifl, (Expected, Vec<CommandResult>)>,
}

impl ShardsPending {
    fn new() -> Self {
        Self {
            pending: HashMap::new(),
        }
    }

    fn register(&mut self, cmd: &Command) {
        let rifl = cmd.rifl();
        trace!("c{}: register {:?}", rifl.source(), rifl);
        let expected = Expected {
            shard_count: cmd.shard_count(),
            total_key_count: cmd.total_key_count(),
        };
        let results = Vec::with_capacity(expected.shard_count);
        let res = self.pending.insert(rifl, (expected, results));
        assert!(res.is_none());
    }

    // Add new `CommandResult`. Return a `Rifl` if some command got the
    // `CommandResult`s from each of the shards accessed.
    fn add(&mut self, result: CommandResult) -> Option<Rifl> {
        let rifl = result.rifl();
        trace!("c{}: received {:?}", rifl.source(), rifl);
        match self.pending.entry(rifl) {
            Entry::Occupied(mut entry) => {
                let (expected, results) = entry.get_mut();
                // add new result
                results.push(result);

                trace!(
                    "c{}: {:?} {}/{}",
                    rifl.source(),
                    rifl,
                    results.len(),
                    expected.shard_count
                );

                // return results if we have one `CommandResult` per shard
                // - TODO: add an assert checking that indeed these
                //   `CommandResult` came from different shards, and are not
                //   sent by the same shard
                if results.len() == expected.shard_count {
                    // assert that all keys accessed got a result
                    let results_key_count: usize = results
                        .into_iter()
                        .map(|cmd_result| cmd_result.results().len())
                        .sum();
                    assert_eq!(results_key_count, expected.total_key_count);
                    entry.remove();
                    Some(rifl)
                } else {
                    None
                }
            }
            Entry::Vacant(_) => panic!(
                "received command result about a rifl we didn't register for"
            ),
        }
    }
}
