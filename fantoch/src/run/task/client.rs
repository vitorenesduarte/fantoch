use crate::command::Command;
use crate::executor::{AggregatePending, ExecutorResult};
use crate::id::{AtomicDotGen, ClientId, ProcessId, ShardId};
use crate::run::prelude::*;
use crate::run::rw::Connection;
use crate::HashMap;
use crate::{info, trace, warn};
use tokio::net::TcpListener;

pub fn start_listener(
    process_id: ProcessId,
    shard_id: ShardId,
    listener: TcpListener,
    atomic_dot_gen: Option<AtomicDotGen>,
    client_to_workers: ClientToWorkers,
    client_to_executors: ClientToExecutors,
    tcp_nodelay: bool,
    client_channel_buffer_size: usize,
) {
    super::spawn(client_listener_task(
        process_id,
        shard_id,
        listener,
        atomic_dot_gen,
        client_to_workers,
        client_to_executors,
        tcp_nodelay,
        client_channel_buffer_size,
    ));
}

/// Listen on new client connections and spawn a client task for each new
/// connection.
async fn client_listener_task(
    process_id: ProcessId,
    shard_id: ShardId,
    listener: TcpListener,
    atomic_dot_gen: Option<AtomicDotGen>,
    client_to_workers: ClientToWorkers,
    client_to_executors: ClientToExecutors,
    tcp_nodelay: bool,
    client_channel_buffer_size: usize,
) {
    // start listener task
    let tcp_buffer_size = 0;
    let mut rx = super::spawn_producer(client_channel_buffer_size, |tx| {
        super::listener_task(listener, tcp_nodelay, tcp_buffer_size, tx)
    });

    loop {
        // handle new client connections
        match rx.recv().await {
            Some(connection) => {
                trace!("[client_listener] new connection");
                // start client server task and give it the producer-end of the
                // channel in order for this client to notify
                // parent
                super::spawn(client_server_task(
                    process_id,
                    shard_id,
                    atomic_dot_gen.clone(),
                    client_to_workers.clone(),
                    client_to_executors.clone(),
                    client_channel_buffer_size,
                    connection,
                ));
            }
            None => {
                warn!(
                    "[client_listener] error receiving message from listener"
                );
            }
        }
    }
}

/// Client server-side task. Checks messages both from the client connection
/// (new commands) and parent (new command results).
async fn client_server_task(
    process_id: ProcessId,
    shard_id: ShardId,
    atomic_dot_gen: Option<AtomicDotGen>,
    mut client_to_workers: ClientToWorkers,
    mut client_to_executors: ClientToExecutors,
    client_channel_buffer_size: usize,
    mut connection: Connection,
) {
    let client = server_receive_hi(
        process_id,
        shard_id,
        client_channel_buffer_size,
        &mut connection,
        &mut client_to_executors,
    )
    .await;
    if client.is_none() {
        warn!("[client_server] giving up on new client {:?} since handshake failed:", connection);
        return;
    }
    let (client_ids, mut executor_results) = client.unwrap();

    // create pending
    let mut pending = AggregatePending::new(process_id, shard_id);

    loop {
        tokio::select! {
            executor_result = executor_results.recv() => {
                trace!("[client_server] new executor result: {:?}", executor_result);
                client_server_task_handle_executor_result(executor_result, &mut connection, &mut pending).await;
            }
            from_client = connection.recv() => {
                trace!("[client_server] from client: {:?}", from_client);
                if !client_server_task_handle_from_client(from_client, &client_ids, &atomic_dot_gen, &mut client_to_workers, &mut client_to_executors, &mut pending).await {
                    return;
                }
            }
        }
    }
}

async fn server_receive_hi(
    process_id: ProcessId,
    shard_id: ShardId,
    client_channel_buffer_size: usize,
    connection: &mut Connection,
    client_to_executors: &mut ClientToExecutors,
) -> Option<(Vec<ClientId>, ExecutorResultReceiver)> {
    // receive hi from client
    let client_ids = if let Some(ClientHi(client_ids)) = connection.recv().await
    {
        trace!("[client_server] received hi from clients {:?}", client_ids);
        client_ids
    } else {
        warn!(
            "[client_server] couldn't receive client ids from connected client"
        );
        return None;
    };

    // create channel where the executors will write executor results
    let (mut executor_results_tx, executor_results_rx) =
        super::channel(client_channel_buffer_size);

    // set channels name
    let ids_repr = ids_repr(&client_ids);
    executor_results_tx
        .set_name(format!("client_server_executor_results_{}", ids_repr));

    // register clients in all executors
    let register =
        ClientToExecutor::Register(client_ids.clone(), executor_results_tx);
    if let Err(e) = client_to_executors.broadcast(register).await {
        warn!(
            "[client_server] error while registering clients in executors: {:?}",
            e
        );
    }

    // say hi back
    let hi = ProcessHi {
        process_id,
        shard_id,
    };
    connection.send(&hi).await;

    // return client id and channel where client should read executor results
    Some((client_ids, executor_results_rx))
}

async fn client_server_task_handle_from_client(
    from_client: Option<ClientToServer>,
    client_ids: &Vec<ClientId>,
    atomic_dot_gen: &Option<AtomicDotGen>,
    client_to_workers: &mut ClientToWorkers,
    client_to_executors: &mut ClientToExecutors,
    pending: &mut AggregatePending,
) -> bool {
    if let Some(from_client) = from_client {
        client_server_task_handle_cmd(
            from_client,
            atomic_dot_gen,
            client_to_workers,
            pending,
        )
        .await;
        true
    } else {
        info!("[client_server] client disconnected.");
        // unregister client in all executors
        if let Err(e) = client_to_executors
            .broadcast(ClientToExecutor::Unregister(client_ids.clone()))
            .await
        {
            warn!(
                "[client_server] error while unregistering client in executors: {:?}",
                e
            );
        }
        false
    }
}

async fn client_server_task_handle_cmd(
    from_client: ClientToServer,
    atomic_dot_gen: &Option<AtomicDotGen>,
    client_to_workers: &mut ClientToWorkers,
    pending: &mut AggregatePending,
) {
    match from_client {
        ClientToServer::Register(cmd) => {
            // only register the command
            client_server_task_register_cmd(&cmd, pending).await;
        }
        ClientToServer::Submit(cmd) => {
            // register the command and submit it
            client_server_task_register_cmd(&cmd, pending).await;

            // create dot for this command (if we have a dot gen)
            let dot = atomic_dot_gen
                .as_ref()
                .map(|atomic_dot_gen| atomic_dot_gen.next_id());
            // forward command to worker process
            if let Err(e) = client_to_workers.forward((dot, cmd)).await {
                warn!(
                    "[client_server] error while sending new command to protocol worker: {:?}",
                    e
                );
            }
        }
    }
}

async fn client_server_task_register_cmd(
    cmd: &Command,
    pending: &mut AggregatePending,
) {
    // we'll receive partial
    // results from the executor, thus  register command in pending
    pending.wait_for(&cmd);
}

async fn client_server_task_handle_executor_result(
    executor_result: Option<ExecutorResult>,
    connection: &mut Connection,
    pending: &mut AggregatePending,
) {
    if let Some(executor_result) = executor_result {
        if let Some(cmd_result) = pending.add_executor_result(executor_result) {
            connection.send(&cmd_result).await;
        }
    } else {
        warn!("[client_server] error while receiving new executor result from executor");
    }
}

pub async fn client_say_hi(
    client_ids: Vec<ClientId>,
    connection: &mut Connection,
) -> Option<(ProcessId, ShardId)> {
    trace!("[client] will say hi with ids {:?}", client_ids);
    // say hi
    let hi = ClientHi(client_ids.clone());
    connection.send(&hi).await;

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

pub fn start_client_rw_tasks(
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
    let (mut s2c_tx, s2c_rx) = super::channel(channel_buffer_size);
    s2c_tx.set_name(format!("server_to_client_{}", ids_repr(&client_ids)));

    let mut process_to_tx = HashMap::with_capacity(connections.len());
    for (process_id, connection) in connections {
        // create client-to-server channels: since clients may send operations
        // to different shards, we create one client-to-rw channel per rw task
        let (mut c2s_tx, c2s_rx) = super::channel(channel_buffer_size);
        c2s_tx.set_name(format!(
            "client_to_server_{}_{}",
            process_id,
            ids_repr(&client_ids)
        ));

        // spawn rw task
        super::spawn(client_rw_task(connection, s2c_tx.clone(), c2s_rx));
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
                    connection.send(&to_server).await;
                } else {
                    warn!("[client_rw] error while receiving message from parent to server");
                    // in this case it means that the parent (the client) is done, and so we can exit the loop
                    break;
                }
            }
        }
    }
}

pub fn ids_repr(client_ids: &Vec<ClientId>) -> String {
    client_ids
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join("-")
}
