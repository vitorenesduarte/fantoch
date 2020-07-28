use crate::command::Command;
use crate::executor::{ExecutorResult, Pending};
use crate::id::{AtomicDotGen, ClientId, ProcessId, Rifl, ShardId};
use crate::kvs::Key;
use crate::log;
use crate::run::prelude::*;
use crate::run::rw::Connection;
use crate::HashMap;
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
                log!("[client_listener] new connection");
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
                println!(
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
        println!("[client_server] giving up on new client {:?} since handshake failed:", connection);
        return;
    }
    let (client_ids, mut rifl_acks, mut executor_results) = client.unwrap();

    // create pending
    let aggregate = true;
    let mut pending = Pending::new(aggregate, process_id, shard_id);

    loop {
        tokio::select! {
            executor_result = executor_results.recv() => {
                log!("[client_server] new executor result: {:?}", executor_result);
                client_server_task_handle_executor_result(executor_result, &mut connection, &mut pending).await;
            }
            from_client = connection.recv() => {
                log!("[client_server] from client: {:?}", from_client);
                if !client_server_task_handle_from_client(from_client, shard_id, &client_ids, &atomic_dot_gen, &mut client_to_workers, &mut client_to_executors, &mut rifl_acks, &mut pending).await {
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
) -> Option<(Vec<ClientId>, RiflAckReceiver, ExecutorResultReceiver)> {
    // receive hi from client
    let client_ids = if let Some(ClientHi(client_ids)) = connection.recv().await
    {
        log!("[client_server] received hi from clients {:?}", client_ids);
        client_ids
    } else {
        println!(
            "[client_server] couldn't receive client ids from connected client"
        );
        return None;
    };

    // create channel where the executors will write:
    // - ack rifl after wait_for_rifl
    // - executor results
    let (mut rifl_acks_tx, rifl_acks_rx) = super::channel(client_channel_buffer_size);
    let (mut executor_results_tx, executor_results_rx) =
        super::channel(client_channel_buffer_size);

    // set channels name
    let ids_repr = ids_repr(&client_ids);
    rifl_acks_tx.set_name(format!("client_server_rifl_acks_{}", ids_repr));
    executor_results_tx
        .set_name(format!("client_server_executor_results_{}", ids_repr));

    // register clients in all executors
    if let Err(e) = client_to_executors
        .broadcast(ClientToExecutor::Register(
            client_ids.clone(),
            rifl_acks_tx,
            executor_results_tx,
        ))
        .await
    {
        println!(
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
    Some((client_ids, rifl_acks_rx, executor_results_rx))
}

async fn client_server_task_handle_from_client(
    from_client: Option<ClientToServer>,
    shard_id: ShardId,
    client_ids: &Vec<ClientId>,
    atomic_dot_gen: &Option<AtomicDotGen>,
    client_to_workers: &mut ClientToWorkers,
    client_to_executors: &mut ClientToExecutors,
    rifl_acks: &mut RiflAckReceiver,
    pending: &mut Pending,
) -> bool {
    if let Some(from_client) = from_client {
        client_server_task_handle_cmd(
            from_client,
            shard_id,
            atomic_dot_gen,
            client_to_workers,
            client_to_executors,
            rifl_acks,
            pending,
        )
        .await;
        true
    } else {
        println!("[client_server] client disconnected.");
        // unregister client in all executors
        if let Err(e) = client_to_executors
            .broadcast(ClientToExecutor::Unregister(client_ids.clone()))
            .await
        {
            println!(
                "[client_server] error while unregistering client in executors: {:?}",
                e
            );
        }
        false
    }
}

async fn client_server_task_handle_cmd(
    from_client: ClientToServer,
    shard_id: ShardId,
    atomic_dot_gen: &Option<AtomicDotGen>,
    client_to_workers: &mut ClientToWorkers,
    client_to_executors: &mut ClientToExecutors,
    rifl_acks: &mut RiflAckReceiver,
    pending: &mut Pending,
) {
    match from_client {
        ClientToServer::Register(cmd) => {
            // only register the command
            client_server_task_register_cmd(
                &cmd,
                shard_id,
                client_to_executors,
                rifl_acks,
                pending,
            )
            .await;
        }
        ClientToServer::Submit(cmd) => {
            // register the command and submit it
            client_server_task_register_cmd(
                &cmd,
                shard_id,
                client_to_executors,
                rifl_acks,
                pending,
            )
            .await;

            // create dot for this command (if we have a dot gen)
            let dot = atomic_dot_gen
                .as_ref()
                .map(|atomic_dot_gen| atomic_dot_gen.next_id());
            // forward command to worker process
            if let Err(e) = client_to_workers.forward((dot, cmd)).await {
                println!(
                "[client_server] error while sending new command to protocol worker: {:?}",
                e
            );
            }
        }
    }
}

async fn client_server_task_register_cmd(
    cmd: &Command,
    shard_id: ShardId,
    client_to_executors: &mut ClientToExecutors,
    rifl_acks: &mut RiflAckReceiver,
    pending: &mut Pending,
) {
    let rifl = cmd.rifl();
    if client_to_executors.pool_size() > 1 {
        // if there's more than one executor, then we'll receive partial
        // results; in this case, register command in pending
        pending.wait_for(&cmd);

        // TODO should we make the following two loops run in parallel?
        for key in cmd.keys(shard_id) {
            client_server_task_register_rifl(key, rifl, client_to_executors)
                .await;
        }
        for _ in 0..cmd.key_count(shard_id) {
            client_server_task_wait_rifl_register_ack(rifl, rifl_acks).await;
        }
    } else {
        debug_assert!(client_to_executors.pool_size() == 1);
        // if there's a single executor, send a single `WaitForRifl`; since the
        // selected key doesn't matter, find any
        let key = cmd
            .keys(shard_id)
            .next()
            .expect("command should have at least one key");

        // register rifl and wait for ack
        client_server_task_register_rifl(key, rifl, client_to_executors).await;
        client_server_task_wait_rifl_register_ack(rifl, rifl_acks).await;
    }
}

async fn client_server_task_register_rifl(
    key: &Key,
    rifl: Rifl,
    client_to_executors: &mut ClientToExecutors,
) {
    let forward = client_to_executors.forward_map((key, rifl), |(_, rifl)| {
        ClientToExecutor::WaitForRifl(rifl)
    });
    if let Err(e) = forward.await {
        println!("[client_server] error while registering new command in executor: {:?}", e);
    }
}

async fn client_server_task_wait_rifl_register_ack(
    rifl: Rifl,
    rifl_acks: &mut RiflAckReceiver,
) {
    if let Some(r) = rifl_acks.recv().await {
        assert_eq!(r, rifl);
    } else {
        println!("[client_server] couldn't receive rifl ack from executor");
    }
}

async fn client_server_task_handle_executor_result(
    executor_result: Option<ExecutorResult>,
    connection: &mut Connection,
    pending: &mut Pending,
) {
    if let Some(executor_result) = executor_result {
        match executor_result {
            ExecutorResult::Ready(cmd_result) => {
                // if the command result is ready, simply send ti
                connection.send(&cmd_result).await;
            }
            ExecutorResult::Partial(rifl, key, op_result) => {
                if let Some(result) =
                    pending.add_partial(rifl, || (key, op_result))
                {
                    // since pending is in aggregate mode, if there's a result,
                    // then it's ready
                    let cmd_result = result.unwrap_ready();
                    connection.send(&cmd_result).await;
                }
            }
        }
    } else {
        println!("[client_server] error while receiving new executor result from executor");
    }
}

pub async fn client_say_hi(
    client_ids: Vec<ClientId>,
    connection: &mut Connection,
) -> Option<(ProcessId, ShardId)> {
    log!("[client] will say hi with ids {:?}", client_ids);
    // say hi
    let hi = ClientHi(client_ids.clone());
    connection.send(&hi).await;

    // receive hi back
    if let Some(ProcessHi {
        process_id,
        shard_id,
    }) = connection.recv().await
    {
        log!(
            "[client] clients {:?} received hi from process {} with shard id {}",
            client_ids,
            process_id,
            shard_id
        );
        Some((process_id, shard_id))
    } else {
        println!("[client] clients {:?} couldn't receive process id from connected process", client_ids);
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
                log!("[client_rw] to client: {:?}", to_client);
                if let Some(to_client) = to_client {
                    if let Err(e) = to_parent.send(to_client).await {
                        println!("[client_rw] error while sending message from server to parent: {:?}", e);
                    }
                } else {
                    println!("[client_rw] error while receiving message from server to parent");
                    break;
                }
            }
            to_server = from_parent.recv() => {
                log!("[client_rw] from client: {:?}", to_server);
                if let Some(to_server) = to_server {
                    connection.send(&to_server).await;
                } else {
                    println!("[client_rw] error while receiving message from parent to server");
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
