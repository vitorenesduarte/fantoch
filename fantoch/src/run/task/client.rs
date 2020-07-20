use crate::command::Command;
use crate::executor::{ExecutorResult, Pending};
use crate::id::{AtomicDotGen, ClientId, ProcessId, Rifl, ShardId};
use crate::kvs::Key;
use crate::log;
use crate::run::prelude::*;
use crate::run::rw::Connection;
use tokio::net::TcpListener;

pub fn start_listener(
    process_id: ProcessId,
    shard_id: ShardId,
    listener: TcpListener,
    atomic_dot_gen: Option<AtomicDotGen>,
    client_to_workers: ClientToWorkers,
    client_to_executors: ClientToExecutors,
    tcp_nodelay: bool,
    channel_buffer_size: usize,
) {
    super::spawn(client_listener_task(
        process_id,
        shard_id,
        listener,
        atomic_dot_gen,
        client_to_workers,
        client_to_executors,
        tcp_nodelay,
        channel_buffer_size,
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
    channel_buffer_size: usize,
) {
    // start listener task
    let tcp_buffer_size = 0;
    let mut rx = super::spawn_producer(channel_buffer_size, |tx| {
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
                    channel_buffer_size,
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
    channel_buffer_size: usize,
    mut connection: Connection,
) {
    let client = server_receive_hi(
        process_id,
        shard_id,
        channel_buffer_size,
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
    let mut pending = Pending::new(aggregate);

    loop {
        tokio::select! {
            executor_result = executor_results.recv() => {
                log!("[client_server] new executor result: {:?}", executor_result);
                client_server_task_handle_executor_result(executor_result, &mut connection, &mut pending).await;
            }
            cmd = connection.recv() => {
                log!("[client_server] new command: {:?}", cmd);
                if !client_server_task_handle_cmd(cmd, &client_ids, &atomic_dot_gen, &mut client_to_workers, &mut client_to_executors, &mut rifl_acks,&mut pending).await {
                    return;
                }
            }
        }
    }
}

async fn server_receive_hi(
    process_id: ProcessId,
    shard_id: ShardId,
    channel_buffer_size: usize,
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
    let (mut rifl_acks_tx, rifl_acks_rx) = super::channel(channel_buffer_size);
    let (mut executor_results_tx, executor_results_rx) =
        super::channel(channel_buffer_size);

    // set channels name
    let ids_repr = ids_repr(&client_ids);
    rifl_acks_tx.set_name(format!("client_server_rifl_acks_{}", ids_repr));
    executor_results_tx
        .set_name(format!("client_server_executor_results_{}", ids_repr));

    // register clients in all executors
    if let Err(e) = client_to_executors
        .broadcast(FromClient::Register(
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

async fn client_server_task_handle_cmd(
    cmd: Option<Command>,
    client_ids: &Vec<ClientId>,
    atomic_dot_gen: &Option<AtomicDotGen>,
    client_to_workers: &mut ClientToWorkers,
    client_to_executors: &mut ClientToExecutors,
    rifl_acks: &mut RiflAckReceiver,
    pending: &mut Pending,
) -> bool {
    if let Some(cmd) = cmd {
        // get command rifl
        let rifl = cmd.rifl();

        if client_to_executors.pool_size() > 1 {
            // if there's more than one executor, then we'll receive partial
            // results; in this case, register command in pending
            pending.wait_for(&cmd);

            // TODO should we make the following two loops run in parallel?
            for key in cmd.keys() {
                client_server_task_register_rifl(
                    key,
                    rifl,
                    client_to_executors,
                )
                .await;
            }
            for _ in cmd.keys() {
                client_server_task_wait_rifl_register_ack(rifl, rifl_acks)
                    .await;
            }
        } else {
            // if there's a single executor, send a single `WaitForRifl`
            debug_assert!(client_to_executors.pool_size() == 1);

            // find any key
            let key = cmd
                .keys()
                .next()
                .expect("command should have at least one key");

            // register rifl and wait for ack
            client_server_task_register_rifl(key, rifl, client_to_executors)
                .await;
            client_server_task_wait_rifl_register_ack(rifl, rifl_acks).await;
        }

        // create dot for this command
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
        true
    } else {
        println!("[client_server] client disconnected.");
        // unregister client in all executors
        if let Err(e) = client_to_executors
            .broadcast(FromClient::Unregister(client_ids.clone()))
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

async fn client_server_task_register_rifl(
    key: &Key,
    rifl: Rifl,
    client_to_executors: &mut ClientToExecutors,
) {
    let forward = client_to_executors
        .forward_map((key, rifl), |(_, rifl)| FromClient::WaitForRifl(rifl));
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

pub fn start_client_rw_task(
    channel_buffer_size: usize,
    connection: Connection,
) -> (CommandResultReceiver, CommandSender) {
    super::spawn_producer_and_consumer(channel_buffer_size, |tx, rx| {
        client_rw_task(connection, tx, rx)
    })
}

async fn client_rw_task(
    mut connection: Connection,
    mut to_parent: CommandResultSender,
    mut from_parent: CommandReceiver,
) {
    loop {
        tokio::select! {
            cmd_result = connection.recv() => {
                log!("[client_rw] from connection: {:?}", cmd_result);
                if let Some(cmd_result) = cmd_result {
                    if let Err(e) = to_parent.send(cmd_result).await {
                        println!("[client_rw] error while sending command result to parent: {:?}", e);
                    }
                } else {
                    println!("[client_rw] error while receiving new command result from connection");
                }
            }
            cmd = from_parent.recv() => {
                log!("[client_rw] from parent: {:?}", cmd);
                if let Some(cmd) = cmd {
                    connection.send(&cmd).await;
                } else {
                    println!("[client_rw] error while receiving new command from parent");
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
