// The architecture of this runner was thought in a way that allows all
/// protocols that implement the `Protocol` trait to achieve their maximum
/// throughput. Below we detail all key decisions.
///
/// We assume:
/// - C clients
/// - E executors
/// - P protocol processes
///
/// 1. When a client connects for the first time it registers itself in all
/// executors. This register request contains the channel in which executors
/// should write command results (potentially partial command results if the
/// command is multi-key).
///
/// 2. When a client issues a command, it registers this command in all
/// executors that are responsible for executing this command. This is how each
/// executor knows if it should notify this client when the command is executed.
/// If the commmand is single-key, this command only needs to be registered in
/// one executor. If multi-key, it needs to be registered in several executors
/// if the keys accessed by the command are assigned to different executors.
///
/// 3. Once the command registration occurs (and the client must wait for an ack
/// from the executor, otherwise the execution info can reach the executor
/// before the "wait for rifl" registration from the client), the command is
/// forwarded to *ONE* protocol process (even if the command is multi-key). This
/// single protocol process *needs to* be chosen by looking the message
/// identifier `Dot`. Using the keys being accessed by the command will not work
/// for all cases, for example, when recovering and the payload is not known, we
/// only have acesss to a `noOp` meaning that we would need to broadcast to all
/// processes, which would be tricky to get correctly. In particular,
/// when the command is being submitted, its `Dot` has not been computed yet. So
/// the idea here is for parallel protocols to have the `DotGen` outside and
/// once the `Dot` is computed, the submit is forwarded to the correct protocol
/// process. For maximum parallelism, this generator can live in the clients and
/// have a lock-free implementation (see `AtomicIdGen`).
//
/// 4. When the protocol process receives the new command from a client it does
/// whatever is specified in the `Protocol` trait, which may include sending
/// messages to other replicas/nodes, which leads to point 5.
///
/// 5. When a message is received from other replicas, the same forward function
/// from point 3. is used to select the protocol process that is responsible for
/// handling that message. This suggests a message should define which `Dot` it
/// refers to. This is achieved through the `MessageDot` trait.
///
/// 6. Everytime a message is handled in a protocol process, the process checks
/// if it has new execution info. If so, it forwards each execution info to the
/// responsible executor. This suggests that execution info should define to
/// which key it refers to. This is achieved through the `MessageKey` trait.
///
/// 7. When execution info is handled in an executor, the executor may have new
/// (potentially partial if the executor is parallel) command results. If the
/// command was previously registered by some client, the result is forwarded to
/// such client.
///
/// 8. When command results are received by a client, they may have to be
/// aggregated in case the executor is parallel. Once the full command result is
/// complete, the notification is sent to the actual client.
///
/// Other notes:
/// - the runner allows `Protocol` workers to share state; however, it assumes
///   that `Executor` workers never do
// This module contains the "runner" prelude.
mod prelude;

// This module contains the definition of `ToPool`.
mod pool;

// This module contains the common read-write (+serde) utilities.
pub mod rw;

// This module contains the implementation of channels, clients, connections,
// executors, and process workers.
pub mod task;

const CONNECT_RETRIES: usize = 100;
type ConnectionDelay = Option<usize>;

// Re-exports.
pub use prelude::{
    worker_dot_index_shift, worker_index_no_shift, worker_index_shift,
    GC_WORKER_INDEX, INDEXES_RESERVED, LEADER_WORKER_INDEX,
};

use crate::client::{Client, ClientData, Workload};
use crate::command::{Command, CommandResult};
use crate::config::Config;
use crate::executor::Executor;
use crate::hash_map::{Entry, HashMap};
use crate::id::{AtomicDotGen, ClientId, ProcessId, Rifl, ShardId};
use crate::log;
use crate::protocol::Protocol;
use crate::time::{RunTime, SysTime};
use crate::HashSet;
use futures::stream::{FuturesUnordered, StreamExt};
use prelude::*;
use std::fmt::Debug;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::ToSocketAddrs;
use tokio::sync::Semaphore;

pub async fn process<P, A>(
    process_id: ProcessId,
    shard_id: ShardId,
    sorted_processes: Option<Vec<(ProcessId, ShardId)>>,
    ip: IpAddr,
    port: u16,
    client_port: u16,
    addresses: Vec<(A, ConnectionDelay)>,
    config: Config,
    tcp_nodelay: bool,
    tcp_buffer_size: usize,
    tcp_flush_interval: Option<usize>,
    process_channel_buffer_size: usize,
    client_channel_buffer_size: usize,
    workers: usize,
    executors: usize,
    multiplexing: usize,
    execution_log: Option<String>,
    tracer_show_interval: Option<usize>,
    ping_interval: Option<usize>,
    metrics_file: Option<String>,
) -> RunResult<()>
where
    P: Protocol + Send + 'static, // TODO what does this 'static do?
    A: ToSocketAddrs + Debug + Clone,
{
    // create semaphore for callers that don't care about the connected
    // notification
    let semaphore = Arc::new(Semaphore::new(0));
    process_with_notify_and_inspect::<P, A, ()>(
        process_id,
        shard_id,
        sorted_processes,
        ip,
        port,
        client_port,
        addresses,
        config,
        tcp_nodelay,
        tcp_buffer_size,
        tcp_flush_interval,
        process_channel_buffer_size,
        client_channel_buffer_size,
        workers,
        executors,
        multiplexing,
        execution_log,
        tracer_show_interval,
        ping_interval,
        metrics_file,
        semaphore,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn process_with_notify_and_inspect<P, A, R>(
    process_id: ProcessId,
    shard_id: ShardId,
    sorted_processes: Option<Vec<(ProcessId, ShardId)>>,
    ip: IpAddr,
    port: u16,
    client_port: u16,
    addresses: Vec<(A, ConnectionDelay)>,
    config: Config,
    tcp_nodelay: bool,
    tcp_buffer_size: usize,
    tcp_flush_interval: Option<usize>,
    process_channel_buffer_size: usize,
    client_channel_buffer_size: usize,
    workers: usize,
    executors: usize,
    multiplexing: usize,
    execution_log: Option<String>,
    tracer_show_interval: Option<usize>,
    ping_interval: Option<usize>,
    metrics_file: Option<String>,
    connected: Arc<Semaphore>,
    inspect_chan: Option<InspectReceiver<P, R>>,
) -> RunResult<()>
where
    P: Protocol + Send + 'static, // TODO what does this 'static do?
    A: ToSocketAddrs + Debug + Clone,
    R: Clone + Debug + Send + 'static,
{
    // panic if protocol is not parallel and we have more than one worker
    if workers > 1 && !P::parallel() {
        panic!("running non-parallel protocol with {} workers", workers,)
    }

    // panic if executor is not parallel and we have more than one executor
    if executors > 1 && !P::Executor::parallel() {
        panic!("running non-parallel executor with {} executors", executors)
    }

    // panic if protocol is leaderless and there's a leader
    if P::leaderless() && config.leader().is_some() {
        panic!("running leaderless protocol with a leader");
    }

    // panic if leader-based and there's no leader
    if !P::leaderless() && config.leader().is_none() {
        panic!("running leader-based protocol without a leader");
    }

    // (maybe) start tracer
    task::spawn(task::tracer::tracer_task(tracer_show_interval));

    // check ports are different
    assert!(port != client_port);

    // ---------------------
    // start process listener
    let listener = task::listen((ip, port)).await?;

    // create forward channels: reader -> workers
    let (reader_to_workers, reader_to_workers_rxs) = ReaderToWorkers::<P>::new(
        "reader_to_workers",
        process_channel_buffer_size,
        workers,
    );

    // connect to all processes
    let (ips, to_writers) = task::process::connect_to_all::<A, P>(
        process_id,
        shard_id,
        config,
        listener,
        addresses,
        reader_to_workers.clone(),
        CONNECT_RETRIES,
        tcp_nodelay,
        tcp_buffer_size,
        tcp_flush_interval,
        process_channel_buffer_size,
        multiplexing,
    )
    .await?;

    // get sorted processes (maybe from ping task)
    let sorted_processes = if let Some(sorted_processes) = sorted_processes {
        // in this case, we already have the sorted processes, so simply span
        // the ping task without a parent and return what we have
        task::spawn(task::ping::ping_task(
            ping_interval,
            process_id,
            shard_id,
            ips,
            None,
        ));
        sorted_processes
    } else {
        // when we don't have the sorted processes, spawn the ping task and ask
        // it for the sorted processes
        let to_ping = task::spawn_consumer(process_channel_buffer_size, |rx| {
            let parent = Some(rx);
            task::ping::ping_task(
                ping_interval,
                process_id,
                shard_id,
                ips,
                parent,
            )
        });
        ask_ping_task(to_ping).await
    };

    // check that we have n processes (all in my shard), plus one connection to
    // each other shard
    assert_eq!(
        sorted_processes.len(),
        config.n() + config.shards() - 1,
        "sorted processes count should be n + shards - 1"
    );

    // ---------------------
    // start client listener
    let client_listener = task::listen((ip, client_port)).await?;

    // create atomic dot generator to be used by clients in case the protocol is
    // leaderless:
    // - leader-based protocols like paxos shouldn't use this and the fact that
    //   there's no `Dot` will make new client commands always be forwarded to
    //   the leader worker (in case there's more than one worker); see
    //   `LEADER_WORKER_INDEX` in FPaxos implementation
    let atomic_dot_gen = if P::leaderless() {
        let atomic_dot_gen = AtomicDotGen::new(process_id);
        Some(atomic_dot_gen)
    } else {
        None
    };

    // create forward channels: periodic task -> workers
    let (periodic_to_workers, periodic_to_workers_rxs) = PeriodicToWorkers::new(
        "periodic_to_workers",
        process_channel_buffer_size,
        workers,
    );

    // create forward channels: client -> workers
    let (client_to_workers, client_to_workers_rxs) = ClientToWorkers::new(
        "client_to_workers",
        client_channel_buffer_size,
        workers,
    );

    // create forward channels: client -> executors
    let (client_to_executors, client_to_executors_rxs) = ClientToExecutors::new(
        "client_to_executors",
        client_channel_buffer_size,
        executors,
    );

    // start client listener
    task::client::start_listener(
        process_id,
        shard_id,
        client_listener,
        atomic_dot_gen,
        client_to_workers,
        client_to_executors,
        tcp_nodelay,
        client_channel_buffer_size,
    );

    // maybe create metrics logger
    let (worker_to_metrics_logger, executor_to_metrics_logger) =
        if let Some(metrics_file) = metrics_file {
            let (worker_to_metrics_logger, from_workers) =
                task::channel(process_channel_buffer_size);
            let (executor_to_metrics_logger, from_executors) =
                task::channel(process_channel_buffer_size);
            task::spawn(task::metrics_logger::metrics_logger_task(
                metrics_file,
                from_workers,
                from_executors,
            ));
            (
                Some(worker_to_metrics_logger),
                Some(executor_to_metrics_logger),
            )
        } else {
            (None, None)
        };

    // create forward channels: worker -> executors
    let (worker_to_executors, worker_to_executors_rxs) =
        WorkerToExecutors::<P>::new(
            "worker_to_executors",
            process_channel_buffer_size,
            executors,
        );

    // start executors
    task::executor::start_executors::<P>(
        process_id,
        shard_id,
        config,
        executors,
        worker_to_executors_rxs,
        client_to_executors_rxs,
        executor_to_metrics_logger,
    );

    // start process workers
    let handles = task::process::start_processes::<P, R>(
        process_id,
        shard_id,
        config,
        sorted_processes,
        reader_to_workers_rxs,
        client_to_workers_rxs,
        periodic_to_workers,
        periodic_to_workers_rxs,
        inspect_chan,
        to_writers,
        reader_to_workers,
        worker_to_executors,
        process_channel_buffer_size,
        execution_log,
        worker_to_metrics_logger,
    );
    println!("process {} started", process_id);

    // notify parent that we're connected
    connected.add_permits(1);

    let mut handles = handles.into_iter().collect::<FuturesUnordered<_>>();
    while let Some(join_result) = handles.next().await {
        println!("process ended {:?}", join_result?);
    }
    Ok(())
}

async fn ask_ping_task(
    mut to_ping: SortedProcessesSender,
) -> Vec<(ProcessId, ShardId)> {
    let (tx, mut rx) = task::channel(1);
    if let Err(e) = to_ping.send(tx).await {
        panic!("error sending request to ping task: {:?}", e);
    }
    if let Some(sorted_processes) = rx.recv().await {
        sorted_processes
    } else {
        panic!("error receiving reply from ping task");
    }
}

const MAX_CLIENT_CONNECTIONS: usize = 128;

pub async fn client<A>(
    ids: Vec<ClientId>,
    addresses: Vec<A>,
    interval: Option<Duration>,
    workload: Workload,
    tcp_nodelay: bool,
    channel_buffer_size: usize,
    metrics_file: Option<String>,
) -> RunResult<()>
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
                    tcp_nodelay,
                    channel_buffer_size,
                ))
            } else {
                task::spawn(closed_loop_client::<A>(
                    client_ids,
                    addresses.clone(),
                    workload,
                    tcp_nodelay,
                    channel_buffer_size,
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
            println!("client {} ended", client.id());
            data.merge(client.data());
            println!("metrics from {} collected", client.id());
        }
    }

    if let Some(file) = metrics_file {
        println!("will write client data to {}", file);
        serialize_client_data(data, file)?;
    }

    println!("all clients ended");
    Ok(())
}

async fn closed_loop_client<A>(
    client_ids: Vec<ClientId>,
    addresses: Vec<A>,
    workload: Workload,
    tcp_nodelay: bool,
    channel_buffer_size: usize,
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
        tcp_nodelay,
        channel_buffer_size,
    )
    .await?;

    // create pending
    let mut pending = ShardsPending::new();

    // generate the first message of each client
    for (_client_id, client) in clients.iter_mut() {
        let workload_finished = None;
        next_cmd(
            client,
            &time,
            &mut process_to_writer,
            &mut pending,
            workload_finished,
        )
        .await;
    }

    // track which clients are finished (i.e. all their commands have completed)
    let mut finished = HashSet::with_capacity(clients.len());

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
            let workload_finished = None;
            next_cmd(
                client,
                &time,
                &mut process_to_writer,
                &mut pending,
                workload_finished,
            )
            .await;
        }
    }

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
    tcp_nodelay: bool,
    channel_buffer_size: usize,
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
        tcp_nodelay,
        channel_buffer_size,
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
                        next_cmd(client, &time, &mut process_to_writer, &mut pending, Some(&mut workload_finished)).await;
                    }
                }
            }
        }
    }

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
    tcp_nodelay: bool,
    channel_buffer_size: usize,
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
            CONNECT_RETRIES,
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
            task::client::client_say_hi(client_ids.clone(), &mut connection)
                .await?;

        // update set of processes to be discovered by the client
        assert!(to_discover.insert(shard_id, process_id).is_none(), "client should try to connect to the same shard more than once, only to the closest one");

        // update list of connected processes
        connections.push((process_id, connection));
    }

    // start client read-write task
    let (read, process_to_write) = task::client::start_client_rw_tasks(
        &client_ids,
        channel_buffer_size,
        connections,
    );

    // create clients
    let clients = client_ids
        .into_iter()
        .map(|client_id| {
            let mut client = Client::new(client_id, workload);
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
    mut workload_finished: Option<&mut HashSet<ClientId>>,
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
        if let Some(workload_finished) = workload_finished.as_mut() {
            assert!(workload_finished.insert(client.id()));
        }
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
        println!("[client] error while sending message to client read-write task: {:?}", e);
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
    if let Some((client_id, cmd_results)) = pending.add(cmd_result) {
        let client = clients
            .get_mut(&client_id)
            .expect("[client] command result should belong to a client");

        // handle command results and check if client is finished
        if client.handle(cmd_results, time) {
            // record that this client is finished
            println!("client {:?} exited loop", client_id);
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

// TODO make this async
fn serialize_client_data(data: ClientData, file: String) -> RunResult<()> {
    // if the file does not exist it will be created, otherwise truncated
    std::fs::File::create(file)
        .ok()
        // create a buf writer
        .map(std::io::BufWriter::new)
        // and try to serialize
        .map(|writer| {
            bincode::serialize_into(writer, &data)
                .expect("error serializing client data")
        })
        .unwrap_or_else(|| panic!("couldn't save client data"));

    Ok(())
}

struct ShardsPending {
    pending: HashMap<Rifl, (usize, Vec<CommandResult>)>,
}

impl ShardsPending {
    fn new() -> Self {
        Self {
            pending: HashMap::new(),
        }
    }

    fn register(&mut self, cmd: &Command) {
        let shard_count = cmd.shard_count();
        let results = Vec::with_capacity(shard_count);
        let res = self.pending.insert(cmd.rifl(), (shard_count, results));
        assert!(res.is_none());
    }

    // Add new `CommandResult` and return all the `CommandResult` if we have the
    // results from all shards.
    fn add(
        &mut self,
        result: CommandResult,
    ) -> Option<(ClientId, Vec<CommandResult>)> {
        let rifl = result.rifl();
        log!("c{}: rifl received {:?}", rifl.source(), rifl);
        match self.pending.entry(rifl) {
            Entry::Occupied(mut entry) => {
                let (shard_count, results) = entry.get_mut();
                // add new result
                results.push(result);

                // return results if we have one `CommandResult` per shard
                // - TODO: add an assert checking that indeed these
                //   `CommandResult` came from different shards, and are not
                //   sent by the same shard
                if results.len() == *shard_count {
                    let (_, results) = entry.remove();
                    Some((rifl.source(), results))
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

// TODO this is `pub` so that `fantoch_ps` can run these `run_test` for the
// protocols implemented
pub mod tests {
    use super::*;
    use crate::protocol::ProtocolMetricsKind;
    use crate::util;
    use rand::Rng;

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

    #[allow(dead_code)]
    fn inspect_stable_commands<P>(worker: &P) -> usize
    where
        P: Protocol,
    {
        worker
            .metrics()
            .get_aggregated(ProtocolMetricsKind::Stable)
            .cloned()
            .unwrap_or_default() as usize
    }

    #[tokio::test]
    async fn run_basic_test() {
        use crate::client::{KeyGen, ShardGen};

        // config
        let n = 3;
        let f = 1;
        let mut config = Config::new(n, f);

        // make sure stability is running
        config.set_gc_interval(Duration::from_millis(100));

        // there's a single shard
        config.set_shards(1);

        // create workload
        let shards_per_command = 1;
        let shard_gen = ShardGen::Random { shard_count: 1 };
        let keys_per_shard = 2;
        let key_gen = KeyGen::ConflictRate { conflict_rate: 50 };
        let commands_per_client = 100;
        let payload_size = 1;
        let workload = Workload::new(
            shards_per_command,
            shard_gen,
            keys_per_shard,
            key_gen,
            commands_per_client,
            payload_size,
        );

        let clients_per_process = 3;
        let workers = 2;
        let executors = 2;
        let tracer_show_interval = None;
        let extra_run_time = Some(Duration::from_secs(5));

        // run test and get total stable commands
        let total_stable_count =
            run_test_with_inspect_fun::<crate::protocol::Basic, usize>(
                config,
                workload,
                clients_per_process,
                workers,
                executors,
                tracer_show_interval,
                Some(inspect_stable_commands),
                extra_run_time,
            )
            .await
            .expect("run should complete successfully")
            .into_iter()
            .map(|(_, stable_counts)| stable_counts.into_iter().sum::<usize>())
            .sum::<usize>();

        // get that all commands stablized at all processes
        let total_commands = n * clients_per_process * commands_per_client;
        assert!(total_stable_count == total_commands * n);
    }

    pub async fn run_test_with_inspect_fun<P, R>(
        config: Config,
        workload: Workload,
        clients_per_process: usize,
        workers: usize,
        executors: usize,
        tracer_show_interval: Option<usize>,
        inspect_fun: Option<fn(&P) -> R>,
        extra_run_time: Option<Duration>,
    ) -> RunResult<HashMap<ProcessId, Vec<R>>>
    where
        P: Protocol + Send + 'static,
        R: Clone + Debug + Send + 'static,
    {
        // create local task set
        let local = tokio::task::LocalSet::new();

        // run test in local task set
        local
            .run_until(async {
                run::<P, R>(
                    config,
                    workload,
                    clients_per_process,
                    workers,
                    executors,
                    tracer_show_interval,
                    inspect_fun,
                    extra_run_time,
                )
                .await
            })
            .await
    }

    async fn run<P, R>(
        config: Config,
        workload: Workload,
        clients_per_process: usize,
        workers: usize,
        executors: usize,
        tracer_show_interval: Option<usize>,
        inspect_fun: Option<fn(&P) -> R>,
        extra_run_time: Option<Duration>,
    ) -> RunResult<HashMap<ProcessId, Vec<R>>>
    where
        P: Protocol + Send + 'static,
        R: Clone + Debug + Send + 'static,
    {
        // create semaphore so that processes can notify once they're connected
        let semaphore = Arc::new(Semaphore::new(0));

        let localhost = "127.0.0.1"
            .parse::<IpAddr>()
            .expect("127.0.0.1 should be a valid ip");
        let tcp_nodelay = true;
        let tcp_buffer_size = 1024;
        let tcp_flush_interval = Some(1); // millis
        let process_channel_buffer_size = 10000;
        let client_channel_buffer_size = 100;
        let multiplexing = 2;
        let ping_interval = Some(1000); // millis

        // create processes ports and client ports
        let n = config.n();
        let shard_count = config.shards();
        let ports: HashMap<_, _> = util::all_process_ids(shard_count, n)
            .map(|(id, _shard_id)| (id, get_available_port()))
            .collect();
        let client_ports: HashMap<_, _> = util::all_process_ids(shard_count, n)
            .map(|(id, _shard_id)| (id, get_available_port()))
            .collect();

        // create connect addresses
        let all_addresses: HashMap<_, _> = ports
            .clone()
            .into_iter()
            .map(|(process_id, port)| {
                let address = format!("localhost:{}", port);
                (process_id, address)
            })
            .collect();

        let mut inspect_channels = HashMap::new();

        // the list of all ids that we can shuffle in order to set
        // `sorted_processes`
        let mut ids: Vec<_> = util::all_process_ids(shard_count, n).collect();

        // function used to figure out which which processes belong to the same
        // "region index"; there are as many region indexes as `n`; we have n =
        // 5, and shard_count = 3, we have:
        // - region index 1: processes 1, 6, 11
        // - region index 2: processes 2, 7, 12
        // - and so on..
        let region_index = |process_id| {
            let mut index = process_id;
            let n = config.n() as u8;
            while index > n {
                index -= n;
            }
            index
        };
        let same_shard_id_but_self =
            |process_id: ProcessId,
             shard_id: ShardId,
             ids: &Vec<(ProcessId, ShardId)>| {
                ids.clone().into_iter().filter(
                    move |(peer_id, peer_shard_id)| {
                        // keep all that have the same shard id (that are not
                        // self)
                        *peer_id != process_id && *peer_shard_id == shard_id
                    },
                )
            };
        let same_region_index_but_self =
            |process_id: ProcessId, ids: &Vec<(ProcessId, ShardId)>| {
                // compute index
                let index = region_index(process_id);
                ids.clone().into_iter().filter(move |(peer_id, _)| {
                    // keep all that have the same index (that are not self)
                    *peer_id != process_id && region_index(*peer_id) == index
                })
            };

        // compute the set of processes we should connect to

        for (process_id, shard_id) in util::all_process_ids(shard_count, n) {
            // the following shuffle is here in case these `connect_to`
            // processes are used to compute `sorted_processes`
            use rand::seq::SliceRandom;
            ids.shuffle(&mut rand::thread_rng());

            // start `connect_to` will the processes within the same region
            // (i.e. one connection to each shard)
            let mut connect_to: Vec<_> =
                same_region_index_but_self(process_id, &ids).collect();

            // make self the first element
            let myself = (process_id, shard_id);
            connect_to.insert(0, myself);

            // add the missing processes from my shard (i.e. the processes from
            // my shard in the other regions)
            connect_to
                .extend(same_shard_id_but_self(process_id, shard_id, &ids));

            let sorted_processes = if shard_count > 1 {
                // don't set sorted processes in partial replication (no reason,
                // just for testing)
                None
            } else {
                // set sorted processes in full replication (no reason, just for
                // testing)
                Some(connect_to.clone())
            };

            // get ports
            let port = *ports.get(&process_id).unwrap();
            let client_port = *client_ports.get(&process_id).unwrap();

            // compute addresses
            let addresses = all_addresses
                .clone()
                .into_iter()
                .filter(|(peer_id, _)| {
                    // connect to all in `connect_to` but self
                    connect_to
                        .iter()
                        .any(|(to_connect_id, _)| to_connect_id == peer_id)
                        && *peer_id != process_id
                })
                .map(|(process_id, address)| {
                    let delay = if process_id % 2 == 1 {
                        // add 0 delay to odd processes
                        Some(0)
                    } else {
                        None
                    };
                    (address, delay)
                })
                .collect();

            // execution log
            let execution_log = Some(format!("p{}.execution_log", process_id));

            // create inspect channel and save sender side
            let (inspect_tx, inspect) = task::channel(1);
            inspect_channels.insert(process_id, inspect_tx);

            // spawn processes
            let metrics_file = format!(".metrics_process_{}", process_id);
            tokio::task::spawn_local(process_with_notify_and_inspect::<
                P,
                String,
                R,
            >(
                process_id,
                shard_id,
                sorted_processes,
                localhost,
                port,
                client_port,
                addresses,
                config,
                tcp_nodelay,
                tcp_buffer_size,
                tcp_flush_interval,
                process_channel_buffer_size,
                client_channel_buffer_size,
                workers,
                executors,
                multiplexing,
                execution_log,
                tracer_show_interval,
                ping_interval,
                Some(metrics_file),
                semaphore.clone(),
                Some(inspect),
            ));
        }

        // wait that all processes are connected
        println!("[main] waiting that processes are connected");
        for _ in util::all_process_ids(shard_count, n) {
            let _ = semaphore.acquire().await;
        }
        println!("[main] processes are connected");

        let clients_per_process = clients_per_process as u64;
        let client_handles: Vec<_> = util::all_process_ids(shard_count, n)
            .map(|(process_id, _)| {
                // if n = 3, this gives the following:
                // id = 1: [1, 2, 3, 4]
                // id = 2: [5, 6, 7, 8]
                // id = 3: [9, 10, 11, 12]
                let client_id_start =
                    ((process_id - 1) as u64 * clients_per_process) + 1;
                let client_id_end = process_id as u64 * clients_per_process;
                let client_ids = (client_id_start..=client_id_end).collect();

                // connect client to all processes in the same "region index"
                let addresses = same_region_index_but_self(process_id, &ids)
                    .map(|(peer_id, _)| peer_id)
                    // also connect to "self"
                    .chain(std::iter::once(process_id))
                    .map(|peer_id| {
                        let client_port = *client_ports.get(&peer_id).unwrap();
                        format!("localhost:{}", client_port)
                    })
                    .collect();

                // compute interval:
                // - if the process id is even, then issue a command every 2ms
                // - otherwise, it's a closed-loop client
                let interval = match process_id % 2 {
                    0 => Some(Duration::from_millis(2)),
                    1 => None,
                    _ => panic!("n mod 2 should be in [0,1]"),
                };

                // spawn client
                let metrics_file = format!(".metrics_client_{}", process_id);
                tokio::task::spawn_local(client(
                    client_ids,
                    addresses,
                    interval,
                    workload,
                    tcp_nodelay,
                    client_channel_buffer_size,
                    Some(metrics_file),
                ))
            })
            .collect();

        // wait for all clients
        for client_handle in client_handles {
            let _ = client_handle.await.expect("client should finish");
        }

        // wait for the extra run time (if any)
        if let Some(extra_run_time) = extra_run_time {
            tokio::time::delay_for(extra_run_time).await;
        }

        // inspect all processes (if there's an inspect function)
        let mut result = HashMap::new();

        if let Some(inspect_fun) = inspect_fun {
            // create reply channel
            let (reply_chan_tx, mut reply_chan) = task::channel(1);

            // contact all processes
            for (process_id, mut inspect_tx) in inspect_channels {
                inspect_tx
                    .blind_send((inspect_fun, reply_chan_tx.clone()))
                    .await;
                let replies =
                    gather_workers_replies(workers, &mut reply_chan).await;
                result.insert(process_id, replies);
            }
        }

        Ok(result)
    }

    async fn gather_workers_replies<R>(
        workers: usize,
        reply_chan: &mut task::chan::ChannelReceiver<R>,
    ) -> Vec<R> {
        let mut replies = Vec::with_capacity(workers);
        for _ in 0..workers {
            let reply = reply_chan
                .recv()
                .await
                .expect("reply from process 1 should work");
            replies.push(reply);
        }
        replies
    }

    // adapted from: https://github.com/rust-lang-nursery/rust-cookbook/issues/500
    fn get_available_port() -> u16 {
        loop {
            let port = rand::thread_rng().gen_range(1025, 65535);
            if port_is_available(port) {
                return port;
            }
        }
    }

    fn port_is_available(port: u16) -> bool {
        std::net::TcpListener::bind(("127.0.0.1", port)).is_ok()
    }
}
