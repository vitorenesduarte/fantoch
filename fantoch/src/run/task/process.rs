use super::execution_logger;
use crate::command::Command;
use crate::config::Config;
use crate::id::{Dot, ProcessId};
use crate::log;
use crate::protocol::{Action, Protocol};
use crate::run::prelude::*;
use crate::run::rw::Connection;
use crate::run::task;
use crate::time::RunTime;
use futures::stream::{FuturesUnordered, StreamExt};
use rand::Rng;
use std::collections::HashMap;
use std::fmt::Debug;
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::task::JoinHandle;
use tokio::time::{self, Duration};

pub async fn connect_to_all<A, P>(
    process_id: ProcessId,
    sorted_processes: Option<Vec<ProcessId>>,
    listener: TcpListener,
    addresses: Vec<A>,
    to_workers: ReaderToWorkers<P>,
    connect_retries: usize,
    tcp_nodelay: bool,
    tcp_buffer_size: usize,
    tcp_flush_interval: Option<usize>,
    channel_buffer_size: usize,
    multiplexing: usize,
) -> RunResult<(Vec<ProcessId>, HashMap<ProcessId, Vec<WriterSender<P>>>)>
where
    A: ToSocketAddrs + Debug,
    P: Protocol + 'static,
{
    // spawn listener
    let mut from_listener = task::spawn_producer(channel_buffer_size, |tx| {
        super::listener_task(listener, tcp_nodelay, tcp_buffer_size, tx)
    });

    // number of addresses
    let n = addresses.len();

    // create list of in and out connections:
    // - even though TCP is full-duplex, due to the current tokio
    //   non-parallel-tcp-socket-read-write limitation, we going to use in
    //   streams for reading and out streams for writing, which can be done in
    //   parallel
    let mut outgoing = Vec::with_capacity(n * multiplexing);
    let mut incoming = Vec::with_capacity(n * multiplexing);

    // connect to all addresses (outgoing)
    for address in addresses {
        // create `multiplexing` connections per address
        for _ in 0..multiplexing {
            let connection = super::connect(
                &address,
                tcp_nodelay,
                tcp_buffer_size,
                connect_retries,
            )
            .await?;
            // save connection if connected successfully
            outgoing.push(connection);
        }
    }

    // receive from listener all connected (incoming)
    for _ in 0..(n * multiplexing) {
        let connection = from_listener
            .recv()
            .await
            .expect("should receive connection from listener");
        incoming.push(connection);
    }

    let res = handshake::<P>(
        process_id,
        sorted_processes,
        n,
        to_workers,
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
    sorted_processes: Option<Vec<ProcessId>>,
    n: usize,
    to_workers: ReaderToWorkers<P>,
    tcp_flush_interval: Option<usize>,
    channel_buffer_size: usize,
    mut connections_0: Vec<Connection>,
    mut connections_1: Vec<Connection>,
) -> (Vec<ProcessId>, HashMap<ProcessId, Vec<WriterSender<P>>>)
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
        process_id,
        sorted_processes,
        n,
        tcp_flush_interval,
        channel_buffer_size,
        id_to_connection_1,
    )
    .await
}

async fn say_hi(process_id: ProcessId, connections: &mut Vec<Connection>) {
    let hi = ProcessHi(process_id);
    // send hi on each connection
    for connection in connections.iter_mut() {
        connection.send(&hi).await;
    }
}

async fn receive_hi(
    connections: Vec<Connection>,
) -> Vec<(ProcessId, Connection)> {
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

/// Starts a reader task per connection received. A `ReaderToWorkers` is passed
/// to each reader so that these can forward immediately to the correct worker
/// process.
fn start_readers<P>(
    to_workers: ReaderToWorkers<P>,
    connections: Vec<(ProcessId, Connection)>,
) where
    P: Protocol + 'static,
{
    for (process_id, connection) in connections {
        let to_workers_clone = to_workers.clone();
        task::spawn(reader_task::<P>(to_workers_clone, process_id, connection));
    }
}

async fn start_writers<P>(
    process_id: ProcessId,
    sorted_processes: Option<Vec<ProcessId>>,
    n: usize,
    tcp_flush_interval: Option<usize>,
    channel_buffer_size: usize,
    connections: Vec<(ProcessId, Connection)>,
) -> (Vec<ProcessId>, HashMap<ProcessId, Vec<WriterSender<P>>>)
where
    P: Protocol + 'static,
{
    // if `sorted_processes` is not set, ping all peers and build it
    let sorted_processes =
        maybe_ping(process_id, sorted_processes, &connections).await;

    // mapping from process id to channel broadcast writer should write to
    let mut writers = HashMap::with_capacity(n);

    // start on writer task per connection
    for (process_id, connection) in connections {
        // create channel where parent should write to
        let mut tx = task::spawn_consumer(channel_buffer_size, |rx| {
            writer_task::<P>(tcp_flush_interval, connection, rx)
        });
        // get list set of writers to this process
        let txs = writers.entry(process_id).or_insert_with(Vec::new);
        // name the channel accordingly
        tx.set_name(format!("to_writer_{}_process_{}", txs.len(), process_id));
        // and add a new writer channel
        txs.push(tx);
    }

    // check `sorted_processes` and `writers` size
    assert_eq!(sorted_processes.len(), n);
    assert_eq!(writers.len(), n);
    (sorted_processes, writers)
}

async fn maybe_ping(
    process_id: ProcessId,
    sorted_processes: Option<Vec<ProcessId>>,
    connections: &Vec<(ProcessId, Connection)>,
) -> Vec<ProcessId> {
    if let Some(sorted_processes) = sorted_processes {
        return sorted_processes;
    }
    let mut pings = HashMap::new();
    for (peer_id, connection) in connections {
        // check if we have already pinged this process or not
        if pings.contains_key(peer_id) {
            continue;
        }
        let ip = connection
            .ip_addr()
            .expect("ip address should be set for outgoing connection");
        let command = format!("ping -c 5 -q {} | tail -n 1 | cut -d/ -f5", ip);
        let out = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(command)
            .output()
            .await
            .expect("ping command should work");
        let stdout =
            String::from_utf8(out.stdout).expect("ping output should be utf8");
        let latency = stdout
            .parse::<f64>()
            .expect("ping output should be a float");
        let rounded_latency = latency as usize;
        println!(
            "p{}: ping to {} with ip {} took {}ms ({})",
            process_id, peer_id, ip, latency, rounded_latency
        );
        pings.insert(*peer_id, rounded_latency);
    }
    // sort processes by ping time
    let mut pings = pings
        .into_iter()
        .map(|(id, latency)| (latency, id))
        .collect::<Vec<_>>();
    pings.sort();
    pings
        .into_iter()
        .map(|(_latency, process_id)| process_id)
        .collect()
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
                if let Err(e) =
                    reader_to_workers.forward((process_id, msg)).await
                {
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
    // if flush interval higher than 0, then flush periodically; otherwise,
    // flush on every write
    if let Some(tcp_flush_interval) = tcp_flush_interval {
        // create interval
        let mut interval =
            time::interval(Duration::from_millis(tcp_flush_interval as u64));
        loop {
            tokio::select! {
                msg = parent.recv() => {
                    if let Some(msg) = msg {
                        // connection write *doesn't* flush
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
                // connection write *does* flush
                connection.send(msg).await;
            } else {
                println!("[writer] error receiving message from parent");
            }
        }
    }
}

/// Starts process workers.
pub fn start_processes<P, R>(
    process_id: ProcessId,
    config: Config,
    sorted_processes: Vec<ProcessId>,
    reader_to_workers_rxs: Vec<ReaderReceiver<P>>,
    client_to_workers_rxs: Vec<SubmitReceiver>,
    periodic_to_workers: PeriodicToWorkers<P, R>,
    periodic_to_workers_rxs: Vec<PeriodicEventReceiver<P, R>>,
    to_periodic_inspect: Option<InspectReceiver<P, R>>,
    to_writers: HashMap<ProcessId, Vec<WriterSender<P>>>,
    reader_to_workers: ReaderToWorkers<P>,
    worker_to_executors: WorkerToExecutors<P>,
    channel_buffer_size: usize,
    execution_log: Option<String>,
) -> Vec<JoinHandle<()>>
where
    P: Protocol + Send + 'static,
    R: Debug + Clone + Send + 'static,
{
    // create process
    let (mut process, process_events) = P::new(process_id, config);

    // discover processes
    process.discover(sorted_processes);

    // spawn periodic task
    task::spawn(task::periodic::periodic_task(
        process_events,
        periodic_to_workers,
        to_periodic_inspect,
    ));

    let to_execution_logger = execution_log.map(|execution_log| {
        // if the execution log was set, then start the execution logger
        let mut tx = task::spawn_consumer(channel_buffer_size, |rx| {
            execution_logger::execution_logger_task::<P>(execution_log, rx)
        });
        tx.set_name("to_execution_logger");
        tx
    });

    // zip rxs'
    let incoming = reader_to_workers_rxs
        .into_iter()
        .zip(client_to_workers_rxs.into_iter())
        .zip(periodic_to_workers_rxs.into_iter());

    // create executor workers
    incoming
        .enumerate()
        .map(
            |(worker_index, ((from_readers, from_clients), from_periodic))| {
                task::spawn(process_task::<P, R>(
                    worker_index,
                    process.clone(),
                    process_id,
                    from_readers,
                    from_clients,
                    from_periodic,
                    to_writers.clone(),
                    reader_to_workers.clone(),
                    worker_to_executors.clone(),
                    to_execution_logger.clone(),
                ))
            },
        )
        .collect()
}

async fn process_task<P, R>(
    worker_index: usize,
    mut process: P,
    process_id: ProcessId,
    mut from_readers: ReaderReceiver<P>,
    mut from_clients: SubmitReceiver,
    mut from_periodic: PeriodicEventReceiver<P, R>,
    mut to_writers: HashMap<ProcessId, Vec<WriterSender<P>>>,
    mut reader_to_workers: ReaderToWorkers<P>,
    mut worker_to_executors: WorkerToExecutors<P>,
    mut to_execution_logger: Option<ExecutionInfoSender<P>>,
) where
    P: Protocol + 'static,
    R: Debug + 'static,
{
    // create time
    let time = RunTime;
    loop {
        tokio::select! {
            msg = from_readers.recv() => {
                selected_from_processes(worker_index, process_id, msg, &mut process, &mut to_writers, &mut reader_to_workers, &mut worker_to_executors, &mut to_execution_logger, &time).await
            }
            event = from_periodic.recv() => {
                selected_from_periodic_task(worker_index, process_id, event, &mut process, &mut to_writers, &mut reader_to_workers, &time).await
            }
            cmd = from_clients.recv() => {
                selected_from_client(worker_index, process_id, cmd, &mut process, &mut to_writers, &mut reader_to_workers, &time).await
            }
        }
    }
}

async fn selected_from_processes<P>(
    worker_index: usize,
    process_id: ProcessId,
    msg: Option<(ProcessId, P::Message)>,
    process: &mut P,
    to_writers: &mut HashMap<ProcessId, Vec<WriterSender<P>>>,
    reader_to_workers: &mut ReaderToWorkers<P>,
    worker_to_executors: &mut WorkerToExecutors<P>,
    to_execution_logger: &mut Option<ExecutionInfoSender<P>>,
    time: &RunTime,
) where
    P: Protocol + 'static,
{
    log!("[server] reader message: {:?}", msg);
    if let Some((from, msg)) = msg {
        handle_from_processes(
            worker_index,
            process_id,
            from,
            msg,
            process,
            to_writers,
            reader_to_workers,
            worker_to_executors,
            to_execution_logger,
            time,
        )
        .await
    } else {
        println!(
            "[server] error while receiving new process message from readers"
        );
    }
}

async fn handle_from_processes<P>(
    worker_index: usize,
    process_id: ProcessId,
    from: ProcessId,
    msg: P::Message,
    process: &mut P,
    to_writers: &mut HashMap<ProcessId, Vec<WriterSender<P>>>,
    reader_to_workers: &mut ReaderToWorkers<P>,
    worker_to_executors: &mut WorkerToExecutors<P>,
    to_execution_logger: &mut Option<ExecutionInfoSender<P>>,
    time: &RunTime,
) where
    P: Protocol + 'static,
{
    // handle message in process and potentially new actions
    let actions = process.handle(from, msg, time);
    handle_actions(
        worker_index,
        process_id,
        actions,
        process,
        to_writers,
        reader_to_workers,
        time,
    )
    .await;

    // get new execution info for the executor
    let to_executor = process.to_executor();

    // if there's an execution logger, then also send execution info to it
    if let Some(to_execution_logger) = to_execution_logger {
        for execution_info in to_executor.clone() {
            if let Err(e) = to_execution_logger.send(execution_info).await {
                println!("[server] error while sending new execution info to execution logger: {:?}", e);
            }
        }
    }

    // notify executors
    for execution_info in to_executor {
        if let Err(e) = worker_to_executors.forward(execution_info).await {
            println!(
                "[server] error while sending new execution info to executor: {:?}",
                e
            );
        }
    }
}

// TODO maybe run in parallel
async fn handle_actions<P>(
    worker_index: usize,
    process_id: ProcessId,
    mut actions: Vec<Action<P>>,
    process: &mut P,
    to_writers: &mut HashMap<ProcessId, Vec<WriterSender<P>>>,
    reader_to_workers: &mut ReaderToWorkers<P>,
    time: &RunTime,
) where
    P: Protocol + 'static,
{
    while let Some(action) = actions.pop() {
        match action {
            Action::ToSend { target, msg } => {
                // send to writers in parallel
                let mut sends = to_writers
                    .iter_mut()
                    .filter_map(|(to, channels)| {
                        if target.contains(to) {
                            Some(send_to_one_writer::<P>(msg.clone(), channels))
                        } else {
                            None
                        }
                    })
                    .collect::<FuturesUnordered<_>>();
                while sends.next().await.is_some() {}

                // check if should handle message locally
                if target.contains(&process_id) {
                    // handle msg locally if self in `target`
                    let new_actions = handle_message_from_self::<P>(
                        worker_index,
                        process_id,
                        msg,
                        process,
                        reader_to_workers,
                        time,
                    )
                    .await;
                    actions.extend(new_actions);
                } else {
                    break;
                }
            }
            Action::ToForward { msg } => {
                // handle msg locally if self in `target`
                let new_actions = handle_message_from_self(
                    worker_index,
                    process_id,
                    msg,
                    process,
                    reader_to_workers,
                    time,
                )
                .await;
                actions.extend(new_actions);
            }
        }
    }
}

async fn handle_message_from_self<P>(
    worker_index: usize,
    process_id: ProcessId,
    msg: P::Message,
    process: &mut P,
    reader_to_workers: &mut ReaderToWorkers<P>,
    time: &RunTime,
) -> Vec<Action<P>>
where
    P: Protocol + 'static,
{
    // create msg to be forwarded
    let to_forward = (process_id, msg);
    // only handle message from self in this worker if the destination worker is
    // us; this means that "messages to self are delivered immediately" is only
    // true for self messages to the same worker
    if reader_to_workers.only_to_self(&to_forward, worker_index) {
        process.handle(process_id, to_forward.1, time)
    } else {
        if let Err(e) = reader_to_workers.forward(to_forward).await {
            println!("[server] error notifying process task with msg from self: {:?}", e);
        }
        vec![]
    }
}

async fn send_to_one_writer<P>(
    msg: P::Message,
    writers: &mut Vec<WriterSender<P>>,
) where
    P: Protocol + 'static,
{
    // pick a random one
    let writer_index = rand::thread_rng().gen_range(0, writers.len());

    if let Err(e) = writers[writer_index].send(msg).await {
        println!(
            "[server] error while sending to writer {}: {:?}",
            writer_index, e
        );
    }
}

async fn selected_from_client<P>(
    worker_index: usize,
    process_id: ProcessId,
    cmd: Option<(Option<Dot>, Command)>,
    process: &mut P,
    to_writers: &mut HashMap<ProcessId, Vec<WriterSender<P>>>,
    reader_to_workers: &mut ReaderToWorkers<P>,
    time: &RunTime,
) where
    P: Protocol + 'static,
{
    log!("[server] from clients: {:?}", cmd);
    if let Some((dot, cmd)) = cmd {
        handle_from_client(
            worker_index,
            process_id,
            dot,
            cmd,
            process,
            to_writers,
            reader_to_workers,
            time,
        )
        .await
    } else {
        println!("[server] error while receiving new command from executor");
    }
}

async fn handle_from_client<P>(
    worker_index: usize,
    process_id: ProcessId,
    dot: Option<Dot>,
    cmd: Command,
    process: &mut P,
    to_writers: &mut HashMap<ProcessId, Vec<WriterSender<P>>>,
    reader_to_workers: &mut ReaderToWorkers<P>,
    time: &RunTime,
) where
    P: Protocol + 'static,
{
    // submit command in process
    let actions = process.submit(dot, cmd, time);
    handle_actions(
        worker_index,
        process_id,
        actions,
        process,
        to_writers,
        reader_to_workers,
        time,
    )
    .await;
}

async fn selected_from_periodic_task<P, R>(
    worker_index: usize,
    process_id: ProcessId,
    event: Option<FromPeriodicMessage<P, R>>,
    process: &mut P,
    to_writers: &mut HashMap<ProcessId, Vec<WriterSender<P>>>,
    reader_to_workers: &mut ReaderToWorkers<P>,
    time: &RunTime,
) where
    P: Protocol + 'static,
    R: Debug + 'static,
{
    log!("[server] from periodic task: {:?}", event);
    if let Some(event) = event {
        handle_from_periodic_task(
            worker_index,
            process_id,
            event,
            process,
            to_writers,
            reader_to_workers,
            time,
        )
        .await
    } else {
        println!("[server] error while receiving new event from periodic task");
    }
}

async fn handle_from_periodic_task<P, R>(
    worker_index: usize,
    process_id: ProcessId,
    msg: FromPeriodicMessage<P, R>,
    process: &mut P,
    to_writers: &mut HashMap<ProcessId, Vec<WriterSender<P>>>,
    reader_to_workers: &mut ReaderToWorkers<P>,
    time: &RunTime,
) where
    P: Protocol + 'static,
    R: Debug + 'static,
{
    match msg {
        FromPeriodicMessage::Event(event) => {
            // handle event in process
            let actions = process.handle_event(event, time);
            handle_actions(
                worker_index,
                process_id,
                actions,
                process,
                to_writers,
                reader_to_workers,
                time,
            )
            .await;
        }
        FromPeriodicMessage::Inspect(f, mut tx) => {
            let outcome = f(&process);
            if let Err(e) = tx.send(outcome).await {
                println!(
                    "[server] error while sending inspect result: {:?}",
                    e
                );
            }
        }
    }
}
