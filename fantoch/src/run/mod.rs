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

const CONNECT_RETRIES: usize = 100;

// This module contains the "runner" prelude.
mod prelude;

// This module contains the definition of `ToPool`.
mod pool;

// This module contains the common read-write (+serde) utilities.
pub mod rw;

// This module contains the implementation of channels, clients, connections,
// executors, and process workers.
pub mod task;

// Re-exports.
pub use prelude::{
    dot_worker_index_reserve, no_worker_index_reserve, worker_index_reserve,
    GC_WORKER_INDEX, LEADER_WORKER_INDEX,
};

use crate::client::{Client, Workload};
use crate::command::CommandResult;
use crate::config::Config;
use crate::executor::Executor;
use crate::id::{AtomicDotGen, ClientId, ProcessId};
use crate::metrics::Histogram;
use crate::protocol::Protocol;
use crate::time::{RunTime, SysTime};
use futures::stream::{FuturesUnordered, StreamExt};
use prelude::*;
use std::fmt::Debug;
use std::net::IpAddr;
use std::sync::Arc;
use tokio::net::ToSocketAddrs;
use tokio::sync::Semaphore;
use tokio::time::{self, Duration};

pub async fn process<P, A>(
    process_id: ProcessId,
    sorted_processes: Vec<ProcessId>,
    ip: IpAddr,
    port: u16,
    client_port: u16,
    addresses: Vec<A>,
    config: Config,
    tcp_nodelay: bool,
    tcp_buffer_size: usize,
    tcp_flush_interval: Option<usize>,
    channel_buffer_size: usize,
    multiplexing: usize,
    execution_log: Option<String>,
    tracer_show_interval: Option<usize>,
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
        sorted_processes,
        ip,
        port,
        client_port,
        addresses,
        config,
        tcp_nodelay,
        tcp_buffer_size,
        tcp_flush_interval,
        channel_buffer_size,
        multiplexing,
        execution_log,
        tracer_show_interval,
        semaphore,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn process_with_notify_and_inspect<P, A, R>(
    process_id: ProcessId,
    sorted_processes: Vec<ProcessId>,
    ip: IpAddr,
    port: u16,
    client_port: u16,
    addresses: Vec<A>,
    config: Config,
    tcp_nodelay: bool,
    tcp_buffer_size: usize,
    tcp_flush_interval: Option<usize>,
    channel_buffer_size: usize,
    multiplexing: usize,
    execution_log: Option<String>,
    tracer_show_interval: Option<usize>,
    connected: Arc<Semaphore>,
    inspect_chan: Option<InspectReceiver<P, R>>,
) -> RunResult<()>
where
    P: Protocol + Send + 'static, // TODO what does this 'static do?
    A: ToSocketAddrs + Debug + Clone,
    R: Clone + Debug + Send + 'static,
{
    // panic if protocol is not parallel and we have more than one worker
    if config.workers() > 1 && !P::parallel() {
        panic!(
            "running non-parallel protocol with {} workers",
            config.workers()
        )
    }

    // panic if executor is not parallel and we have more than one executor
    if config.executors() > 1 && !P::Executor::parallel() {
        panic!(
            "running non-parallel executor with {} executors",
            config.executors()
        )
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

    // start process listener
    let listener = task::listen((ip, port)).await?;

    // create forward channels: reader -> workers
    let (reader_to_workers, reader_to_workers_rxs) = ReaderToWorkers::<P>::new(
        "reader_to_workers",
        channel_buffer_size,
        config.workers(),
    );

    // connect to all processes
    let to_writers = task::process::connect_to_all::<A, P>(
        process_id,
        listener,
        addresses,
        reader_to_workers.clone(),
        CONNECT_RETRIES,
        tcp_nodelay,
        tcp_buffer_size,
        tcp_flush_interval,
        channel_buffer_size,
        multiplexing,
    )
    .await?;

    // start client listener
    let listener = task::listen((ip, client_port)).await?;

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

    // create forward channels: client -> workers
    let (client_to_workers, client_to_workers_rxs) = ClientToWorkers::new(
        "client_to_workers",
        channel_buffer_size,
        config.workers(),
    );

    // create forward channels: periodic task -> workers
    let (periodic_to_workers, periodic_to_workers_rxs) = PeriodicToWorkers::new(
        "periodic_to_workers",
        channel_buffer_size,
        config.workers(),
    );

    // create forward channels: client -> executors
    let (client_to_executors, client_to_executors_rxs) = ClientToExecutors::new(
        "client_to_executors",
        channel_buffer_size,
        config.executors(),
    );

    // start listener
    task::client::start_listener(
        process_id,
        listener,
        atomic_dot_gen,
        client_to_workers,
        client_to_executors,
        tcp_nodelay,
        channel_buffer_size,
    );

    // create forward channels: worker -> executors
    let (worker_to_executors, worker_to_executors_rxs) =
        WorkerToExecutors::<P>::new(
            "worker_to_executors",
            channel_buffer_size,
            config.executors(),
        );

    // start executors
    task::executor::start_executors::<P>(
        process_id,
        config,
        worker_to_executors_rxs,
        client_to_executors_rxs,
    );

    // start process workers
    let handles = task::process::start_processes::<P, R>(
        process_id,
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
        channel_buffer_size,
        execution_log,
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

pub async fn client<A>(
    ids: Vec<ClientId>,
    address: A,
    interval_ms: Option<u64>,
    workload: Workload,
    tcp_nodelay: bool,
    channel_buffer_size: usize,
) -> RunResult<()>
where
    A: ToSocketAddrs + Clone + Debug + Send + 'static + Sync,
{
    // start one client per id
    let handles = ids.into_iter().map(|client_id| {
        // start the open loop client if some interval was provided
        if let Some(interval_ms) = interval_ms {
            task::spawn(open_loop_client::<A>(
                client_id,
                address.clone(),
                interval_ms,
                workload,
                tcp_nodelay,
                channel_buffer_size,
            ))
        } else {
            task::spawn(closed_loop_client::<A>(
                client_id,
                address.clone(),
                workload,
                tcp_nodelay,
                channel_buffer_size,
            ))
        }
    });

    // wait for all clients to complete and aggregate their metrics
    let mut latency = Histogram::new();
    // let mut throughput = Histogram::new();

    let mut handles = handles.into_iter().collect::<FuturesUnordered<_>>();
    while let Some(join_result) = handles.next().await {
        let client = join_result?;
        println!("client {} ended", client.id());
        latency.merge(client.latency_histogram());
        // throughput.merge(client.throughput_histogram());
        println!("metrics from {} collected", client.id());
    }

    // show global metrics
    // TODO write both metrics (latency and throughput) to a file; the filename
    // should be provided as input (as an Option)
    println!("latency: {:?}", latency);
    // println!("throughput: {}", throughput.all_values());
    println!("all clients ended");
    Ok(())
}

async fn closed_loop_client<A>(
    client_id: ClientId,
    address: A,
    workload: Workload,
    tcp_nodelay: bool,
    channel_buffer_size: usize,
) -> Client
where
    A: ToSocketAddrs + Clone + Debug + Send + 'static + Sync,
{
    // create system time
    let time = RunTime;

    // setup client
    let (mut client, mut read, mut write) = client_setup(
        client_id,
        address,
        workload,
        tcp_nodelay,
        channel_buffer_size,
    )
    .await;

    // generate and submit commands while there are commands to be generated
    while next_cmd(&mut client, &time, &mut write).await {
        // and wait for their return
        let cmd_result = read.recv().await;
        handle_cmd_result(&mut client, &time, cmd_result);
    }
    println!("closed loop client {} exited loop", client_id);

    // return client
    client
}

async fn open_loop_client<A>(
    client_id: ClientId,
    address: A,
    interval_ms: u64,
    workload: Workload,
    tcp_nodelay: bool,
    channel_buffer_size: usize,
) -> Client
where
    A: ToSocketAddrs + Clone + Debug + Send + 'static + Sync,
{
    // create system time
    let time = RunTime;

    // setup client
    let (mut client, mut read, mut write) = client_setup(
        client_id,
        address,
        workload,
        tcp_nodelay,
        channel_buffer_size,
    )
    .await;

    // create interval
    let mut interval = time::interval(Duration::from_millis(interval_ms));

    loop {
        tokio::select! {
            cmd_result = read.recv() => {
                if handle_cmd_result(&mut client, &time, cmd_result) {
                    // check if we have generated all commands and received all the corresponding command results, exit
                    break;
                }
            }
            _ = interval.tick() => {
                // submit new command on every tick (if there are still commands to be generated)
                next_cmd(&mut client, &time, &mut write).await;
            }
        }
    }

    println!("open loop client {} exited loop", client_id);

    // return client
    client
}

async fn client_setup<A>(
    client_id: ClientId,
    address: A,
    workload: Workload,
    tcp_nodelay: bool,
    channel_buffer_size: usize,
) -> (Client, CommandResultReceiver, CommandSender)
where
    A: ToSocketAddrs + Clone + Debug + Send + 'static + Sync,
{
    // connect to process
    let tcp_buffer_size = 0;
    let mut connection = match task::connect(
        address,
        tcp_nodelay,
        tcp_buffer_size,
        CONNECT_RETRIES,
    )
    .await
    {
        Ok(connection) => connection,
        Err(e) => {
            // TODO panicking here as not sure how to make error handling send +
            // 'static (required by tokio::spawn) and still be able
            // to use the ? operator
            panic!(
                "[client] error connecting at client {}: {:?}",
                client_id, e
            );
        }
    };

    // create client
    let mut client = Client::new(client_id, workload);

    // say hi
    let process_id =
        task::client::client_say_hi(client_id, &mut connection).await;

    // discover process (although this won't be used)
    client.discover(vec![process_id]);

    // start client read-write task
    let (read, mut write) =
        task::client::start_client_rw_task(channel_buffer_size, connection);
    write.set_name(format!("command_result_sender_client_{}", client_id));

    // return client its connection
    (client, read, write)
}

/// Generate the next command, returning a boolean representing whether a new
/// command was generated or not.
async fn next_cmd(
    client: &mut Client,
    time: &dyn SysTime,
    write: &mut CommandSender,
) -> bool {
    if let Some((_, cmd)) = client.next_cmd(time) {
        if let Err(e) = write.send(cmd).await {
            println!(
                "[client] error while sending command to client read-write task: {:?}",
                e
            );
        }
        true
    } else {
        false
    }
}

/// Handles a command result. The returned boolean indicates whether this client
/// is finished or not.
fn handle_cmd_result(
    client: &mut Client,
    time: &dyn SysTime,
    cmd_result: Option<CommandResult>,
) -> bool {
    if let Some(cmd_result) = cmd_result {
        client.handle(cmd_result, time)
    } else {
        panic!("[client] error while receiving command result from client read-write task");
    }
}

// TODO this is `pub` so that `fantoch_ps` can run these `run_test` for the
// protocols implemented
pub mod tests {
    use super::*;
    use crate::protocol::ProtocolMetricsKind;
    use crate::util;
    use rand::Rng;
    use std::collections::HashMap;

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
        // config
        let n = 3;
        let f = 1;
        let with_leader = false;
        let workers = 2;
        let executors = 3;
        let conflict_rate = 100;
        let commands_per_client = 100;
        let clients_per_region = 3;
        let tracer_show_interval = Some(3000);
        let extra_run_time = Some(5000);

        // run test and get total stable commands
        let total_stable_count =
            run_test_with_inspect_fun::<crate::protocol::Basic, usize>(
                n,
                f,
                with_leader,
                workers,
                executors,
                conflict_rate,
                commands_per_client,
                clients_per_region,
                extra_run_time,
                tracer_show_interval,
                Some(inspect_stable_commands),
            )
            .await
            .expect("run should complete successfully")
            .into_iter()
            .map(|(_, stable_counts)| stable_counts.into_iter().sum::<usize>())
            .sum::<usize>();

        // get that all commands stablized at all processes
        let total_commands = n * clients_per_region * commands_per_client;
        assert!(total_stable_count == total_commands * n);
    }

    pub async fn run_test_with_inspect_fun<P, R>(
        n: usize,
        f: usize,
        with_leader: bool,
        workers: usize,
        executors: usize,
        conflict_rate: usize,
        commands_per_client: usize,
        clients_per_region: usize,
        extra_run_time: Option<u64>,
        tracer_show_interval: Option<usize>,
        inspect_fun: Option<fn(&P) -> R>,
    ) -> RunResult<HashMap<ProcessId, Vec<R>>>
    where
        P: Protocol + Send + 'static,
        R: Clone + Debug + Send + 'static,
    {
        // TODO remove task local set?
        // create local task set
        let local = tokio::task::LocalSet::new();

        // run test in local task set
        local
            .run_until(async {
                run::<P, R>(
                    n,
                    f,
                    with_leader,
                    workers,
                    executors,
                    conflict_rate,
                    commands_per_client,
                    clients_per_region,
                    extra_run_time,
                    tracer_show_interval,
                    inspect_fun,
                )
                .await
            })
            .await
    }

    async fn run<P, R>(
        n: usize,
        f: usize,
        with_leader: bool,
        workers: usize,
        executors: usize,
        conflict_rate: usize,
        commands_per_client: usize,
        clients_per_region: usize,
        extra_run_time: Option<u64>,
        tracer_show_interval: Option<usize>,
        inspect_fun: Option<fn(&P) -> R>,
    ) -> RunResult<HashMap<ProcessId, Vec<R>>>
    where
        P: Protocol + Send + 'static,
        R: Clone + Debug + Send + 'static,
    {
        // create config
        let mut config = Config::new(n, f);

        // if we should set a leader, set process 1 as the leader
        if with_leader {
            config.set_leader(1);
        }

        // create semaphore so that processes can notify once they're connected
        let semaphore = Arc::new(Semaphore::new(0));

        let localhost = "127.0.0.1"
            .parse::<IpAddr>()
            .expect("127.0.0.1 should be a valid ip");
        let tcp_nodelay = true;
        let tcp_buffer_size = 1024;
        let tcp_flush_interval = Some(100); // millis
        let channel_buffer_size = 10000;
        let multiplexing = 2;

        // set parallel protocol and executors in config
        config.set_workers(workers);
        config.set_executors(executors);

        // create processes ports and client ports
        let ports: HashMap<_, _> = util::process_ids(n)
            .map(|id| (id, get_available_port()))
            .collect();
        let client_ports: HashMap<_, _> = util::process_ids(n)
            .map(|id| (id, get_available_port()))
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

        for process_id in util::process_ids(n) {
            // if n = 3, this gives the following:
            // - id = 1:  [1, 2, 3]
            // - id = 2:  [2, 3, 1]
            // - id = 3:  [3, 1, 2]
            let sorted_processes =
                (process_id..=(n as u64)).chain(1..process_id).collect();

            // get ports
            let port = *ports.get(&process_id).unwrap();
            let client_port = *client_ports.get(&process_id).unwrap();

            // addresses: all but self
            let mut addresses = all_addresses.clone();
            addresses.remove(&process_id);
            let addresses =
                addresses.into_iter().map(|(_, address)| address).collect();

            // execution log
            let execution_log = Some(format!("p{}.execution_log", process_id));

            // create inspect channel and save sender side
            let (inspect_tx, inspect) = task::channel(channel_buffer_size);
            inspect_channels.insert(process_id, inspect_tx);

            // spawn processes
            tokio::task::spawn_local(process_with_notify_and_inspect::<
                P,
                String,
                R,
            >(
                process_id,
                sorted_processes,
                localhost,
                port,
                client_port,
                addresses,
                config,
                tcp_nodelay,
                tcp_buffer_size,
                tcp_flush_interval,
                channel_buffer_size,
                multiplexing,
                execution_log,
                tracer_show_interval,
                semaphore.clone(),
                Some(inspect),
            ));
        }

        // wait that all processes are connected
        println!("[main] waiting that processes are connected");
        for _ in util::process_ids(n) {
            let _ = semaphore.acquire().await;
        }
        println!("[main] processes are connected");

        // create workload
        let payload_size = 100;
        let workload =
            Workload::new(conflict_rate, commands_per_client, payload_size);

        let clients_per_region = clients_per_region as u64;
        let client_handles: Vec<_> = util::process_ids(n)
            .map(|process_id| {
                // if n = 3, this gives the following:
                // id = 1: [1, 2, 3, 4]
                // id = 2: [5, 6, 7, 8]
                // id = 3: [9, 10, 11, 12]
                let id_start = ((process_id - 1) * clients_per_region) + 1;
                let id_end = process_id * clients_per_region;
                let ids = (id_start..=id_end).collect();

                // get port
                let client_port = *client_ports.get(&process_id).unwrap();
                let address = format!("localhost:{}", client_port);

                // compute interval:
                // - if the process id is even, then issue a command every 2ms
                // - otherwise, it's a closed-loop client
                let interval_ms = match process_id % 2 {
                    0 => Some(2),
                    1 => None,
                    _ => panic!("n mod 2 should be in [0,1]"),
                };

                // spawn client
                tokio::task::spawn_local(client(
                    ids,
                    address,
                    interval_ms,
                    workload,
                    tcp_nodelay,
                    channel_buffer_size,
                ))
            })
            .collect();

        // wait for all clients
        for client_handle in client_handles {
            let _ = client_handle.await.expect("client should finish");
        }

        // wait for the extra run time (if any)
        if let Some(extra_run_time) = extra_run_time {
            tokio::time::delay_for(Duration::from_millis(extra_run_time)).await;
        }

        // inspect all processes (if there's an inspect function)
        let mut result = HashMap::new();

        if let Some(inspect_fun) = inspect_fun {
            // create reply channel
            let (reply_chan_tx, mut reply_chan) =
                task::channel(channel_buffer_size);

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
