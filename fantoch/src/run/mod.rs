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

// This module contains the definition of `ChannelSender` and `ChannelReceiver`.
pub mod chan;

// This module contains the implementaion on client-side and server-side logic.
pub mod task;

const CONNECT_RETRIES: usize = 100;

use crate::client::Workload;
use crate::config::Config;
use crate::executor::Executor;
use crate::hash_map::HashMap;
use crate::id::{AtomicDotGen, ClientId, ProcessId, ShardId};
use crate::info;
use crate::protocol::Protocol;
use color_eyre::Report;
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
    addresses: Vec<(A, Option<Duration>)>,
    config: Config,
    tcp_nodelay: bool,
    tcp_buffer_size: usize,
    tcp_flush_interval: Option<Duration>,
    process_channel_buffer_size: usize,
    client_channel_buffer_size: usize,
    workers: usize,
    executors: usize,
    multiplexing: usize,
    execution_log: Option<String>,
    tracer_show_interval: Option<Duration>,
    ping_interval: Option<Duration>,
    metrics_file: Option<String>,
) -> Result<(), Report>
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
    addresses: Vec<(A, Option<Duration>)>,
    config: Config,
    tcp_nodelay: bool,
    tcp_buffer_size: usize,
    tcp_flush_interval: Option<Duration>,
    process_channel_buffer_size: usize,
    client_channel_buffer_size: usize,
    workers: usize,
    executors: usize,
    multiplexing: usize,
    execution_log: Option<String>,
    tracer_show_interval: Option<Duration>,
    ping_interval: Option<Duration>,
    metrics_file: Option<String>,
    connected: Arc<Semaphore>,
    inspect_chan: Option<InspectReceiver<P, R>>,
) -> Result<(), Report>
where
    P: Protocol + Send + 'static, // TODO what does this 'static do?
    A: ToSocketAddrs + Debug + Clone,
    R: Clone + Debug + Send + 'static,
{
    // panic if protocol is not parallel and we have more than one worker
    if workers > 1 && !P::parallel() {
        panic!("running non-parallel protocol with {} workers", workers);
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
    task::spawn(task::server::tracer::tracer_task(tracer_show_interval));

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

    // create forward channels: worker /readers -> executors
    let (to_executors, to_executors_rxs) = ToExecutors::<P>::new(
        "to_executors",
        process_channel_buffer_size,
        executors,
    );

    // connect to all processes
    let (ips, to_writers) = task::server::connect_to_all::<A, P>(
        process_id,
        shard_id,
        config,
        listener,
        addresses,
        reader_to_workers.clone(),
        to_executors.clone(),
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
        task::spawn(task::server::ping::ping_task(
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
            task::server::ping::ping_task(
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
        config.n() + config.shard_count() - 1,
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

    // create forward channels: executors -> workers
    let (executors_to_workers, executors_to_workers_rxs) =
        ExecutorsToWorkers::new(
            "executors_to_workers",
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
    task::server::client::start_listener(
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
                chan::channel(process_channel_buffer_size);
            let (executor_to_metrics_logger, from_executors) =
                chan::channel(process_channel_buffer_size);
            task::spawn(task::server::metrics_logger::metrics_logger_task(
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

    // create process
    let (mut process, process_events) = P::new(process_id, shard_id, config);

    // discover processes
    let (connect_ok, closest_shard_process) =
        process.discover(sorted_processes);
    assert!(connect_ok, "process should have discovered successfully");

    // spawn periodic task
    task::spawn(task::server::periodic::periodic_task(
        process_events,
        periodic_to_workers,
        inspect_chan,
    ));

    // create mapping from shard id to writers
    let mut shard_writers = HashMap::with_capacity(closest_shard_process.len());
    for (shard_id, peer_id) in closest_shard_process {
        let writers = to_writers
            .get(&peer_id)
            .expect("closest shard process should be connected")
            .clone();
        shard_writers.insert(shard_id, writers);
    }

    // start executors
    task::server::executor::start_executors::<P>(
        process_id,
        shard_id,
        config,
        to_executors_rxs,
        client_to_executors_rxs,
        executors_to_workers,
        shard_writers,
        to_executors.clone(),
        executor_to_metrics_logger,
    );

    // start process workers
    let handles = task::server::process::start_processes::<P, R>(
        process,
        reader_to_workers_rxs,
        client_to_workers_rxs,
        periodic_to_workers_rxs,
        executors_to_workers_rxs,
        to_writers,
        reader_to_workers,
        to_executors,
        process_channel_buffer_size,
        execution_log,
        worker_to_metrics_logger,
    );
    info!("process {} started", process_id);

    // notify parent that we're connected
    connected.add_permits(1);

    let mut handles = handles.into_iter().collect::<FuturesUnordered<_>>();
    while let Some(join_result) = handles.next().await {
        let join_result = join_result?;
        info!("process ended {:?}", join_result);
    }
    Ok(())
}

pub async fn client<A>(
    ids: Vec<ClientId>,
    addresses: Vec<A>,
    interval: Option<Duration>,
    workload: Workload,
    batch_max_size: usize,
    batch_max_delay: Duration,
    tcp_nodelay: bool,
    channel_buffer_size: usize,
    status_frequency: Option<usize>,
    metrics_file: Option<String>,
) -> Result<(), Report>
where
    A: ToSocketAddrs + Clone + Debug + Send + 'static + Sync,
{
    task::client::client(
        ids,
        addresses,
        interval,
        workload,
        batch_max_size,
        batch_max_delay,
        CONNECT_RETRIES,
        tcp_nodelay,
        channel_buffer_size,
        status_frequency,
        metrics_file,
    )
    .await
}

async fn ask_ping_task(
    mut to_ping: SortedProcessesSender,
) -> Vec<(ProcessId, ShardId)> {
    let (tx, mut rx) = chan::channel(1);
    if let Err(e) = to_ping.send(tx).await {
        panic!("error sending request to ping task: {:?}", e);
    }
    if let Some(sorted_processes) = rx.recv().await {
        sorted_processes
    } else {
        panic!("error receiving reply from ping task");
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
            tokio::time::sleep(Duration::from_secs(5)).await;
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

    #[test]
    fn run_basic_test() {
        use crate::client::KeyGen;

        // config
        let n = 3;
        let f = 1;
        let mut config = Config::new(n, f);

        // make sure stability is running
        config.set_gc_interval(Duration::from_millis(100));

        // there's a single shard
        config.set_shard_count(1);

        // create workload
        let keys_per_command = 1;
        let shard_count = 1;
        let conflict_rate = 50;
        let pool_size = 1;
        let key_gen = KeyGen::ConflictPool {
            conflict_rate,
            pool_size,
        };
        let commands_per_client = 100;
        let payload_size = 1;
        let workload = Workload::new(
            shard_count,
            key_gen,
            keys_per_command,
            commands_per_client,
            payload_size,
        );

        let clients_per_process = 3;
        let workers = 2;
        let executors = 2;
        let tracer_show_interval = Some(Duration::from_secs(1));
        let tracing_directives = Some("fantoch=trace");
        let extra_run_time = Some(Duration::from_secs(5));

        // run test and get total stable commands
        let total_stable_count = tokio_test_runtime()
            .block_on(
                run_test_with_inspect_fun::<crate::protocol::Basic, usize>(
                    config,
                    workload,
                    clients_per_process,
                    workers,
                    executors,
                    tracer_show_interval,
                    tracing_directives,
                    Some(inspect_stable_commands),
                    extra_run_time,
                ),
            )
            .expect("run should complete successfully")
            .into_iter()
            .map(|(_, stable_counts)| stable_counts.into_iter().sum::<usize>())
            .sum::<usize>();

        // get that all commands stablized at all processes
        let total_commands = n * clients_per_process * commands_per_client;
        assert!(total_stable_count == total_commands * n);
    }

    pub fn tokio_test_runtime() -> tokio::runtime::Runtime {
        // create tokio runtime
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(num_cpus::get())
            .thread_stack_size(32 * 1024 * 1024) // 32MB
            .enable_io()
            .enable_time()
            .thread_name("runner")
            .build()
            .expect("tokio runtime build should work")
    }

    pub async fn run_test_with_inspect_fun<P, R>(
        config: Config,
        workload: Workload,
        clients_per_process: usize,
        workers: usize,
        executors: usize,
        tracer_show_interval: Option<Duration>,
        tracing_directives: Option<&'static str>,
        inspect_fun: Option<fn(&P) -> R>,
        extra_run_time: Option<Duration>,
    ) -> Result<HashMap<ProcessId, Vec<R>>, Report>
    where
        P: Protocol + Send + 'static,
        R: Clone + Debug + Send + 'static,
    {
        // init tracing if directives were set
        let _guard = if tracing_directives.is_some() {
            // create unique test timestamp
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("we're way past epoch")
                .as_nanos();
            let log_file = Some(format!("test_{}.log", timestamp));
            Some(crate::util::init_tracing_subscriber(
                log_file,
                tracing_directives,
            ))
        } else {
            None
        };

        // create semaphore so that processes can notify once they're connected
        let semaphore = Arc::new(Semaphore::new(0));

        let localhost = "127.0.0.1"
            .parse::<IpAddr>()
            .expect("127.0.0.1 should be a valid ip");
        let tcp_nodelay = true;
        let tcp_buffer_size = 1024;
        let tcp_flush_interval = Some(Duration::from_millis(1));
        let process_channel_buffer_size = 10000;
        let client_channel_buffer_size = 10000;
        let multiplexing = 2;
        let ping_interval = Some(Duration::from_secs(1));

        // create processes ports and client ports
        let n = config.n();
        let shard_count = config.shard_count();
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
                        Some(Duration::from_secs(0))
                    } else {
                        None
                    };
                    (address, delay)
                })
                .collect();

            // execution log
            let execution_log = Some(format!("p{}.execution_log", process_id));

            // create inspect channel and save sender side
            let (inspect_tx, inspect) = chan::channel(1);
            inspect_channels.insert(process_id, inspect_tx);

            // spawn processes
            let metrics_file = format!(".metrics_process_{}", process_id);
            tokio::task::spawn(
                process_with_notify_and_inspect::<P, String, R>(
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
                ),
            );
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

                // batching config
                let batch_max_size = 5;
                let batch_max_delay = Duration::from_millis(1);

                // spawn client
                let status_frequency = None;
                let metrics_file =
                    Some(format!(".metrics_client_{}", process_id));
                tokio::task::spawn(client(
                    client_ids,
                    addresses,
                    interval,
                    workload,
                    batch_max_size,
                    batch_max_delay,
                    tcp_nodelay,
                    client_channel_buffer_size,
                    status_frequency,
                    metrics_file,
                ))
            })
            .collect();

        // wait for all clients
        for client_handle in client_handles {
            let _ = client_handle.await.expect("client should finish");
        }

        // wait for the extra run time (if any)
        if let Some(extra_run_time) = extra_run_time {
            tokio::time::sleep(extra_run_time).await;
        }

        // inspect all processes (if there's an inspect function)
        let mut result = HashMap::new();

        if let Some(inspect_fun) = inspect_fun {
            // create reply channel
            let (reply_chan_tx, mut reply_chan) = chan::channel(1);

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
        reply_chan: &mut chan::ChannelReceiver<R>,
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
            let port = rand::thread_rng().gen_range(1025..65535);
            if port_is_available(port) {
                return port;
            }
        }
    }

    fn port_is_available(port: u16) -> bool {
        std::net::TcpListener::bind(("127.0.0.1", port)).is_ok()
    }
}
