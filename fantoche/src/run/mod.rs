/// The architecture of this runner was thought in a way that allows all
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

use crate::client::{Client, Workload};
use crate::command::CommandResult;
use crate::config::Config;
use crate::executor::Executor;
use crate::id::{AtomicDotGen, ClientId, ProcessId};
use crate::metrics::Histogram;
use crate::protocol::Protocol;
use crate::time::{RunTime, SysTime};
use futures::future::join_all;
use futures::future::FutureExt;
use futures::select_biased;
use prelude::*;
use std::fmt::Debug;
use std::net::IpAddr;
use std::sync::Arc;
use tokio::net::ToSocketAddrs;
use tokio::sync::Semaphore;
use tokio::time::{self, Duration};

pub async fn process<A, P>(
    process: P,
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
) -> RunResult<()>
where
    A: ToSocketAddrs + Debug + Clone,
    P: Protocol + Send + 'static, // TODO what does this 'static do?
{
    // create semaphore for callers that don't care about the connected
    // notification
    let semaphore = Arc::new(Semaphore::new(0));
    process_with_notify::<A, P>(
        process,
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
        semaphore,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn process_with_notify<A, P>(
    mut process: P,
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
    connected: Arc<Semaphore>,
) -> RunResult<()>
where
    A: ToSocketAddrs + Debug + Clone,
    P: Protocol + Send + 'static, // TODO what does this 'static do?
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

    // discover processes
    process.discover(sorted_processes);

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

    // create forward channels: client -> executors
    let (client_to_executors, client_to_executors_rxs) = ClientToExecutors::new(
        "client_to_executors",
        channel_buffer_size,
        config.executors(),
    );

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
        config,
        worker_to_executors_rxs,
        client_to_executors_rxs,
    );

    let handles = task::process::start_processes::<P>(
        process,
        process_id,
        reader_to_workers_rxs,
        client_to_workers_rxs,
        to_writers,
        reader_to_workers,
        worker_to_executors,
        channel_buffer_size,
        execution_log,
    );
    println!("process {} started", process_id);

    // notify parent that we're connected
    connected.add_permits(1);

    for join_result in join_all(handles).await {
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

    for join_result in join_all(handles).await {
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
        select_biased! {
            cmd_result = read.recv().fuse() => {
                if handle_cmd_result(&mut client, &time, cmd_result) {
                    // check if we have generated all commands and received all the corresponding command results, exit
                    break;
                }
            }
            _ = interval.tick().fuse()  => {
                // submit new command on every tick (if there are still commands to be generated)
                next_cmd(&mut client, &time, &mut write).await;
            }
        }
    }
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
    let (read, write) =
        task::client::start_client_rw_task(channel_buffer_size, connection);

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{
        Basic,
    };
    use rand::Rng;
    use tokio::task;
    use tokio::time::Duration;

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

    #[tokio::test]
    async fn run_basic_test() {
        // basic is a parallel protocol with parallel execution
        let workers = 2;
        let executors = 3;
        let with_leader = false;
        run_test::<Basic>(workers, executors, with_leader).await
    }

    // #[tokio::test]
    // async fn run_newt_sequential_test() {
    //     // newt sequential can only handle one worker but many executors
    //     let workers = 1;
    //     let executors = 2;
    //     let with_leader = false;
    //     run_test::<NewtSequential>(workers, executors, with_leader).await
    // }

    // #[tokio::test]
    // async fn run_newt_atomic_test() {
    //     // newt atomic can handle as many workers as we want but we may want to
    //     // only have one executor
    //     let workers = 3;
    //     let executors = 1;
    //     let with_leader = false;
    //     run_test::<NewtAtomic>(workers, executors, with_leader).await
    // }

    // #[tokio::test]
    // async fn run_atlas_sequential_test() {
    //     // atlas sequential can only handle one worker and one executor
    //     let workers = 1;
    //     let executors = 1;
    //     let with_leader = false;
    //     run_test::<AtlasSequential>(workers, executors, with_leader).await
    // }

    // #[tokio::test]
    // async fn run_atlas_locked_test() {
    //     // atlas locked can handle as many workers as we want but only one
    //     // executor
    //     let workers = 3;
    //     let executors = 1;
    //     let with_leader = false;
    //     run_test::<AtlasLocked>(workers, executors, with_leader).await
    // }

    // #[tokio::test]
    // async fn run_epaxos_sequential_test() {
    //     // epaxos sequential can only handle one worker and one executor
    //     let workers = 1;
    //     let executors = 1;
    //     let with_leader = false;
    //     run_test::<EPaxosSequential>(workers, executors, with_leader).await
    // }

    // #[tokio::test]
    // async fn run_epaxos_locked_test() {
    //     // epaxos locked can handle as many workers as we want but only one
    //     // executor
    //     let workers = 3;
    //     let executors = 1;
    //     let with_leader = false;
    //     run_test::<EPaxosLocked>(workers, executors, with_leader).await
    // }

    // #[tokio::test]
    // async fn run_fpaxos_sequential_test() {
    //     // run fpaxos in sequential mode
    //     let workers = 1;
    //     let executors = 1;
    //     let with_leader = true;
    //     run_test::<FPaxos>(workers, executors, with_leader).await
    // }

    // #[tokio::test]
    // async fn run_fpaxos_parallel_test() {
    //     // run fpaxos in paralel mode (in terms of workers, since execution is
    //     // never parallel)
    //     let workers = 3;
    //     let executors = 1;
    //     let with_leader = true;
    //     run_test::<FPaxos>(workers, executors, with_leader).await
    // }

    async fn run_test<P>(workers: usize, executors: usize, with_leader: bool)
    where
        P: Protocol + Send + 'static,
    {
        // create local task set
        let local = task::LocalSet::new();

        // run test in local task set
        local
            .run_until(async {
                match run::<P>(workers, executors, with_leader).await {
                    Ok(()) => {}
                    Err(e) => panic!("run failed: {:?}", e),
                }
            })
            .await;
    }

    async fn run<P>(
        workers: usize,
        executors: usize,
        with_leader: bool,
    ) -> RunResult<()>
    where
        P: Protocol + Send + 'static,
    {
        // create config
        let n = 3;
        let f = 1;
        let mut config = Config::new(n, f);

        // if we should set a leader, set process 1 as the leader
        if with_leader {
            config.set_leader(1);
        }

        // create processes
        let process_1 = P::new(1, config);
        let process_2 = P::new(2, config);
        let process_3 = P::new(3, config);

        // create semaphore so that processes can notify once they're connected
        let semaphore = Arc::new(Semaphore::new(0));

        let localhost = "127.0.0.1"
            .parse::<IpAddr>()
            .expect("127.0.0.1 should be a valid ip");
        let tcp_nodelay = true;
        let tcp_buffer_size = 1024;
        let tcp_flush_interval = Some(100); // micros
        let channel_buffer_size = 10000;
        let multiplexing = 2;

        // set parallel protocol and executors in config
        config.set_workers(workers);
        config.set_executors(executors);

        // get ports and client ports
        let p1_port = get_available_port();
        let p2_port = get_available_port();
        let p3_port = get_available_port();
        let p1_client_port = get_available_port();
        let p2_client_port = get_available_port();
        let p3_client_port = get_available_port();

        // execution logs
        let p1_execution_log = Some(String::from("p1.execution_log"));
        let p2_execution_log = Some(String::from("p2.execution_log"));
        let p3_execution_log = Some(String::from("p3.execution_log"));

        // spawn processes
        task::spawn_local(process_with_notify::<String, P>(
            process_1,
            1,
            vec![1, 2, 3],
            localhost,
            p1_port,
            p1_client_port,
            vec![
                format!("localhost:{}", p2_port),
                format!("localhost:{}", p3_port),
            ],
            config,
            tcp_nodelay,
            tcp_buffer_size,
            tcp_flush_interval,
            channel_buffer_size,
            multiplexing,
            p1_execution_log,
            semaphore.clone(),
        ));
        task::spawn_local(process_with_notify::<String, P>(
            process_2,
            2,
            vec![2, 3, 1],
            localhost,
            p2_port,
            p2_client_port,
            vec![
                format!("localhost:{}", p1_port),
                format!("localhost:{}", p3_port),
            ],
            config,
            tcp_nodelay,
            tcp_buffer_size,
            tcp_flush_interval,
            channel_buffer_size,
            multiplexing,
            p2_execution_log,
            semaphore.clone(),
        ));
        task::spawn_local(process_with_notify::<String, P>(
            process_3,
            3,
            vec![3, 1, 2],
            localhost,
            p3_port,
            p3_client_port,
            vec![
                format!("localhost:{}", p1_port),
                format!("localhost:{}", p2_port),
            ],
            config,
            tcp_nodelay,
            tcp_buffer_size,
            tcp_flush_interval,
            channel_buffer_size,
            multiplexing,
            p3_execution_log,
            semaphore.clone(),
        ));

        // wait that all processes are connected
        println!("[main] waiting that processes are connected");
        let _ = semaphore.acquire().await;
        let _ = semaphore.acquire().await;
        let _ = semaphore.acquire().await;
        println!("[main] processes are connected");

        // create workload
        let conflict_rate = 100;
        let total_commands = 100;
        let payload_size = 100;
        let workload =
            Workload::new(conflict_rate, total_commands, payload_size);

        // clients:
        // - the first spawns 1 closed-loop client (1)
        // - the second spawns 3 closed-loop clients (2, 22, 222)
        // - the third spawns 1 open-loop client (3)
        let client_1_handle = task::spawn_local(closed_loop_client(
            1,
            format!("localhost:{}", p1_client_port),
            workload,
            tcp_nodelay,
            channel_buffer_size,
        ));
        let client_2_handle = task::spawn_local(client(
            vec![2, 22, 222],
            format!("localhost:{}", p2_client_port),
            None,
            workload,
            tcp_nodelay,
            channel_buffer_size,
        ));
        let client_3_handle = task::spawn_local(open_loop_client(
            3,
            format!("localhost:{}", p3_client_port),
            100, // 100ms interval between ops
            workload,
            tcp_nodelay,
            channel_buffer_size,
        ));

        // wait for the 3 clients
        let _ = client_1_handle.await.expect("client 1 should finish");
        let _ = client_2_handle.await.expect("client 2 should finish");
        let _ = client_3_handle.await.expect("client 3 should finish");
        Ok(())
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
        match std::net::TcpListener::bind(("127.0.0.1", port)) {
            Ok(_) => true,
            Err(_) => false,
        }
    }
}
