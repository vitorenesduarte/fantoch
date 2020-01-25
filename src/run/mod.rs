/// The architecture of this runner was thought in a way that allows all protocols that implement
/// the `Protocol` trait to achieve their maximum throughput. Below we detail all key decisions.
///
/// We assume:
/// - C clients
/// - E executors
/// - P protocol processes
///
/// 1. When a client connects for the first time it registers itself in all executors. This register
/// request contains the channel in which executors should write command results (potentially
/// partial command results if the command is multi-key).
///
/// 2. When a client issues a command, it registers this command in all executors that are
/// responsible for executing this command. This is how each executor knows if it should notify this
/// client when the command is executed. If the commmand is single-key, this command only needs to
/// be registered in one executor. If multi-key, it needs to be registered in several executors if
/// the keys accessed by the command are assigned to different executors.
///
/// 3. Once the command registration occurs (does the client need to wait for registration
/// completion or can it do it asynchronously?), the command is forwarded to *ONE* protocol process
/// (even if the command is multi-key). This single protocol process *needs to* be chosen by looking
/// the message identifier `Dot`. Using the keys being accessed by the command will not work for all
/// cases, for example, when recovering and the payload is not known, we only have acesss to a
/// `noOp` meaning that we would need to broadcast to all processes, which would be tricky to get
/// correctly. In particular, when the command is being submitted, its `Dot` has not been computed
/// yet. So the idea here is for parallel protocols to have the `DotGen` outside and once the `Dot`
/// is computed, the submit is forwarded to the correct protocol process. For maximum parallelism,
/// this generator can live in the clients and have a lock-free implementation (see `AtomicIdGen`).
///
/// 4. When the protocol process receives the new command from a client it does whatever is
/// specified in the `Protocol` trait, which may include sending messages to other replicas/nodes,
/// which leads to point 5.
///
/// 5. When a message is received from other replicas, the same forward function from point 3. is
/// used to select the protocol process that is responsible for handling that message. This suggests
/// a message should define which `Dot` it refers to. This is achieved through the `MessageDot`
/// trait.
///
/// 6. Everytime a message is handled in a protocol process, the process checks if it has new
/// execution info. If so, it forwards each execution info to the responsible executor. This
/// suggests that execution info should define to which key it refers to. This is achieved through
/// the `ExecutionInfoKey` trait.
///
/// 7. When execution info is handled in a executor, the executor may have new (potentially partial
/// if the executor is parallel) command results. If the command was previously registered by some
/// client, the result is forwarded to such client.
///
/// 8. When command results are received by a client, they may have to be aggregated in case the
/// executor is parallel. Once the full command result is complete, the notification is sent to the
/// actual client.

const CONNECT_RETRIES: usize = 100;

// This module contains the prelude.
mod prelude;

// This module contains forwarding logic.
pub mod forward;

// TODO This module contains the definition of...
pub mod task;

use crate::client::{Client, Workload};
use crate::command::{Command, CommandResult};
use crate::config::Config;
use crate::id::{ClientId, ProcessId};
use crate::log;
use crate::metrics::Histogram;
use crate::protocol::{Protocol, ToSend};
use crate::run::forward::ToWorkers;
use crate::time::{RunTime, SysTime};
use futures::future::{join_all, FutureExt};
use futures::select;
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
    socket_buffer_size: usize,
    channel_buffer_size: usize,
    workers: usize,
    executors: usize,
) -> RunResult<()>
where
    A: ToSocketAddrs + Debug + Clone,
    P: Protocol + 'static, // TODO what does this 'static do?
{
    // create semaphore for callers that don't care about the connected notification
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
        socket_buffer_size,
        channel_buffer_size,
        workers,
        executors,
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
    socket_buffer_size: usize,
    channel_buffer_size: usize,
    workers: usize,
    executors: usize,
    connected: Arc<Semaphore>,
) -> RunResult<()>
where
    A: ToSocketAddrs + Debug + Clone,
    P: Protocol + 'static, // TODO what does this 'static do?
{
    // discover processes
    process.discover(sorted_processes);

    // check ports are different
    assert!(port != client_port);

    // start process listener
    let listener = task::listen((ip, port)).await?;

    // create forward channels
    let (to_workers_tx, to_workers_rx) = ToWorkers::new(channel_buffer_size, workers);

    // connect to all processes
    let to_writers = task::process::connect_to_all::<A, P>(
        process_id,
        listener,
        addresses,
        to_workers_tx,
        CONNECT_RETRIES,
        tcp_nodelay,
        socket_buffer_size,
        channel_buffer_size,
    )
    .await?;

    // start client listener
    let listener = task::listen((ip, client_port)).await?;
    let from_clients = task::client::start_listener(
        process_id,
        listener,
        tcp_nodelay,
        socket_buffer_size,
        channel_buffer_size,
    );

    // start executor
    let (mut from_executor, mut to_executor) =
        task::process::start_executor::<P>(config, channel_buffer_size, from_clients);

    // notify parent that we're connected
    connected.add_permits(1);

    println!("process {} started", process_id);
    Ok(())

    // loop {
    //     select! {
    //         msg = from_readers.recv().fuse() => {
    //             log!("[server] reader message: {:?}", msg);
    //             if let Some((from, msg)) = msg {
    //                 handle_from_processes(process_id, from, msg, &mut process, &mut to_writer,
    // &mut to_executor).await             } else {
    //                 println!("[server] error while receiving new process message from readers");
    //             }
    //         }
    //         cmd = from_executor.recv().fuse() => {
    //             log!("[server] from executor: {:?}", cmd);
    //             if let Some(cmd) = cmd {
    //                 handle_from_client(process_id, cmd, &mut process, &mut to_writer).await
    //             } else {
    //                 println!("[server] error while receiving new command from executor");
    //             }
    //         }
    //     }
    // }
}

// async fn handle_from_processes<P>(
//     process_id: ProcessId,
//     from: ProcessId,
//     msg: P::Message,
//     process: &mut P,
//     to_writer: &mut BroadcastWriterSender<P::Message>,
//     to_executor: &mut ExecutionInfoSender<P>,
// ) where
//     P: Protocol + 'static,
// {
//     // handle message in process
//     let to_send = process.handle(from, msg);
//     send_to_writer(process_id, to_send, process, to_writer).await;

//     // check if there's new execution info for the executor
//     let execution_info = process.to_executor();
//     if !execution_info.is_empty() {
//         if let Err(e) = to_executor.send(execution_info).await {
//             println!("[server] error while sending to executor: {:?}", e);
//         }
//     }
// }

// async fn handle_from_client<P>(
//     process_id: ProcessId,
//     cmd: Command,
//     process: &mut P,
//     to_writer: &mut BroadcastWriterSender<P::Message>,
// ) where
//     P: Protocol + 'static,
// {
//     // submit command in process
//     let to_send = process.submit(None, cmd);
//     send_to_writer(process_id, Some(to_send), process, to_writer).await;
// }

// async fn send_to_writer<P>(
//     process_id: ProcessId,
//     to_send: Option<ToSend<P::Message>>,
//     process: &mut P,
//     to_writer: &mut BroadcastWriterSender<P::Message>,
// ) where
//     P: Protocol + 'static,
// {
//     if let Some(to_send) = to_send {
//         // handle msg locally if self in `to_send.target` and make sure that, if there's
// something         // to be sent, it is to self, i.e. messages from self to self shouldn't
// generate messages         if to_send.target.contains(&process_id) {
//             // TODO can we avoid cloning here?
//             if let Some(ToSend { target, msg, .. }) =
//                 process.handle(process_id, to_send.msg.clone())
//             {
//                 assert!(target.len() == 1);
//                 assert!(target.contains(&process_id));
//                 // handling this message shouldn't generate a new message
//                 let nothing = process.handle(process_id, msg);
//                 assert!(nothing.is_none());
//             }
//         }
//         if let Err(e) = to_writer.send(to_send).await {
//             println!("[server] error while sending to broadcast writer: {:?}", e);
//         }
//     }
// }

pub async fn client<A>(
    ids: Vec<ClientId>,
    address: A,
    interval_ms: Option<u64>,
    workload: Workload,
    tcp_nodelay: bool,
    socket_buffer_size: usize,
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
                socket_buffer_size,
                channel_buffer_size,
            ))
        } else {
            task::spawn(closed_loop_client::<A>(
                client_id,
                address.clone(),
                workload,
                tcp_nodelay,
                socket_buffer_size,
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
    // TODO write both metrics (latency and throughput) to a file; the filename should be provided
    // as input (as an Option)
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
    socket_buffer_size: usize,
    channel_buffer_size: usize,
) -> Client
where
    A: ToSocketAddrs + Debug + Send + 'static + Sync,
{
    // create system time
    let time = RunTime;

    // setup client
    let (mut client, mut read, mut write) = client_setup(
        client_id,
        address,
        workload,
        tcp_nodelay,
        socket_buffer_size,
        channel_buffer_size,
    )
    .await;

    // generate and submit commands while there are commands to be generated
    while next_cmd(&mut client, &time, &mut write).await {
        // and wait for their return
        let cmd_result = read.recv().await;
        handle_cmd_result(&mut client, &time, cmd_result);
    }

    // return client
    client
}

async fn open_loop_client<A>(
    client_id: ClientId,
    address: A,
    interval_ms: u64,
    workload: Workload,
    tcp_nodelay: bool,
    socket_buffer_size: usize,
    channel_buffer_size: usize,
) -> Client
where
    A: ToSocketAddrs + Debug + Send + 'static + Sync,
{
    // create system time
    let time = RunTime;

    // setup client
    let (mut client, mut read, mut write) = client_setup(
        client_id,
        address,
        workload,
        tcp_nodelay,
        socket_buffer_size,
        channel_buffer_size,
    )
    .await;

    // create interval
    let mut interval = time::interval(Duration::from_millis(interval_ms));

    loop {
        select! {
            _ = interval.tick().fuse() => {
                // submit new command on every tick (if there are still commands to be generated)
                next_cmd(&mut client, &time, &mut write).await;
            }
            cmd_result = read.recv().fuse() => {
                if handle_cmd_result(&mut client, &time, cmd_result) {
                    // check if we have generated all commands and received all the corresponding command results, exit
                    break;
                }
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
    socket_buffer_size: usize,
    channel_buffer_size: usize,
) -> (Client, CommandResultReceiver, CommandSender)
where
    A: ToSocketAddrs + Debug + Send + 'static + Sync,
{
    // connect to process
    let mut connection = match task::connect(address, tcp_nodelay, socket_buffer_size).await {
        Ok(connection) => connection,
        Err(e) => {
            // TODO panicking here as not sure how to make error handling send + 'static (required
            // by tokio::spawn) and still be able to use the ? operator
            panic!("[client] error connecting at client {}: {:?}", client_id, e);
        }
    };

    // create client
    let mut client = Client::new(client_id, workload);

    // say hi
    let process_id = task::client::client_say_hi(client_id, &mut connection).await;

    // discover process (although this won't be used)
    client.discover(vec![process_id]);

    // start client read-write task
    let (read, write) = task::client::start_client_rw_task(channel_buffer_size, connection);

    // return client its connection
    (client, read, write)
}

/// Generate the next command, returning a boolean representing whether a new command was generated
/// or not.
async fn next_cmd(client: &mut Client, time: &dyn SysTime, write: &mut CommandSender) -> bool {
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

/// Handles a command result. The returned boolean indicates whether this client is finished or not.
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
    use crate::protocol::Newt;
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
    async fn test_run() {
        // test with newt
        // create local task set
        let local = task::LocalSet::new();

        // run test in local task set
        local
            .run_until(async {
                match run::<Newt>().await {
                    Ok(()) => {}
                    Err(e) => panic!("run failed: {:?}", e),
                }
            })
            .await;
    }

    async fn run<P>() -> RunResult<()>
    where
        P: Protocol + 'static,
    {
        // create config
        let n = 3;
        let f = 1;
        let config = Config::new(n, f);

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
        let socket_buffer_size = 1000;
        let channel_buffer_size = 10000;
        let workers = 5;
        let executors = 5;

        // spawn processes
        task::spawn_local(process_with_notify::<String, P>(
            process_1,
            1,
            vec![1, 2, 3],
            localhost,
            3001,
            4001,
            vec![
                String::from("localhost:3002"),
                String::from("localhost:3003"),
            ],
            config,
            tcp_nodelay,
            socket_buffer_size,
            channel_buffer_size,
            workers,
            executors,
            semaphore.clone(),
        ));
        task::spawn_local(process_with_notify::<String, P>(
            process_2,
            2,
            vec![2, 3, 1],
            localhost,
            3002,
            4002,
            vec![
                String::from("localhost:3001"),
                String::from("localhost:3003"),
            ],
            config,
            tcp_nodelay,
            socket_buffer_size,
            channel_buffer_size,
            workers,
            executors,
            semaphore.clone(),
        ));
        task::spawn_local(process_with_notify::<String, P>(
            process_3,
            3,
            vec![3, 1, 2],
            localhost,
            3003,
            4003,
            vec![
                String::from("localhost:3001"),
                String::from("localhost:3002"),
            ],
            config,
            tcp_nodelay,
            socket_buffer_size,
            channel_buffer_size,
            workers,
            executors,
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
        let workload = Workload::new(conflict_rate, total_commands);

        // clients:
        // - the first spawns 1 closed-loop client (1)
        // - the second spawns 3 closed-loop clients (2, 22, 222)
        // - the third spawns 1 open-loop client (3)
        let client_1_handle = task::spawn_local(closed_loop_client(
            1,
            String::from("localhost:4001"),
            workload,
            tcp_nodelay,
            socket_buffer_size,
            channel_buffer_size,
        ));
        let client_2_handle = task::spawn_local(client(
            vec![2, 22, 222],
            String::from("localhost:4002"),
            None,
            workload,
            tcp_nodelay,
            socket_buffer_size,
            channel_buffer_size,
        ));
        let client_3_handle = task::spawn_local(open_loop_client(
            3,
            String::from("localhost:4003"),
            100, // 100ms interval between ops
            workload,
            tcp_nodelay,
            socket_buffer_size,
            channel_buffer_size,
        ));

        // wait for the 3 clients
        let _ = client_1_handle.await.expect("client 1 should finish");
        let _ = client_2_handle.await.expect("client 2 should finish");
        let _ = client_3_handle.await.expect("client 3 should finish");
        Ok(())
    }
}
