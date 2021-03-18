use super::execution_logger;
use crate::command::Command;
use crate::id::{Dot, ProcessId, ShardId};
use crate::protocol::{Action, CommittedAndExecuted, Protocol};
use crate::run::prelude::*;
use crate::run::task;
use crate::time::RunTime;
use crate::HashMap;
use crate::{trace, warn};
use rand::Rng;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio::time;

/// Starts process workers.
pub fn start_processes<P, R>(
    process: P,
    reader_to_workers_rxs: Vec<ReaderReceiver<P>>,
    client_to_workers_rxs: Vec<SubmitReceiver>,
    periodic_to_workers_rxs: Vec<PeriodicEventReceiver<P, R>>,
    executors_to_workers_rxs: Vec<ExecutedReceiver>,
    to_writers: HashMap<ProcessId, Vec<WriterSender<P>>>,
    reader_to_workers: ReaderToWorkers<P>,
    to_executors: ToExecutors<P>,
    process_channel_buffer_size: usize,
    execution_log: Option<String>,
    to_metrics_logger: Option<ProtocolMetricsSender>,
) -> Vec<JoinHandle<()>>
where
    P: Protocol + Send + 'static,
    R: Debug + Clone + Send + 'static,
{
    let to_execution_logger = execution_log.map(|execution_log| {
        // if the execution log was set, then start the execution logger
        let mut tx = task::spawn_consumer(process_channel_buffer_size, |rx| {
            execution_logger::execution_logger_task::<P>(execution_log, rx)
        });
        tx.set_name("to_execution_logger");
        tx
    });

    // zip rxs'
    let incoming = reader_to_workers_rxs
        .into_iter()
        .zip(client_to_workers_rxs.into_iter())
        .zip(periodic_to_workers_rxs.into_iter())
        .zip(executors_to_workers_rxs.into_iter());

    // create executor workers
    incoming
        .enumerate()
        .map(
            |(
                worker_index,
                (((from_readers, from_clients), from_periodic), from_executors),
            )| {
                // create task
                let task = process_task::<P, R>(
                    worker_index,
                    process.clone(),
                    from_readers,
                    from_clients,
                    from_periodic,
                    from_executors,
                    to_writers.clone(),
                    reader_to_workers.clone(),
                    to_executors.clone(),
                    to_execution_logger.clone(),
                    to_metrics_logger.clone(),
                );
                task::spawn(task)
                // // if this is a reserved worker, run it on its own runtime
                // if worker_index < super::INDEXES_RESERVED {
                //     let thread_name =
                //         format!("worker_{}_runtime", worker_index);
                //     tokio::task::spawn_blocking(|| {
                //         // create tokio runtime
                //         let mut runtime = tokio::runtime::Builder::new()
                //             .threaded_scheduler()
                //             .core_threads(1)
                //             .thread_name(thread_name)
                //             .build()
                //             .expect("tokio runtime build should work");
                //         runtime.block_on(task)
                //     });
                //     None
                // } else {
                //     Some(task::spawn(task))
                // }
            },
        )
        .collect()
}

async fn process_task<P, R>(
    worker_index: usize,
    mut process: P,
    mut from_readers: ReaderReceiver<P>,
    mut from_clients: SubmitReceiver,
    mut from_periodic: PeriodicEventReceiver<P, R>,
    mut from_executors: ExecutedReceiver,
    mut to_writers: HashMap<ProcessId, Vec<WriterSender<P>>>,
    mut reader_to_workers: ReaderToWorkers<P>,
    mut to_executors: ToExecutors<P>,
    mut to_execution_logger: Option<ExecutionInfoSender<P>>,
    mut to_metrics_logger: Option<ProtocolMetricsSender>,
) where
    P: Protocol + 'static,
    R: Debug + 'static,
{
    // create time
    let time = RunTime;

    // create interval (for metrics notification)
    let mut interval = time::interval(super::metrics_logger::METRICS_INTERVAL);

    loop {
        // TODO maybe used select_biased
        tokio::select! {
            msg = from_readers.recv() => {
                selected_from_processes(worker_index, msg, &mut process, &mut to_writers, &mut reader_to_workers, &mut to_executors, &mut to_execution_logger, &time).await
            }
            event = from_periodic.recv() => {
                selected_from_periodic_task(worker_index, event, &mut process, &mut to_writers, &mut reader_to_workers, &mut to_executors, &mut to_execution_logger, &time).await
            }
            executed = from_executors.recv() => {
                selected_from_executors(worker_index, executed, &mut process, &mut to_writers, &mut reader_to_workers, &mut to_executors, &mut to_execution_logger, &time).await
            }
            cmd = from_clients.recv() => {
                selected_from_clients(worker_index, cmd, &mut process, &mut to_writers, &mut reader_to_workers, &mut to_executors, &mut to_execution_logger, &time).await
            }
            _ = interval.tick()  => {
                if let Some(to_metrics_logger) = to_metrics_logger.as_mut() {
                    // send metrics to logger (in case there's one)
                    let protocol_metrics = process.metrics().clone();
                    if let Err(e) = to_metrics_logger.send((worker_index, protocol_metrics)).await {
                        warn!("[server] error while sending metrics to metrics logger: {:?}", e);
                    }
                }
            }
        }
    }
}

async fn selected_from_processes<P>(
    worker_index: usize,
    msg: Option<(ProcessId, ShardId, P::Message)>,
    process: &mut P,
    to_writers: &mut HashMap<ProcessId, Vec<WriterSender<P>>>,
    reader_to_workers: &mut ReaderToWorkers<P>,
    to_executors: &mut ToExecutors<P>,
    to_execution_logger: &mut Option<ExecutionInfoSender<P>>,
    time: &RunTime,
) where
    P: Protocol + 'static,
{
    trace!("[server] reader message: {:?}", msg);
    if let Some((from_id, from_shard_id, msg)) = msg {
        handle_from_processes(
            worker_index,
            from_id,
            from_shard_id,
            msg,
            process,
            to_writers,
            reader_to_workers,
            to_executors,
            to_execution_logger,
            time,
        )
        .await
    } else {
        warn!(
            "[server] error while receiving new process message from readers"
        );
    }
}

async fn handle_from_processes<P>(
    worker_index: usize,
    from_id: ProcessId,
    from_shard_id: ShardId,
    msg: P::Message,
    process: &mut P,
    to_writers: &mut HashMap<ProcessId, Vec<WriterSender<P>>>,
    reader_to_workers: &mut ReaderToWorkers<P>,
    to_executors: &mut ToExecutors<P>,
    to_execution_logger: &mut Option<ExecutionInfoSender<P>>,
    time: &RunTime,
) where
    P: Protocol + 'static,
{
    // handle message in process and potentially new actions
    process.handle(from_id, from_shard_id, msg, time);
    send_to_processes_and_executors(
        worker_index,
        process,
        to_writers,
        reader_to_workers,
        to_executors,
        to_execution_logger,
        time,
    )
    .await;
}

// TODO maybe run in parallel
async fn send_to_processes_and_executors<P>(
    worker_index: usize,
    process: &mut P,
    to_writers: &mut HashMap<ProcessId, Vec<WriterSender<P>>>,
    reader_to_workers: &mut ReaderToWorkers<P>,
    to_executors: &mut ToExecutors<P>,
    to_execution_logger: &mut Option<ExecutionInfoSender<P>>,
    time: &RunTime,
) where
    P: Protocol + 'static,
{
    while let Some(action) = process.to_processes() {
        match action {
            Action::ToSend { target, msg } => {
                // check if should handle message locally
                if target.contains(&process.id()) {
                    // handle msg locally if self in `target`
                    handle_message_from_self::<P>(
                        worker_index,
                        msg.clone(),
                        process,
                        reader_to_workers,
                        time,
                    )
                    .await;
                }

                // prevent unnecessary cloning of messages, since send only
                // requires a reference to the message
                let msg_to_send = Arc::new(POEMessage::Protocol(msg));

                // send message to writers in target
                for (to, channels) in to_writers.iter_mut() {
                    if target.contains(to) {
                        send_to_one_writer::<P>(
                            "server",
                            msg_to_send.clone(),
                            channels,
                        )
                        .await
                    }
                }
            }
            Action::ToForward { msg } => {
                // handle msg locally if self in `target`
                handle_message_from_self(
                    worker_index,
                    msg,
                    process,
                    reader_to_workers,
                    time,
                )
                .await;
            }
        }
    }

    // notify executors
    for execution_info in process.to_executors_iter() {
        // if there's an execution logger, then also send execution info to it
        if let Some(to_execution_logger) = to_execution_logger {
            if let Err(e) =
                to_execution_logger.send(execution_info.clone()).await
            {
                warn!("[server] error while sending new execution info to execution logger: {:?}", e);
            }
        }
        // notify executor
        if let Err(e) = to_executors.forward(execution_info).await {
            warn!(
                "[server] error while sending new execution info to executor: {:?}",
                e
            );
        }
    }
}

async fn handle_message_from_self<P>(
    worker_index: usize,
    msg: P::Message,
    process: &mut P,
    reader_to_workers: &mut ReaderToWorkers<P>,
    time: &RunTime,
) where
    P: Protocol + 'static,
{
    // create msg to be forwarded
    let to_forward = (process.id(), process.shard_id(), msg);
    // only handle message from self in this worker if the destination worker is
    // us; this means that "messages to self are delivered immediately" is only
    // true for self messages to the same worker
    if reader_to_workers.only_to_self(&to_forward, worker_index) {
        process.handle(to_forward.0, to_forward.1, to_forward.2, time)
    } else {
        if let Err(e) = reader_to_workers.forward(to_forward).await {
            warn!("[server] error notifying process task with msg from self: {:?}", e);
        }
    }
}

pub async fn send_to_one_writer<P>(
    tag: &'static str,
    msg: Arc<POEMessage<P>>,
    writers: &mut Vec<WriterSender<P>>,
) where
    P: Protocol + 'static,
{
    // pick a random one
    let writer_index = rand::thread_rng().gen_range(0..writers.len());

    if let Err(e) = writers[writer_index].send(msg).await {
        warn!(
            "[{}] error while sending to writer {}: {:?}",
            tag, writer_index, e
        );
    }
}

async fn selected_from_clients<P>(
    worker_index: usize,
    cmd: Option<(Option<Dot>, Command)>,
    process: &mut P,
    to_writers: &mut HashMap<ProcessId, Vec<WriterSender<P>>>,
    reader_to_workers: &mut ReaderToWorkers<P>,
    to_executors: &mut ToExecutors<P>,
    to_execution_logger: &mut Option<ExecutionInfoSender<P>>,
    time: &RunTime,
) where
    P: Protocol + 'static,
{
    trace!("[server] from clients: {:?}", cmd);
    if let Some((dot, cmd)) = cmd {
        handle_from_clients(
            worker_index,
            dot,
            cmd,
            process,
            to_writers,
            reader_to_workers,
            to_executors,
            to_execution_logger,
            time,
        )
        .await
    } else {
        warn!("[server] error while receiving new command from clients");
    }
}

async fn handle_from_clients<P>(
    worker_index: usize,
    dot: Option<Dot>,
    cmd: Command,
    process: &mut P,
    to_writers: &mut HashMap<ProcessId, Vec<WriterSender<P>>>,
    reader_to_workers: &mut ReaderToWorkers<P>,
    to_executors: &mut ToExecutors<P>,
    to_execution_logger: &mut Option<ExecutionInfoSender<P>>,
    time: &RunTime,
) where
    P: Protocol + 'static,
{
    // submit command in process
    process.submit(dot, cmd, time);
    send_to_processes_and_executors(
        worker_index,
        process,
        to_writers,
        reader_to_workers,
        to_executors,
        to_execution_logger,
        time,
    )
    .await;
}

async fn selected_from_periodic_task<P, R>(
    worker_index: usize,
    event: Option<FromPeriodicMessage<P, R>>,
    process: &mut P,
    to_writers: &mut HashMap<ProcessId, Vec<WriterSender<P>>>,
    reader_to_workers: &mut ReaderToWorkers<P>,
    to_executors: &mut ToExecutors<P>,
    to_execution_logger: &mut Option<ExecutionInfoSender<P>>,
    time: &RunTime,
) where
    P: Protocol + 'static,
    R: Debug + 'static,
{
    trace!("[server] from periodic task: {:?}", event);
    if let Some(event) = event {
        handle_from_periodic_task(
            worker_index,
            event,
            process,
            to_writers,
            reader_to_workers,
            to_executors,
            to_execution_logger,
            time,
        )
        .await
    } else {
        warn!("[server] error while receiving new event from periodic task");
    }
}

async fn handle_from_periodic_task<P, R>(
    worker_index: usize,
    msg: FromPeriodicMessage<P, R>,
    process: &mut P,
    to_writers: &mut HashMap<ProcessId, Vec<WriterSender<P>>>,
    reader_to_workers: &mut ReaderToWorkers<P>,
    to_executors: &mut ToExecutors<P>,
    to_execution_logger: &mut Option<ExecutionInfoSender<P>>,
    time: &RunTime,
) where
    P: Protocol + 'static,
    R: Debug + 'static,
{
    match msg {
        FromPeriodicMessage::Event(event) => {
            // handle event in process
            process.handle_event(event, time);
            send_to_processes_and_executors(
                worker_index,
                process,
                to_writers,
                reader_to_workers,
                to_executors,
                to_execution_logger,
                time,
            )
            .await;
        }
        FromPeriodicMessage::Inspect(f, mut tx) => {
            let outcome = f(&process);
            if let Err(e) = tx.send(outcome).await {
                warn!("[server] error while sending inspect result: {:?}", e);
            }
        }
    }
}

async fn selected_from_executors<P>(
    worker_index: usize,
    committed_and_executed: Option<CommittedAndExecuted>,
    process: &mut P,
    to_writers: &mut HashMap<ProcessId, Vec<WriterSender<P>>>,
    reader_to_workers: &mut ReaderToWorkers<P>,
    to_executors: &mut ToExecutors<P>,
    to_execution_logger: &mut Option<ExecutionInfoSender<P>>,
    time: &RunTime,
) where
    P: Protocol + 'static,
{
    trace!("[server] from executors: {:?}", committed_and_executed);
    if let Some(committed_and_executed) = committed_and_executed {
        handle_from_executors(
            worker_index,
            committed_and_executed,
            process,
            to_writers,
            reader_to_workers,
            to_executors,
            to_execution_logger,
            time,
        )
        .await
    } else {
        warn!("[server] error while receiving message from executors");
    }
}

async fn handle_from_executors<P>(
    worker_index: usize,
    committed_and_executed: CommittedAndExecuted,
    process: &mut P,
    to_writers: &mut HashMap<ProcessId, Vec<WriterSender<P>>>,
    reader_to_workers: &mut ReaderToWorkers<P>,
    to_executors: &mut ToExecutors<P>,
    to_execution_logger: &mut Option<ExecutionInfoSender<P>>,
    time: &RunTime,
) where
    P: Protocol + 'static,
{
    process.handle_executed(committed_and_executed, time);
    send_to_processes_and_executors(
        worker_index,
        process,
        to_writers,
        reader_to_workers,
        to_executors,
        to_execution_logger,
        time,
    )
    .await;
}
