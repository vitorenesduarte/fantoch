use crate::config::Config;
use crate::executor::Executor;
use crate::id::{ClientId, ProcessId, ShardId};
use crate::protocol::Protocol;
use crate::run::prelude::*;
use crate::run::task;
use crate::time::RunTime;
use crate::HashMap;
use crate::{debug, trace, warn};
use std::sync::Arc;
use tokio::time;

/// Starts executors.
pub fn start_executors<P>(
    process_id: ProcessId,
    shard_id: ShardId,
    config: Config,
    to_executors_rxs: Vec<ExecutionInfoReceiver<P>>,
    client_to_executors_rxs: Vec<ClientToExecutorReceiver>,
    executors_to_workers: ExecutorsToWorkers,
    shard_writers: HashMap<ShardId, Vec<WriterSender<P>>>,
    to_executors: ToExecutors<P>,
    to_metrics_logger: Option<ExecutorMetricsSender>,
) where
    P: Protocol + 'static,
{
    // zip rxs'
    let incoming = to_executors_rxs
        .into_iter()
        .zip(client_to_executors_rxs.into_iter());

    // create executor
    let executor = P::Executor::new(process_id, shard_id, config);

    // create executor workers
    for (executor_index, (from_workers, from_clients)) in incoming.enumerate() {
        task::spawn(executor_task::<P>(
            executor_index,
            executor.clone(),
            shard_id,
            config,
            from_workers,
            from_clients,
            executors_to_workers.clone(),
            shard_writers.clone(),
            to_executors.clone(),
            to_metrics_logger.clone(),
        ));
    }
}

async fn executor_task<P>(
    executor_index: usize,
    mut executor: P::Executor,
    shard_id: ShardId,
    config: Config,
    mut from_workers: ExecutionInfoReceiver<P>,
    mut from_clients: ClientToExecutorReceiver,
    mut executors_to_workers: ExecutorsToWorkers,
    mut shard_writers: HashMap<ShardId, Vec<WriterSender<P>>>,
    mut to_executors: ToExecutors<P>,
    mut to_metrics_logger: Option<ExecutorMetricsSender>,
) where
    P: Protocol + 'static,
{
    // set executor index
    executor.set_executor_index(executor_index);

    // create time
    let time = RunTime;

    // holder of all client info
    let mut to_clients = ToClients::new();

    // create a tokio sleep
    let sleep = |interval| Box::pin(time::sleep(interval));

    // create executors cleanup interval
    let gen_cleanup_delay = || sleep(config.executor_cleanup_interval());
    let mut cleanup_delay = gen_cleanup_delay();

    // create executors executed notification delay
    let gen_executed_notification_delay =
        || sleep(config.executor_executed_notification_interval());
    let mut executed_notification_delay = gen_executed_notification_delay();

    // create metrics interval
    let gen_metrics_delay = || sleep(super::metrics_logger::METRICS_INTERVAL);
    let mut metrics_delay = gen_metrics_delay();

    // check if executors monitor pending interval is set
    if let Some(monitor_pending_interval) =
        config.executor_monitor_pending_interval()
    {
        // create executors monitor pending interval
        let gen_monitor_pending_delay =
            || Box::pin(time::sleep(monitor_pending_interval));
        let mut monitor_pending_delay = gen_monitor_pending_delay();

        loop {
            tokio::select! {
                _ = &mut monitor_pending_delay => {
                    executor.monitor_pending(&time);
                    monitor_pending_delay = gen_monitor_pending_delay();
                }
                execution_info = from_workers.recv() => {
                    handle_execution_info(execution_info, &mut executor, shard_id, &mut shard_writers, &mut to_executors, &mut to_clients, &time).await;
                }
                from_client = from_clients.recv() => {
                    handle_from_client::<P>(from_client, &mut to_clients).await;
                }
                _ = &mut cleanup_delay => {
                    cleanup_tick(&mut executor, shard_id, &mut shard_writers, &mut to_executors, &mut to_clients, &time).await;
                    cleanup_delay = gen_cleanup_delay();
                }
                _ = &mut executed_notification_delay => {
                    executed_notification_tick::<P>(&mut executor, &mut executors_to_workers, &time).await;
                    executed_notification_delay = gen_executed_notification_delay();
                }
                _ = &mut metrics_delay => {
                    metrics_tick::<P>(executor_index, &mut executor, &mut to_metrics_logger).await;
                    metrics_delay = gen_metrics_delay();
                }
            }
        }
    } else {
        loop {
            tokio::select! {
                execution_info = from_workers.recv() => {
                    handle_execution_info(execution_info, &mut executor, shard_id, &mut shard_writers, &mut to_executors, &mut to_clients, &time).await;
                }
                from_client = from_clients.recv() => {
                    handle_from_client::<P>(from_client, &mut to_clients).await;
                }
                _ = &mut cleanup_delay => {
                    cleanup_tick(&mut executor, shard_id, &mut shard_writers, &mut to_executors, &mut to_clients, &time).await;
                    cleanup_delay = gen_cleanup_delay();
                }
                _ = &mut executed_notification_delay => {
                    executed_notification_tick::<P>(&mut executor, &mut executors_to_workers, &time).await;
                    executed_notification_delay = gen_executed_notification_delay();
                }
                _ = &mut metrics_delay  => {
                    metrics_tick::<P>(executor_index, &mut executor, &mut to_metrics_logger).await;
                    metrics_delay = gen_metrics_delay();
                }
            }
        }
    }
}

async fn handle_execution_info<P>(
    execution_info: Option<<P::Executor as Executor>::ExecutionInfo>,
    executor: &mut P::Executor,
    shard_id: ShardId,
    shard_writers: &mut HashMap<ShardId, Vec<WriterSender<P>>>,
    to_executors: &mut ToExecutors<P>,
    to_clients: &mut ToClients,
    time: &RunTime,
) where
    P: Protocol + 'static,
{
    trace!("[executor] from workers: {:?}", execution_info);
    if let Some(execution_info) = execution_info {
        executor.handle(execution_info, time);
        fetch_results(
            executor,
            shard_id,
            shard_writers,
            to_executors,
            to_clients,
        )
        .await;
    } else {
        warn!("[executor] error while receiving execution info from worker");
    }
}

async fn fetch_results<P>(
    executor: &mut P::Executor,
    shard_id: ShardId,
    shard_writers: &mut HashMap<ShardId, Vec<WriterSender<P>>>,
    to_executors: &mut ToExecutors<P>,
    to_clients: &mut ToClients,
) where
    P: Protocol + 'static,
{
    fetch_new_command_results::<P>(executor, to_clients).await;
    fetch_info_to_executors::<P>(
        executor,
        shard_id,
        shard_writers,
        to_executors,
    )
    .await;
}

async fn fetch_new_command_results<P>(
    executor: &mut P::Executor,
    to_clients: &mut ToClients,
) where
    P: Protocol,
{
    // forward executor results (commands or partial commands) to clients that
    // are waiting for them
    for executor_result in executor.to_clients_iter() {
        // get client id
        let client_id = executor_result.rifl.source();

        // send executor result to client (in case it is registered)
        if let Some(executor_results_tx) = to_clients.to_client(&client_id) {
            if let Err(e) = executor_results_tx.send(executor_result).await {
                warn!(
                    "[executor] error while sending executor result to client {}: {:?}",
                    client_id, e
                );
            }
        }
    }
}

async fn fetch_info_to_executors<P>(
    executor: &mut P::Executor,
    shard_id: ShardId,
    shard_writers: &mut HashMap<ShardId, Vec<WriterSender<P>>>,
    to_executors: &mut ToExecutors<P>,
) where
    P: Protocol + 'static,
{
    // forward execution info to other shards
    for (target_shard, execution_info) in executor.to_executors_iter() {
        debug!(
            "[executor] to executors in shard {}: {:?}",
            target_shard, execution_info
        );
        // check if it's a message to self
        if shard_id == target_shard {
            // notify executor
            if let Err(e) = to_executors.forward(execution_info).await {
                warn!("[executor] error while notifying other executors with new execution info: {:?}", e);
            }
        } else {
            let msg_to_send = Arc::new(POEMessage::Executor(execution_info));
            if let Some(channels) = shard_writers.get_mut(&target_shard) {
                crate::run::task::server::process::send_to_one_writer::<P>(
                    "executor",
                    msg_to_send,
                    channels,
                )
                .await
            } else {
                panic!(
                    "[executor] tried to send a message to a non-connected shard"
                );
            }
        }
    }
}

async fn handle_from_client<P>(
    from_client: Option<ClientToExecutor>,
    to_clients: &mut ToClients,
) where
    P: Protocol,
{
    trace!("[executor] from client: {:?}", from_client);
    if let Some(from_client) = from_client {
        match from_client {
            ClientToExecutor::Register(client_ids, executor_results_tx) => {
                to_clients.register(client_ids, executor_results_tx);
            }
            ClientToExecutor::Unregister(client_ids) => {
                to_clients.unregister(client_ids);
            }
        }
    } else {
        warn!("[executor] error while receiving new command from clients");
    }
}

async fn cleanup_tick<P>(
    executor: &mut P::Executor,
    shard_id: ShardId,
    shard_writers: &mut HashMap<ShardId, Vec<WriterSender<P>>>,
    to_executors: &mut ToExecutors<P>,
    to_clients: &mut ToClients,
    time: &RunTime,
) where
    P: Protocol + 'static,
{
    trace!("[executor] cleanup");
    executor.cleanup(time);
    fetch_results(executor, shard_id, shard_writers, to_executors, to_clients)
        .await;
}

async fn executed_notification_tick<P>(
    executor: &mut P::Executor,
    executors_to_workers: &mut ExecutorsToWorkers,
    time: &RunTime,
) where
    P: Protocol + 'static,
{
    trace!("[executor] executed");
    if let Some(executed) = executor.executed(time) {
        if let Err(e) = executors_to_workers.forward(executed).await {
            warn!(
                "[executor] error while sending executed to workers: {:?}",
                e
            );
        }
    }
}

async fn metrics_tick<P>(
    executor_index: usize,
    executor: &mut P::Executor,
    to_metrics_logger: &mut Option<ExecutorMetricsSender>,
) where
    P: Protocol + 'static,
{
    if let Some(to_metrics_logger) = to_metrics_logger.as_mut() {
        // send metrics to logger (in case there's one)
        let executor_metrics = executor.metrics().clone();
        if let Err(e) = to_metrics_logger
            .send((executor_index, executor_metrics))
            .await
        {
            warn!("[executor] error while sending metrics to metrics logger: {:?}", e);
        }
    }
}

struct ToClients {
    /// since many `ClientId` can share the same `ExecutorResultSender`, in
    /// order to avoid cloning these senders we'll have this additional index
    /// that tells us which `ToClient` to use for each `ClientId`
    next_id: usize,
    index: HashMap<ClientId, usize>,
    to_clients: HashMap<usize, ExecutorResultSender>,
}

impl ToClients {
    fn new() -> Self {
        Self {
            next_id: 0,
            index: HashMap::new(),
            to_clients: HashMap::new(),
        }
    }

    fn register(
        &mut self,
        client_ids: Vec<ClientId>,
        executor_results_tx: ExecutorResultSender,
    ) {
        // compute id for this set of clients
        let id = self.next_id;
        self.next_id += 1;

        // map each `ClientId` to the computed id
        for client_id in client_ids {
            trace!("[executor] clients {} registered", client_id);
            assert!(
                self.index.insert(client_id, id).is_none(),
                "client already registered"
            );
        }

        // save executor result sender
        assert!(self.to_clients.insert(id, executor_results_tx).is_none());
    }

    fn unregister(&mut self, client_ids: Vec<ClientId>) {
        let mut ids: Vec<_> = client_ids
            .into_iter()
            .filter_map(|client_id| {
                trace!("[executor] clients {} unregistered", client_id);
                self.index.remove(&client_id)
            })
            .collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 1, "id indexing client ids should be the same");

        assert!(self.to_clients.remove(&ids[0]).is_some());
    }

    fn to_client(
        &mut self,
        client_id: &ClientId,
    ) -> Option<&mut ExecutorResultSender> {
        // search index
        if let Some(id) = self.index.get(client_id) {
            // get client channel
            Some(
                self.to_clients
                    .get_mut(id)
                    .expect("indexed client not found"),
            )
        } else {
            None
        }
    }
}
