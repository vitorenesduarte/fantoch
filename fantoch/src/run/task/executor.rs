use crate::config::Config;
use crate::executor::Executor;
use crate::id::{ClientId, ProcessId, ShardId};
use crate::log;
use crate::protocol::Protocol;
use crate::run::prelude::*;
use crate::run::task;
use crate::HashMap;
use tokio::time;

/// Starts executors.
pub fn start_executors<P>(
    process_id: ProcessId,
    shard_id: ShardId,
    config: Config,
    executors: usize,
    worker_to_executors_rxs: Vec<ExecutionInfoReceiver<P>>,
    client_to_executors_rxs: Vec<ClientToExecutorReceiver>,
    to_metrics_logger: Option<ExecutorMetricsSender>,
) where
    P: Protocol + 'static,
{
    // zip rxs'
    let incoming = worker_to_executors_rxs
        .into_iter()
        .zip(client_to_executors_rxs.into_iter());

    // create executor workers
    for (executor_index, (from_workers, from_clients)) in incoming.enumerate() {
        task::spawn(executor_task::<P>(
            executor_index,
            process_id,
            shard_id,
            config,
            executors,
            from_workers,
            from_clients,
            to_metrics_logger.clone(),
        ));
    }
}

async fn executor_task<P>(
    executor_index: usize,
    process_id: ProcessId,
    shard_id: ShardId,
    config: Config,
    executors: usize,
    mut from_workers: ExecutionInfoReceiver<P>,
    mut from_clients: ClientToExecutorReceiver,
    mut to_metrics_logger: Option<ExecutorMetricsSender>,
) where
    P: Protocol,
{
    // create executor
    let mut executor =
        P::Executor::new(process_id, shard_id, config, executors);

    // holder of all client info
    let mut to_clients = ToClients::new();

    // create interval (for metrics notification)
    let mut interval = time::interval(super::metrics_logger::METRICS_INTERVAL);

    loop {
        tokio::select! {
            execution_info = from_workers.recv() => {
                log!("[executor] from parent: {:?}", execution_info);
                if let Some(execution_info) = execution_info {
                    handle_execution_info::<P>(execution_info, &mut executor, &mut to_clients).await;
                } else {
                    println!("[executor] error while receiving execution info from parent");
                }
            }
            from_client = from_clients.recv() => {
                log!("[executor] from client: {:?}", from_client);
                if let Some(from_client) = from_client {
                    handle_from_client::<P>(from_client, &mut executor, &mut to_clients).await;
                } else {
                    println!("[executor] error while receiving new command from clients");
                }
            }
            _ = interval.tick()  => {
                if let Some(to_metrics_logger) = to_metrics_logger.as_mut() {
                    // send metrics to logger (in case there's one)
                    let executor_metrics = executor.metrics().clone();
                    if let Err(e) = to_metrics_logger.send((executor_index, executor_metrics)).await {
                        println!("[executor] error while sending metrics to metrics logger: {:?}", e);
                    }
                }
            }
        }
    }
}

async fn handle_execution_info<P>(
    execution_info: <P::Executor as Executor>::ExecutionInfo,
    executor: &mut P::Executor,
    to_clients: &mut ToClients,
) where
    P: Protocol,
{
    // forward executor results (commands or partial commands) to clients that
    // are waiting for them
    executor.handle(execution_info);
    for executor_result in executor.to_clients_iter() {
        // get client id
        let client_id = executor_result.client();

        // send executor result to client
        let send = to_clients
            .to_client(&client_id)
            .executor_results_tx
            .send(executor_result)
            .await;
        if let Err(e) = send {
            println!(
                "[executor] error while sending executor result to client {}: {:?}",
                client_id, e
            );
        }
    }
}

async fn handle_from_client<P>(
    from_client: ClientToExecutor,
    executor: &mut P::Executor,
    to_clients: &mut ToClients,
) where
    P: Protocol,
{
    match from_client {
        // TODO maybe send the channel in the wait for rifl msg
        ClientToExecutor::WaitForRifl(rifl) => {
            // register in executor
            executor.wait_for_rifl(rifl);

            // get client id
            let client_id = rifl.source();

            // send executor result to client
            let send = to_clients
                .to_client(&client_id)
                .rifl_acks_tx
                .send(rifl)
                .await;
            if let Err(e) = send {
                println!(
                    "[executor] error while sending rifl ack to client {}: {:?}",
                    client_id, e
                );
            }
        }
        ClientToExecutor::Register(
            client_ids,
            rifl_acks_tx,
            executor_results_tx,
        ) => {
            to_clients.register(client_ids, rifl_acks_tx, executor_results_tx);
        }
        ClientToExecutor::Unregister(client_ids) => {
            to_clients.unregister(client_ids);
        }
    }
}

struct ToClient {
    rifl_acks_tx: RiflAckSender,
    executor_results_tx: ExecutorResultSender,
}

impl ToClient {
    fn new(
        rifl_acks_tx: RiflAckSender,
        executor_results_tx: ExecutorResultSender,
    ) -> Self {
        Self {
            rifl_acks_tx,
            executor_results_tx,
        }
    }
}

struct ToClients {
    /// since many `ClientId` can share the same `ToClient`, in order to avoid
    /// cloning these senders we'll have this additional index that tells us
    /// which `ToClient` to use for each `ClientId`
    next_id: usize,
    index: HashMap<ClientId, usize>,
    to_clients: HashMap<usize, ToClient>,
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
        rifl_acks_tx: RiflAckSender,
        executor_results_tx: ExecutorResultSender,
    ) {
        // compute id for this set of clients
        let id = self.next_id;
        self.next_id += 1;

        // map each `ClientId` to the computed id
        for client_id in client_ids {
            log!("[executor] clients {} registered", client_id);
            assert!(
                self.index.insert(client_id, id).is_none(),
                "client already registered"
            );
        }

        // create `ToClient` and save it
        let to_client = ToClient::new(rifl_acks_tx, executor_results_tx);
        assert!(self.to_clients.insert(id, to_client).is_none());
    }

    fn unregister(&mut self, client_ids: Vec<ClientId>) {
        let mut ids: Vec<_> = client_ids
            .into_iter()
            .filter_map(|client_id| {
                log!("[executor] clients {} unregistered", client_id);
                self.index.remove(&client_id)
            })
            .collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 1, "id indexing client ids should be the same");

        assert!(self.to_clients.remove(&ids[0]).is_some());
    }

    fn to_client(&mut self, client_id: &ClientId) -> &mut ToClient {
        // search index
        let id = self
            .index
            .get(client_id)
            .expect("command result should belong to an indexed client");
        // get client channels
        self.to_clients
            .get_mut(id)
            .expect("indexed client not found")
    }
}
