use crate::config::Config;
use crate::executor::Executor;
use crate::id::{ClientId, ProcessId};
use crate::log;
use crate::protocol::Protocol;
use crate::run::prelude::*;
use crate::run::task;
use std::collections::HashMap;

/// Starts executors.
pub fn start_executors<P>(
    process_id: ProcessId,
    config: Config,
    executors: usize,
    worker_to_executors_rxs: Vec<ExecutionInfoReceiver<P>>,
    client_to_executors_rxs: Vec<ClientReceiver>,
) where
    P: Protocol + 'static,
{
    // zip rxs'
    let incoming = worker_to_executors_rxs
        .into_iter()
        .zip(client_to_executors_rxs.into_iter());

    // create executor workers
    for (from_workers, from_clients) in incoming {
        task::spawn(executor_task::<P>(
            process_id,
            config,
            executors,
            from_workers,
            from_clients,
        ));
    }
}

async fn executor_task<P>(
    process_id: ProcessId,
    config: Config,
    executors: usize,
    mut from_workers: ExecutionInfoReceiver<P>,
    mut from_clients: ClientReceiver,
) where
    P: Protocol,
{
    // create executor
    let mut executor = P::Executor::new(process_id, config, executors);

    // mapping from client id to its rifl acks channel
    let mut client_rifl_acks = HashMap::new();
    // mapping from client id to its executor results channel
    let mut client_executor_results = HashMap::new();

    loop {
        tokio::select! {
            execution_info = from_workers.recv() => {
                log!("[executor] from parent: {:?}", execution_info);
                if let Some(execution_info) = execution_info {
                    handle_execution_info::<P>(execution_info, &mut executor, &mut client_executor_results).await;
                } else {
                    println!("[executor] error while receiving execution info from parent");
                }
            }
            from_client = from_clients.recv() => {
                log!("[executor] from client: {:?}", from_client);
                if let Some(from_client) = from_client {
                    handle_from_client::<P>(from_client, &mut executor, &mut client_rifl_acks, &mut client_executor_results).await;
                } else {
                    println!("[executor] error while receiving new command from clients");
                }
            }
        }
    }
}

async fn handle_execution_info<P>(
    execution_info: <P::Executor as Executor>::ExecutionInfo,
    executor: &mut P::Executor,
    client_executor_results: &mut HashMap<ClientId, ExecutorResultSender>,
) where
    P: Protocol,
{
    // forward executor results (commands or partial commands) to clients that
    // are waiting for them
    for executor_result in executor.handle(execution_info) {
        // get client id
        let client_id = executor_result.client();
        // get client channel
        let tx = client_executor_results
            .get_mut(&client_id)
            .expect("command result should belong to a registered client");

        // send executor result to client
        if let Err(e) = tx.send(executor_result).await {
            println!(
                "[executor] error while sending executor result to client {}: {:?}",
                client_id, e
            );
        }
    }
}

async fn handle_from_client<P>(
    from_client: FromClient,
    executor: &mut P::Executor,
    client_rifl_acks: &mut HashMap<ClientId, RiflAckSender>,
    client_executor_results: &mut HashMap<ClientId, ExecutorResultSender>,
) where
    P: Protocol,
{
    match from_client {
        // TODO maybe send the channel in the wait for rifl msg
        FromClient::WaitForRifl(rifl) => {
            // register in executor
            executor.wait_for_rifl(rifl);

            // get client id
            let client_id = rifl.source();
            // get client channel
            let tx = client_rifl_acks
                .get_mut(&client_id)
                .expect("wait for rifl should belong to a registered client");

            // send executor result to client
            if let Err(e) = tx.send(rifl).await {
                println!(
                    "[executor] error while sending rifl ack to client {}: {:?}",
                    client_id, e
                );
            }
        }
        FromClient::Register(client_ids, rifl_acks_tx, executor_results_tx) => {
            for client_id in client_ids {
                log!("[executor] clients {} registered", client_id);
                let res = client_rifl_acks.insert(client_id, rifl_acks_tx.clone());
                assert!(res.is_none());
                let res = client_executor_results
                    .insert(client_id, executor_results_tx.clone());
                assert!(res.is_none());
            }
        }
        FromClient::Unregister(client_ids) => {
            for client_id in client_ids {
                log!("[executor] client {} unregistered", client_id);
                let res = client_rifl_acks.remove(&client_id);
                assert!(res.is_some());
                let res = client_executor_results.remove(&client_id);
                assert!(res.is_some());
            }
        }
    }
}
