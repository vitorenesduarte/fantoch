use crate::config::Config;
use crate::executor::Executor;
use crate::id::ClientId;
use crate::log;
use crate::protocol::Protocol;
use crate::run::prelude::*;
use crate::run::task;
use futures::future::FutureExt;
use futures::select;
use std::collections::HashMap;

/// Starts executors.
pub fn start_executors<P>(
    config: Config,
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
        task::spawn(executor_task::<P>(config, from_workers, from_clients));
    }
}

async fn executor_task<P>(
    config: Config,
    mut from_workers: ExecutionInfoReceiver<P>,
    mut from_clients: ClientReceiver,
) where
    P: Protocol,
{
    // create executor
    let mut executor = P::Executor::new(config);

    // mapping from client id to its channel
    let mut clients = HashMap::new();

    loop {
        select! {
            execution_info = from_workers.recv().fuse() => {
                log!("[executor] from parent: {:?}", execution_info);
                if let Some(execution_info) = execution_info {
                    handle_execution_info::<P>(execution_info, &mut executor, &mut clients).await;
                } else {
                    println!("[executor] error while receiving execution info from parent");
                }
            }
            from_client = from_clients.recv().fuse() => {
                log!("[executor] from client: {:?}", from_client);
                if let Some(from_client) = from_client {
                    handle_from_client::<P>(from_client, &mut executor, &mut clients).await;
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
    clients: &mut HashMap<ClientId, CommandResultSender>,
) where
    P: Protocol,
{
    // forward executor results (commands or partial commands) to clients that are waiting for them
    for executor_result in executor.handle(execution_info) {
        // get client id
        let client_id = executor_result.client();
        // get client channel
        let tx = clients
            .get_mut(&client_id)
            .expect("command result should belong to a registered client");

        // TODO handle partial results
        let cmd_result = executor_result.unwrap_ready();

        // send executor result to client
        if let Err(e) = tx.send(cmd_result).await {
            println!(
                "[executor] error while sending to executor result to client {}: {:?}",
                client_id, e
            );
        }
    }
}

async fn handle_from_client<P>(
    from_client: FromClient,
    executor: &mut P::Executor,
    clients: &mut HashMap<ClientId, CommandResultSender>,
) where
    P: Protocol,
{
    match from_client {
        FromClient::WaitRifl(rifl) => {
            // register in executor
            executor.register_rifl(rifl);
        }
        FromClient::Register(client_id, tx) => {
            println!("[executor] client {} registered", client_id);
            let res = clients.insert(client_id, tx);
            assert!(res.is_none());
        }
        FromClient::Unregister(client_id) => {
            println!("[executor] client {} unregistered", client_id);
            let res = clients.remove(&client_id);
            assert!(res.is_some());
        }
    }
}
