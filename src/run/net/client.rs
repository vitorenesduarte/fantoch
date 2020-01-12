use super::{ClientHi, ProcessHi};
use crate::command::{Command, CommandResult};
use crate::id::{ClientId, ProcessId};
use crate::run::net::connection::Connection;
use crate::run::task;
use crate::run::FromClient;
use futures::future::FutureExt;
use futures::select;
use tokio::net::TcpListener;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

pub fn start_listener(
    process_id: ProcessId,
    listener: TcpListener,
) -> UnboundedReceiver<FromClient> {
    task::spawn_producer(|tx| client_listener_task(process_id, listener, tx))
}

/// Listen on new client connections and spawn a client task for each new connection.
async fn client_listener_task(
    process_id: ProcessId,
    listener: TcpListener,
    parent: UnboundedSender<FromClient>,
) {
    // start listener task
    let mut rx = task::spawn_producer(|tx| super::listener_task(listener, tx));

    loop {
        // handle new client connections
        match rx.recv().await {
            Some(connection) => {
                println!("[client_listener] new connection");
                // start client server task and give it the producer-end of the channel in order for
                // this client to notify parent
                task::spawn(client_server_task(process_id, connection, parent.clone()));
            }
            None => {
                println!("[client_listener] error receiving message from listener");
            }
        }
    }
}

/// Client server-side task. Checks messages both from the client connection (new commands) and
/// parent (new command results).
async fn client_server_task(
    process_id: ProcessId,
    mut connection: Connection,
    parent: UnboundedSender<FromClient>,
) {
    let (client_id, mut parent_results) =
        server_receive_hi(process_id, &mut connection, &parent).await;

    loop {
        select! {
            cmd = connection.recv().fuse() => {
                if !client_server_task_handle_cmd(cmd, client_id, &parent) {
                    return;
                }
            }
            cmd_result = parent_results.recv().fuse() => {
                client_server_task_handle_cmd_result(cmd_result, &mut connection).await;
            }
        }
    }
}

fn client_server_task_handle_cmd(
    cmd: Option<Command>,
    client_id: ClientId,
    parent: &UnboundedSender<FromClient>,
) -> bool {
    println!("new command: {:?}", cmd);
    if let Some(cmd) = cmd {
        if let Err(e) = parent.send(FromClient::Submit(cmd)) {
            println!(
                "[client_server] error while sending new command to parent: {:?}",
                e
            );
        }
        return true;
    } else {
        println!("[client_server] client disconnected.");
        if let Err(e) = parent.send(FromClient::Unregister(client_id)) {
            println!(
                "[client_server] error while sending unregister to parent: {:?}",
                e
            );
        }
        return false;
    }
}

async fn client_server_task_handle_cmd_result(
    cmd_result: Option<CommandResult>,
    connection: &mut Connection,
) {
    println!("new command result: {:?}", cmd_result);
    if let Some(cmd_result) = cmd_result {
        connection.send(cmd_result).await;
    } else {
        println!("[client_server] error while receiving new command result from parent");
    }
}

async fn server_receive_hi(
    process_id: ProcessId,
    connection: &mut Connection,
    parent: &UnboundedSender<FromClient>,
) -> (ClientId, UnboundedReceiver<CommandResult>) {
    // create channel where the process will write command results and where client will read them
    let (tx, rx) = task::channel();

    // receive hi from client and register in parent, sending it tx
    let client_id = if let Some(ClientHi(client_id)) = connection.recv().await {
        client_id
    } else {
        panic!("[client_server] couldn't receive client id from connected client");
    };

    // notify parent with the channel where it should write command results
    if let Err(e) = parent.send(FromClient::Register(client_id, tx)) {
        println!(
            "[client_server] error while registering client in parent: {:?}",
            e
        );
    }

    // say hi back
    let hi = ProcessHi(process_id);
    connection.send(hi).await;

    // return client id and channel where client should read command results
    (client_id, rx)
}

pub async fn client_say_hi(client_id: ClientId, connection: &mut Connection) -> ProcessId {
    // say hi
    let hi = ClientHi(client_id);
    connection.send(hi).await;

    // receive hi back
    if let Some(ProcessHi(process_id)) = connection.recv().await {
        process_id
    } else {
        panic!("[client] couldn't receive process id from connected process");
    }
}
