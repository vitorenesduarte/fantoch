use super::{ClientHi, ProcessHi};
use crate::command::CommandResult;
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
    let mut parent = receive_hi(process_id, &mut connection, parent).await;

    loop {
        select! {
            new_submit = connection.recv::<String>().fuse() => {
                println!("new submit: {:?}", new_submit);
            }
            new_msg = parent.recv().fuse() => {
                println!("new msg: {:?}", new_msg);
            }
        }
    }
}

async fn receive_hi(
    process_id: ProcessId,
    connection: &mut Connection,
    parent: UnboundedSender<FromClient>,
) -> UnboundedReceiver<CommandResult> {
    // create channel where the process will write command results and where client will read them
    let (tx, rx) = task::channel();

    // receive hi from client and register in parent, sending it tx
    let client_id = if let Some(ClientHi(client_id)) = connection.recv().await {
        client_id
    } else {
        panic!("couldn't receive client id from connected client");
    };

    // notify parent with the channel where it should write command results
    if let Err(e) = parent.send(FromClient::Register(client_id, tx)) {
        println!("error while registering client in parent: {:?}", e);
    }

    // say hi back
    let hi = ProcessHi(process_id);
    connection.send(hi).await;

    // return channel where client should read command results
    rx
}

pub async fn say_hi(client_id: ClientId, connection: &mut Connection) -> ProcessId {
    // say hi
    let hi = ClientHi(client_id);
    connection.send(hi).await;

    // receive hi back
    if let Some(ProcessHi(process_id)) = connection.recv().await {
        process_id
    } else {
        panic!("couldn't receive process id from connected process");
    }
}
