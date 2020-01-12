use super::ProcessHi;
use crate::id::ProcessId;
use crate::protocol::ToSend;
use crate::run::net::connection::Connection;
use crate::run::task;
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::Debug;
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::time::Duration;

pub async fn connect_to_all<A, V>(
    process_id: ProcessId,
    listener: TcpListener,
    addresses: Vec<A>,
    connect_retries: usize,
) -> Result<(UnboundedReceiver<V>, UnboundedSender<ToSend<V>>), Box<dyn Error>>
where
    A: ToSocketAddrs + Debug,
    V: Debug + Serialize + DeserializeOwned + Send + 'static,
{
    // spawn listener
    let mut rx = task::spawn_producer(|tx| super::listener_task(listener, tx));

    // number of addresses
    let n = addresses.len();

    // create list of in and out connections:
    // - even though TCP is full-duplex, due to the current tokio parallel-tcp-socket-read-write
    //   limitation, we going to use in streams for reading and out streams for writing, which can
    //   be done in parallel
    let mut outgoing = Vec::with_capacity(n);
    let mut incoming = Vec::with_capacity(n);

    // connect to all addresses (and get the writers)
    for address in addresses {
        let mut tries = 0;
        loop {
            match super::connect(&address).await {
                Ok(connection) => {
                    // save connection if connected successfully
                    outgoing.push(connection);
                    break;
                }
                Err(e) => {
                    // if not, try again if we shouldn't give up (due to too many attempts)
                    tries += 1;
                    if tries < connect_retries {
                        println!("failed to connect to {:?}: {}", address, e);
                        println!(
                            "will try again in 1 second ({} out of {})",
                            tries, connect_retries,
                        );
                        tokio::time::delay_for(Duration::from_secs(1)).await;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
    }

    // receive from listener all connected (the readers)
    for _ in 0..n {
        let connection = rx
            .recv()
            .await
            .expect("should receive connection from listener");
        incoming.push(connection);
    }

    Ok(say_hi::<V>(process_id, incoming, outgoing).await)
}

async fn say_hi<V>(
    process_id: ProcessId,
    mut connections_0: Vec<Connection>,
    connections_1: Vec<Connection>,
) -> (UnboundedReceiver<V>, UnboundedSender<ToSend<V>>)
where
    V: Debug + Serialize + DeserializeOwned + Send + 'static,
{
    // say hi to all processes
    let hi = ProcessHi(process_id);
    for connection in connections_0.iter_mut() {
        connection.send(&hi).await;
    }
    println!("said hi to all processes");

    // create mapping from process id to connection
    let mut id_to_connection = HashMap::new();
    for mut connection in connections_1 {
        if let Some(ProcessHi(from)) = connection.recv().await {
            // save entry and check it has not been inserted before
            let res = id_to_connection.insert(from, connection);
            assert!(res.is_none());
        } else {
            panic!("error receiving hi");
        }
    }

    (
        start_readers::<V>(connections_0),
        start_broadcast_writer::<V>(id_to_connection),
    )
}

/// Starts a reader task per connection received and returns an unbounded channel to which
/// readers will write to.
fn start_readers<V>(connections: Vec<Connection>) -> UnboundedReceiver<V>
where
    V: Debug + DeserializeOwned + Send + 'static,
{
    task::spawn_producers(connections, |connection, tx| {
        reader_task::<V>(connection, tx)
    })
}

fn start_broadcast_writer<V>(
    connections: HashMap<ProcessId, Connection>,
) -> UnboundedSender<ToSend<V>>
where
    V: Serialize + Send + 'static,
{
    // mapping from process id to channel broadcast writer should write to
    let mut writers = HashMap::new();

    // start on writer task per connection
    for (process_id, connection) in connections {
        // create channel where parent should write to
        let tx = task::spawn_consumer(|rx| writer_task(connection, rx));
        writers.insert(process_id, tx);
    }

    // spawn broadcast writer
    task::spawn_consumer(|rx| broadcast_writer_task::<V>(writers, rx))
}

/// Reader task.
async fn reader_task<V>(mut connection: Connection, parent: UnboundedSender<V>)
where
    V: Debug + DeserializeOwned + Send + 'static,
{
    loop {
        match connection.recv().await {
            Some(msg) => {
                if let Err(e) = parent.send(msg) {
                    println!("[reader] error notifying parent task with new msg: {:?}", e);
                }
            }
            None => {
                println!("[reader] error receiving message from connection");
            }
        }
    }
}

/// Broadcast Writer task.
async fn broadcast_writer_task<V>(
    mut writers: HashMap<ProcessId, UnboundedSender<Bytes>>,
    mut parent: UnboundedReceiver<ToSend<V>>,
) where
    V: Serialize + Send + 'static,
{
    loop {
        if let Some(ToSend { target, msg, .. }) = parent.recv().await {
            // serialize message
            let bytes = Connection::serialize(&msg);
            for id in target {
                // find writer
                let writer = writers
                    .get_mut(&id)
                    .expect("[broadcast_writer] identifier in target should have a writer");
                if let Err(e) = writer.send(bytes.clone()) {
                    println!(
                        "[broadcast_writer] error sending bytes to writer {:?}: {:?}",
                        id, e
                    );
                }
            }
        } else {
            println!("[broadcast_writer] error receiving message from parent");
        }
    }
}

/// Writer task.
async fn writer_task(mut connection: Connection, mut parent: UnboundedReceiver<Bytes>) {
    loop {
        if let Some(bytes) = parent.recv().await {
            connection.send_serialized(bytes).await;
        } else {
            println!("[writer] error receiving message from parent");
        }
    }
}
