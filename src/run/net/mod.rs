// This module contains the definition of `Connection`.
mod connection;

use crate::id::ProcessId;
use crate::protocol::Process;
use connection::Connection;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::Debug;
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::time::Duration;

const LOCALHOST: &str = "127.0.0.1";
const CONNECT_RETRIES: usize = 100;

/// Connect to all processes. It receives:
/// - local port to bind to
/// - list of addresses to connect to
pub async fn connect_to_all<A>(
    process_id: ProcessId,
    port: u16,
    addresses: Vec<A>,
) -> Result<(Vec<Connection>, HashMap<ProcessId, Connection>), Box<dyn Error>>
where
    A: ToSocketAddrs + Debug + Clone,
{
    // connect to all
    let (connections_0, connections_1) = connect(port, addresses).await?;

    // say hi
    say_hi(process_id, connections_0, connections_1).await
}

/// Starts a reader task per connection received and returns an unbounded channel to which readers
/// will write to.
pub async fn start_readers<P>(
    connections: Vec<Connection>,
) -> Result<UnboundedReceiver<P::Message>, Box<dyn Error>>
where
    P: Process + 'static, // TODO what does this 'static do?
{
    // create channel where readers should write to
    let (tx, rx) = mpsc::unbounded_channel();

    for connection in connections {
        tokio::spawn(reader_task::<P>(connection, tx.clone()));
    }

    Ok(rx)
}

async fn connect<A>(
    port: u16,
    addresses: Vec<A>,
) -> Result<(Vec<Connection>, Vec<Connection>), Box<dyn Error>>
where
    A: ToSocketAddrs + Debug + Clone,
{
    // number of processes
    let n = addresses.len();

    // try to bind localy
    let listener = TcpListener::bind((LOCALHOST, port)).await?;

    // create channel to communicate with listener
    let (tx, mut rx) = mpsc::unbounded_channel();

    // spawn listener
    tokio::spawn(listener_task(listener, tx));

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
            match TcpStream::connect(address.clone()).await {
                Ok(stream) => {
                    // save stream if connected successfully
                    let connection = Connection::new(stream);
                    outgoing.push(connection);
                    break;
                }
                Err(e) => {
                    // if not, try again if we shouldn't give up (due to too many attempts)
                    tries += 1;
                    if tries < CONNECT_RETRIES {
                        println!("failed to connect to {:?}: {}", address, e);
                        println!("will try again in 1 second");
                        tokio::time::delay_for(Duration::from_secs(1)).await;
                    } else {
                        return Err(Box::new(e));
                    }
                }
            }
        }
    }

    // receive from listener all connected (the readers)
    for _ in 0..n {
        let stream = rx
            .recv()
            .await
            .expect("should receive stream from listener");
        let connection = Connection::new(stream);
        incoming.push(connection);
    }

    Ok((incoming, outgoing))
}

async fn say_hi(
    process_id: ProcessId,
    connections: Vec<Connection>,
    mut connections_say_hi: Vec<Connection>,
) -> Result<(Vec<Connection>, HashMap<ProcessId, Connection>), Box<dyn Error>> {
    // say hi to all processes
    #[derive(Serialize, Deserialize)]
    struct Hi(ProcessId);

    let hi = Hi(process_id);
    for connection in connections_say_hi.iter_mut() {
        connection.send(&hi).await;
    }
    println!("said hi to all processes");

    // create mapping from process id to connection
    let mut id_to_connection = HashMap::new();
    for mut connection in connections {
        if let Some(Hi(from)) = connection.recv().await {
            // save entry and check it has not been inserted before
            let res = id_to_connection.insert(from, connection);
            assert!(res.is_none());
        } else {
            panic!("error receiving hi");
        }
    }
    Ok((connections_say_hi, id_to_connection))
}

/// Listen on new connections and send them to parent process.
async fn listener_task(mut listener: TcpListener, parent: UnboundedSender<TcpStream>) {
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                println!("[listener] new connection: {:?}", addr);

                if let Err(e) = parent.send(stream) {
                    println!("[listener] error sending stream to parent process: {:?}", e);
                }
            }
            Err(e) => println!("[listener] couldn't accept new connection: {:?}", e),
        }
    }
}

/// Reader task.
async fn reader_task<P>(mut connection: Connection, parent: UnboundedSender<P::Message>)
where
    P: Process + 'static, // TODO what does this 'static do?
{
    loop {
        match connection.recv().await {
            Some(msg) => {
                if let Err(e) = parent.send(msg) {
                    println!("[reader] error notifying parent task with new msg: {:?}", e);
                }
            }
            None => {
                println!("[reader] error receiving message");
            }
        }
    }
}
