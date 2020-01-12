// This module contains the definition of `Connection`.
pub mod connection;

// This module contains the definition of ...
pub mod process;

// This module contains the definition of ...
pub mod client;

use crate::id::{ClientId, ProcessId};
use connection::Connection;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::future::Future;
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

#[derive(Debug, Serialize, Deserialize)]
struct ProcessHi(ProcessId);
#[derive(Debug, Serialize, Deserialize)]
struct ClientHi(ClientId);

/// Just a wrapper around tokio::spawn.
pub fn spawn<F>(task: F)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(task);
}

/// Just a wrapper around mpsc::unbounded_channel.
pub fn channel<M>() -> (UnboundedSender<M>, UnboundedReceiver<M>) {
    mpsc::unbounded_channel()
}

/// Spawns a single producer, returning the consumer-end of the channel.
pub fn spawn_producer<M, F>(producer: impl FnOnce(UnboundedSender<M>) -> F) -> UnboundedReceiver<M>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    // create channel and:
    // - pass the producer-end of the channel to producer
    // - return the consumer-end of the channel to the caller
    let (tx, rx) = channel();
    tokio::spawn(producer(tx));
    rx
}

/// Spawns many producers, returning the consumer-end of the channel.
pub fn spawn_producers<A, M, F>(
    args: Vec<A>,
    producer: impl Fn(A, UnboundedSender<M>) -> F,
) -> UnboundedReceiver<M>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    // create channel and:
    // - pass a clone of the producer-end of the channel to each producer
    // - return the consumer-end of the channel to the caller
    let (tx, rx) = channel();
    for arg in args {
        tokio::spawn(producer(arg, tx.clone()));
    }
    rx
}

/// Spawns a consumer, returning the producer-end of the channel.
pub fn spawn_consumer<M, F>(receiver: impl FnOnce(UnboundedReceiver<M>) -> F) -> UnboundedSender<M>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    // create channel and:
    // - pass the consumer-end of the channel to the consumer
    // - return the producer-end of the channel to the caller
    let (tx, rx) = channel();
    tokio::spawn(receiver(rx));
    tx
}

/// Connect to some address.
pub async fn connect<A>(address: A) -> Result<Connection, Box<dyn Error>>
where
    A: ToSocketAddrs,
{
    let stream = TcpStream::connect(address).await?;
    let connection = Connection::new(stream);
    Ok(connection)
}

/// Listen on some address.
pub async fn listen<A>(address: A) -> Result<TcpListener, Box<dyn Error>>
where
    A: ToSocketAddrs,
{
    Ok(TcpListener::bind(address).await?)
}

/// Listen on new connections and send them to parent process.
async fn listener_task(mut listener: TcpListener, parent: UnboundedSender<Connection>) {
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                println!("[listener] new connection: {:?}", addr);

                // create connection
                let connection = Connection::new(stream);

                if let Err(e) = parent.send(connection) {
                    println!("[listener] error sending stream to parent process: {:?}", e);
                }
            }
            Err(e) => println!("[listener] couldn't accept new connection: {:?}", e),
        }
    }
}
