mod util;

// This module contains server's side logic.
pub mod server;

// This module contains client's side logic.
pub mod client;

use crate::run::chan::{self, ChannelReceiver, ChannelSender};
use crate::run::rw::Connection;
use crate::{info, trace, warn};
use color_eyre::Report;
use std::fmt::Debug;
use std::future::Future;
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::task::JoinHandle;
use tokio::time::Duration;

/// Just a wrapper around tokio::spawn.
pub fn spawn<F>(task: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(task)
}

/// Spawns a single producer, returning the consumer-end of the channel.
pub fn spawn_producer<M, F>(
    channel_buffer_size: usize,
    producer: impl FnOnce(ChannelSender<M>) -> F,
) -> ChannelReceiver<M>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    // create channel and:
    // - pass the producer-end of the channel to producer
    // - return the consumer-end of the channel to the caller
    let (tx, rx) = chan::channel(channel_buffer_size);
    spawn(producer(tx));
    rx
}

/// Spawns many producers, returning the consumer-end of the channel.
pub fn spawn_producers<A, T, M, F>(
    channel_buffer_size: usize,
    args: T,
    producer: impl Fn(A, ChannelSender<M>) -> F,
) -> ChannelReceiver<M>
where
    T: IntoIterator<Item = A>,
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    // create channel and:
    // - pass a clone of the producer-end of the channel to each producer
    // - return the consumer-end of the channel to the caller
    let (tx, rx) = chan::channel(channel_buffer_size);
    for arg in args {
        spawn(producer(arg, tx.clone()));
    }
    rx
}

/// Spawns a consumer, returning the producer-end of the channel.
pub fn spawn_consumer<M, F>(
    channel_buffer_size: usize,
    consumer: impl FnOnce(ChannelReceiver<M>) -> F,
) -> ChannelSender<M>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    // create channel and:
    // - pass the consumer-end of the channel to the consumer
    // - return the producer-end of the channel to the caller
    let (tx, rx) = chan::channel(channel_buffer_size);
    spawn(consumer(rx));
    tx
}

/// Spawns a producer/consumer, returning two channels: the consumer-end and the
/// producer-end of the channels.
pub fn spawn_producer_and_consumer<M, N, F>(
    channel_buffer_size: usize,
    task: impl FnOnce(ChannelSender<M>, ChannelReceiver<N>) -> F,
) -> (ChannelReceiver<M>, ChannelSender<N>)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    // create two channels and:
    // - pass the producer-end of the 1st channel and the consumer-end of the
    //   2nd channel to the task
    // - return the consumer-end of the 1st channel and the producer-end of the
    //   2nd channel to the caller
    let (tx1, rx1) = chan::channel(channel_buffer_size);
    let (tx2, rx2) = chan::channel(channel_buffer_size);
    spawn(task(tx1, rx2));
    (rx1, tx2)
}

/// Connect to some address.
pub async fn connect<A>(
    address: A,
    tcp_nodelay: bool,
    tcp_buffer_size: usize,
    connect_retries: usize,
) -> Result<Connection, Report>
where
    A: ToSocketAddrs + Clone + Debug,
{
    let mut tries = 0;
    loop {
        match TcpStream::connect(address.clone()).await {
            Ok(stream) => {
                let connection =
                    Connection::new(stream, tcp_nodelay, tcp_buffer_size);
                return Ok(connection);
            }
            Err(e) => {
                // if not, try again if we shouldn't give up (due to too many
                // attempts)
                tries += 1;
                if tries < connect_retries {
                    info!("failed to connect to {:?}: {}", address, e);
                    info!(
                        "will try again in 1 second ({} out of {})",
                        tries, connect_retries
                    );
                    tokio::time::sleep(Duration::from_secs(1)).await;
                } else {
                    return Err(e.into());
                }
            }
        }
    }
}

/// Listen on some address.
pub async fn listen<A>(address: A) -> Result<TcpListener, Report>
where
    A: ToSocketAddrs,
{
    Ok(TcpListener::bind(address).await?)
}

/// Listen on new connections and send them to parent process.
async fn listener_task(
    listener: TcpListener,
    tcp_nodelay: bool,
    tcp_buffer_size: usize,
    mut parent: ChannelSender<Connection>,
) {
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                trace!("[listener] new connection: {:?}", _addr);

                // create connection
                let connection =
                    Connection::new(stream, tcp_nodelay, tcp_buffer_size);

                if let Err(e) = parent.send(connection).await {
                    warn!("[listener] error sending stream to parent process: {:?}", e);
                }
            }
            Err(e) => {
                warn!("[listener] couldn't accept new connection: {:?}", e)
            }
        }
    }
}
