use std::future::Future;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

/// Just a wrapper around tokio::spawn.
pub fn spawn<F>(task: F)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(task);
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
    let (tx, rx) = mpsc::unbounded_channel();
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
    let (tx, rx) = mpsc::unbounded_channel();
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
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(receiver(rx));
    tx
}
