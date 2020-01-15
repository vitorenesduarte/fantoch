use std::error::Error;
use std::fmt::Debug;
use tokio::sync::mpsc::{self, Receiver, Sender};

const DEFAULT_CHANNEL_BUFFER_SIZE: usize = 10000;

#[derive(Debug)]
pub struct ChannelSender<M> {
    sender: Sender<M>,
}

#[derive(Debug)]
pub struct ChannelReceiver<M> {
    receiver: Receiver<M>,
}

pub fn channel<M>() -> (ChannelSender<M>, ChannelReceiver<M>) {
    let (sender, receiver) = mpsc::channel(DEFAULT_CHANNEL_BUFFER_SIZE);
    (ChannelSender { sender }, ChannelReceiver { receiver })
}

impl<M> ChannelSender<M>
where
    M: Debug + 'static,
{
    pub async fn send(&mut self, value: M) -> Result<(), Box<dyn Error>> {
        // TODO try send first and show a log message if channel is full; only after, block on send
        self.sender.send(value).await.map_err(|err| err.into())
    }
}

impl<M> ChannelReceiver<M> {
    pub async fn recv(&mut self) -> Option<M> {
        self.receiver.recv().await
    }
}

impl<T> Clone for ChannelSender<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}
