use crate::run::prelude::*;
use std::fmt::Debug;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{self, Receiver, Sender};

#[derive(Debug)]
pub struct ChannelSender<M> {
    name: Option<String>,
    sender: Sender<M>,
}

#[derive(Debug)]
pub struct ChannelReceiver<M> {
    receiver: Receiver<M>,
}

pub fn channel<M>(
    buffer_size: usize,
) -> (ChannelSender<M>, ChannelReceiver<M>) {
    let (sender, receiver) = mpsc::channel(buffer_size);
    (
        ChannelSender { name: None, sender },
        ChannelReceiver { receiver },
    )
}

impl<M> ChannelSender<M>
where
    M: Debug + 'static,
{
    pub fn set_name<S: Into<String>>(&mut self, name: S) {
        self.name = Some(name.into());
    }

    pub async fn send(&mut self, value: M) -> RunResult<()> {
        match self.sender.try_send(value) {
            Ok(()) => {
                // if it was sent, we're done
                Ok(())
            }
            Err(TrySendError::Full(value)) => {
                // if it's full, use `send` and `await` on it
                match &self.name {
                    Some(name) => println!("named channel {} is full", name),
                    None => println!("unnamed channel is full"),
                }
                self.sender.send(value).await.map_err(|err| err.into())
            }
            Err(e) => {
                // otherwise, upstream the error
                Err(e.into())
            }
        }
    }
}

impl<M> ChannelSender<M> {
    pub async fn blind_send(&mut self, value: M) {
        let res = self.sender.send(value).await;
        assert!(res.is_ok(), "blind_send should succeeed");
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
            name: self.name.clone(),
            sender: self.sender.clone(),
        }
    }
}
