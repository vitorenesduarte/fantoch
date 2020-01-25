use super::MessageDot;
use crate::run::prelude::*;
use crate::run::task;
use crate::run::task::chan::{ChannelReceiver, ChannelSender};
use std::fmt::Debug;

pub struct ToWorkers<M> {
    workers: Vec<ChannelSender<M>>,
}

impl<M> ToWorkers<M>
where
    M: MessageDot + Debug + 'static,
{
    pub fn new(
        channel_buffer_size: usize,
        worker_number: usize,
    ) -> (Self, Vec<ChannelReceiver<M>>) {
        let mut workers = Vec::with_capacity(worker_number);
        // create a channel per worker:
        // - save the sender-side so it can be used by writer tasks
        // - return the receiver-side so it can be used by workers
        let rxs = (0..worker_number)
            .map(|index| {
                let (tx, rx) = task::chan::channel(channel_buffer_size);
                workers[index] = tx;
                rx
            })
            .collect();
        (Self { workers }, rxs)
    }

    pub async fn forward(&mut self, msg: M) -> RunResult<()> {
        match msg.dot() {
            Some(dot) => {
                // find dot's sequence and mod it with the number of workers
                let index = (dot.sequence() as usize) % self.workers.len();
                self.workers[index].send(msg).await
            }
            None => {
                // TODO in this case, there's a single process handling all protocol messages;
                // implement this once we have Paxos
                todo!()
            }
        }
    }
}

// TODO somehow, rustc can't derive this Clone impl; why?
impl<M> Clone for ToWorkers<M> {
    fn clone(&self) -> Self {
        Self {
            workers: self.workers.clone(),
        }
    }
}
