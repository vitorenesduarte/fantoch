use crate::id::ProcessId;
use crate::protocol::{MessageDot, Protocol};
use crate::run::prelude::*;
use crate::run::task;

pub struct ToWorkers<P: Protocol> {
    workers: Vec<ReaderSender<P>>,
}

impl<P> ToWorkers<P>
where
    P: Protocol + 'static,
{
    pub fn new(channel_buffer_size: usize, worker_number: usize) -> (Self, Vec<ReaderReceiver<P>>) {
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

    pub async fn forward(&mut self, from: ProcessId, msg: P::Message) -> RunResult<()> {
        match msg.dot() {
            Some(dot) => {
                // find dot's sequence and mod it with the number of workers
                let index = (dot.sequence() as usize) % self.workers.len();
                self.workers[index].send((from, msg)).await
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
impl<P> Clone for ToWorkers<P>
where
    P: Protocol,
{
    fn clone(&self) -> Self {
        Self {
            workers: self.workers.clone(),
        }
    }
}
