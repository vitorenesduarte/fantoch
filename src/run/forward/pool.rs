use crate::run::prelude::*;
use crate::run::task;
use crate::run::task::chan::{ChannelReceiver, ChannelSender};
use std::fmt::Debug;

pub trait Index {
    fn index(&self) -> Option<usize>;
}

// #[derive(Clone)]
pub struct ToPool<M> {
    pool: Vec<ChannelSender<M>>,
}

impl<M> ToPool<M>
where
    M: Debug + 'static,
{
    /// Creates a pool with size `pool_size`.
    pub fn new(channel_buffer_size: usize, pool_size: usize) -> (Self, Vec<ChannelReceiver<M>>) {
        let mut pool = Vec::with_capacity(pool_size);
        // create a channel per pool worker:
        // - save the sender-side so it can be used by to forward messages to the pool
        // - return the receiver-side so it can be used by the pool workers
        let rxs = (0..pool_size)
            .map(|index| {
                let (tx, rx) = task::chan::channel(channel_buffer_size);
                pool[index] = tx;
                rx
            })
            .collect();
        (Self { pool }, rxs)
    }

    /// Forwards message `msg` to the pool worker with id `msg.index() % pool_size`.
    pub async fn forward(&mut self, msg: M) -> RunResult<()>
    where
        M: Index,
    {
        let index = msg.index();
        self.do_forward(index, msg).await
    }

    /// Forwards message `transform(value)` to the pool worker with id `value.index() % pool_size`.
    pub async fn forward_after<V, F>(&mut self, value: V, transform: F) -> RunResult<()>
    where
        V: Index,
        F: FnOnce(V) -> M,
    {
        let index = value.index();
        self.do_forward(index, transform(value)).await
    }

    async fn do_forward(&mut self, index: Option<usize>, msg: M) -> RunResult<()> {
        match index {
            Some(index) => {
                // the actual index is computed based on the pool size
                let index = index % self.pool.len();
                self.pool[index].send(msg).await
            }
            None => {
                // in this case, there's a single pool handling all messages;
                // TODO implement this once we have Paxos
                todo!()
            }
        }
    }
}

// TODO somehow, rustc can't derive this Clone impl; why?
impl<M> Clone for ToPool<M> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
        }
    }
}
