use crate::log;
use crate::run::prelude::*;
use crate::run::task;
use crate::run::task::chan::{ChannelReceiver, ChannelSender};
use std::fmt::Debug;

pub trait Index {
    fn index(&self) -> Option<usize>;
}

#[derive(Clone)]
pub struct ToPool<M> {
    name: String,
    pool: Vec<ChannelSender<M>>,
}

impl<M> ToPool<M>
where
    M: Debug + 'static,
{
    /// Creates a pool with size `pool_size`.
    pub fn new<S: Into<String>>(
        name: S,
        channel_buffer_size: usize,
        pool_size: usize,
    ) -> (Self, Vec<ChannelReceiver<M>>) {
        let mut pool = Vec::with_capacity(pool_size);
        // create a channel per pool worker:
        // - save the sender-side so it can be used by to forward messages to the pool
        // - return the receiver-side so it can be used by the pool workers
        let rxs = (0..pool_size)
            .map(|_| {
                let (tx, rx) = task::chan::channel(channel_buffer_size);
                pool.push(tx);
                rx
            })
            .collect();
        // create pool
        let to_pool = Self {
            name: name.into(),
            pool,
        };
        (to_pool, rxs)
    }

    /// Returns the size of the pool.
    pub fn pool_size(&self) -> usize {
        self.pool.len()
    }

    /// Forwards message `msg` to the pool worker with id `msg.index() % pool_size`.
    pub async fn forward(&mut self, msg: M) -> RunResult<()>
    where
        M: Index,
    {
        let index = msg.index();
        self.do_forward(index, msg).await
    }

    /// Forwards message `map(value)` to the pool worker with id `value.index() % pool_size`.
    pub async fn forward_map<V, F>(&mut self, value: V, map: F) -> RunResult<()>
    where
        V: Index,
        F: FnOnce(V) -> M,
    {
        let index = value.index();
        self.do_forward(index, map(value)).await
    }

    /// Forwards a message to the pool. Note that this implementation is not as efficient as it
    /// could be, as it's only meant to be used for setup/periodic messages.
    pub async fn broadcast(&mut self, msg: M) -> RunResult<()>
    where
        M: Clone,
    {
        for tx in self.pool.iter_mut() {
            tx.send(msg.clone()).await?;
        }
        Ok(())
    }

    async fn do_forward(&mut self, index: Option<usize>, msg: M) -> RunResult<()> {
        let index = match index {
            Some(index) => {
                // the actual index is computed based on the pool size
                index % self.pool.len()
            }
            None => {
                // in this case, there's a single pool handling all messages:
                // - check that the pool has size 1 and forward to the single worker
                assert_eq!(self.pool.len(), 1);
                1
            }
        };

        log!("index: {} {} of {}", self.name, index, self.pool_size());
        self.pool[index].send(msg).await
    }
}
