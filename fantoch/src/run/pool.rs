use crate::log;
use crate::run::prelude::*;
use crate::run::task;
use crate::run::task::chan::{ChannelReceiver, ChannelSender};
use std::fmt::Debug;

pub trait PoolIndex {
    fn index(&self) -> Option<usize>;
}

#[derive(Clone)]
pub struct ToPool<M> {
    name: String,
    pool: Vec<ChannelSender<M>>,
}

impl<M> ToPool<M>
where
    M: Clone + Debug + 'static,
{
    /// Creates a pool with size `pool_size`.
    pub fn new<S: Into<String>>(
        name: S,
        channel_buffer_size: usize,
        pool_size: usize,
    ) -> (Self, Vec<ChannelReceiver<M>>) {
        let mut pool = Vec::with_capacity(pool_size);
        // create a channel per pool worker:
        // - save the sender-side so it can be used by to forward messages to
        //   the pool
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

    /// Checks the index of the destination worker.
    pub fn only_to_self(&self, msg: &M, worker_index: usize) -> bool
    where
        M: PoolIndex,
    {
        match self.index(msg) {
            Some(index) => index == worker_index,
            None => false,
        }
    }

    /// Forwards message `msg` to the pool worker with id `msg.index() %
    /// pool_size`.
    pub async fn forward(&mut self, msg: M) -> RunResult<()>
    where
        M: PoolIndex,
    {
        let index = self.index(&msg);
        self.do_forward(index, msg).await
    }

    /// Forwards message `map(value)` to the pool worker with id `value.index()
    /// % pool_size`.
    pub async fn forward_map<V, F>(&mut self, value: V, map: F) -> RunResult<()>
    where
        V: PoolIndex,
        F: FnOnce(V) -> M,
    {
        let index = self.index(&value);
        self.do_forward(index, map(value)).await
    }

    /// Forwards a message to the pool. Note that this implementation is not as
    /// efficient as it could be, as it's only meant to be used for
    /// setup/periodic messages.
    pub async fn broadcast(&mut self, msg: M) -> RunResult<()>
    where
        M: Clone,
    {
        for tx in self.pool.iter_mut() {
            tx.send(msg.clone()).await?;
        }
        Ok(())
    }

    fn index<T>(&self, msg: &T) -> Option<usize>
    where
        T: PoolIndex,
    {
        msg.index().map(|index| {
            // the actual index is computed based on the pool size
            index % self.pool.len()
        })
    }

    async fn do_forward(
        &mut self,
        index: Option<usize>,
        msg: M,
    ) -> RunResult<()> {
        log!("index: {} {:?} of {}", self.name, index, self.pool_size());
        // send to the correct worker if an index was specified. otherwise, send
        // to all workers.
        match index {
            Some(index) => self.pool[index].send(msg).await,
            None => {
                // TODO send this in parallel
                for index in 0..self.pool.len() {
                    self.pool[index].send(msg.clone()).await?
                }
                Ok(())
            }
        }
    }
}
