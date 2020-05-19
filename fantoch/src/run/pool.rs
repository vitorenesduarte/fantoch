use crate::log;
use crate::run::prelude::*;
use crate::run::task;
use crate::run::task::chan::{ChannelReceiver, ChannelSender};
use std::fmt::Debug;

pub trait PoolIndex {
    fn index(&self) -> Option<(usize, usize)>;
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
    pub fn new<S>(
        name: S,
        channel_buffer_size: usize,
        pool_size: usize,
    ) -> (Self, Vec<ChannelReceiver<M>>)
    where
        S: Into<String> + Debug,
    {
        let mut pool = Vec::with_capacity(pool_size);
        // create a channel per pool worker:
        // - save the sender-side so it can be used by to forward messages to
        //   the pool
        // - return the receiver-side so it can be used by the pool workers
        let rxs = (0..pool_size)
            .map(|index| {
                let (mut tx, rx) = task::chan::channel(channel_buffer_size);
                tx.set_name(format!("{:?}_{}", name, index));
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
        msg.index().map(|(reserved, index)| {
            Self::do_index(reserved, index, self.pool_size())
        })
    }

    fn do_index(reserved: usize, index: usize, pool_size: usize) -> usize {
        if reserved < pool_size {
            // compute the actual index only in the remaining indexes
            reserved + (index % (pool_size - reserved))
        } else {
            // if there's as many reserved (or more) than workers in the
            // pool, then ignore reservation
            index % pool_size
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn do_index(reserved: usize, index: usize, pool_size: usize) -> usize {
        ToPool::<()>::do_index(reserved, index, pool_size)
    }

    #[test]
    fn index() {
        let pool_size = 1;
        // if the pool size is 1, the remaining arguments are irrelevant
        assert_eq!(do_index(0, 0, pool_size), 0);
        assert_eq!(do_index(0, 1, pool_size), 0);
        assert_eq!(do_index(0, 2, pool_size), 0);
        assert_eq!(do_index(0, 3, pool_size), 0);
        assert_eq!(do_index(1, 0, pool_size), 0);
        assert_eq!(do_index(1, 1, pool_size), 0);
        assert_eq!(do_index(1, 2, pool_size), 0);
        assert_eq!(do_index(1, 3, pool_size), 0);
        assert_eq!(do_index(2, 0, pool_size), 0);
        assert_eq!(do_index(2, 1, pool_size), 0);
        assert_eq!(do_index(2, 2, pool_size), 0);
        assert_eq!(do_index(2, 3, pool_size), 0);
        assert_eq!(do_index(3, 0, pool_size), 0);
        assert_eq!(do_index(3, 1, pool_size), 0);
        assert_eq!(do_index(3, 2, pool_size), 0);
        assert_eq!(do_index(3, 3, pool_size), 0);

        let pool_size = 2;
        // if the pool size is 2, with reserved = 0, we simply %
        assert_eq!(do_index(0, 0, pool_size), 0);
        assert_eq!(do_index(0, 1, pool_size), 1);
        assert_eq!(do_index(0, 2, pool_size), 0);
        assert_eq!(do_index(0, 3, pool_size), 1);
        // if the pool size is 2, with reserved = 1, all requests go to 1
        assert_eq!(do_index(1, 0, pool_size), 1);
        assert_eq!(do_index(1, 1, pool_size), 1);
        assert_eq!(do_index(1, 2, pool_size), 1);
        assert_eq!(do_index(1, 3, pool_size), 1);
        // if the pool size is 2, with reserved >= 2, we simply %
        assert_eq!(do_index(2, 0, pool_size), 0);
        assert_eq!(do_index(2, 1, pool_size), 1);
        assert_eq!(do_index(2, 2, pool_size), 0);
        assert_eq!(do_index(2, 3, pool_size), 1);
        assert_eq!(do_index(3, 0, pool_size), 0);
        assert_eq!(do_index(3, 1, pool_size), 1);
        assert_eq!(do_index(3, 2, pool_size), 0);
        assert_eq!(do_index(3, 3, pool_size), 1);

        let pool_size = 3;
        // if the pool size is 3, with reserved = 0, we simply %
        assert_eq!(do_index(0, 0, pool_size), 0);
        assert_eq!(do_index(0, 1, pool_size), 1);
        assert_eq!(do_index(0, 2, pool_size), 2);
        assert_eq!(do_index(0, 3, pool_size), 0);
        // if the pool size is 3, with reserved = 1, we % between 1 and 2
        assert_eq!(do_index(1, 0, pool_size), 1);
        assert_eq!(do_index(1, 1, pool_size), 2);
        assert_eq!(do_index(1, 2, pool_size), 1);
        assert_eq!(do_index(1, 3, pool_size), 2);
        // if the pool size is 3, with reserved = 2, all requests go to 2
        assert_eq!(do_index(2, 0, pool_size), 2);
        assert_eq!(do_index(2, 1, pool_size), 2);
        assert_eq!(do_index(2, 2, pool_size), 2);
        assert_eq!(do_index(2, 3, pool_size), 2);
        // if the pool size is 3, with reserved >= 3, we simply %
        assert_eq!(do_index(3, 0, pool_size), 0);
        assert_eq!(do_index(3, 1, pool_size), 1);
        assert_eq!(do_index(3, 2, pool_size), 2);
        assert_eq!(do_index(3, 3, pool_size), 0);
        assert_eq!(do_index(4, 0, pool_size), 0);
        assert_eq!(do_index(4, 1, pool_size), 1);
        assert_eq!(do_index(4, 2, pool_size), 2);
        assert_eq!(do_index(4, 3, pool_size), 0);
    }
}
