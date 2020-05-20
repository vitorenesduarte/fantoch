use super::pool;
use super::task::chan::{ChannelReceiver, ChannelSender};
use crate::command::{Command, CommandResult};
use crate::executor::{Executor, ExecutorResult, MessageKey};
use crate::id::{ClientId, Dot, ProcessId, Rifl};
use crate::kvs::Key;
use crate::protocol::{MessageIndex, PeriodicEventIndex, Protocol};
use crate::util;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;

// the worker index that should be used by leader-based protocols
pub const LEADER_WORKER_INDEX: usize = 0;

// the worker index that should be for garbage collection:
// - it's okay to be the same as the leader index because this value is not used
//   by leader-based protocols
// - e.g. in fpaxos, the gc only runs in the acceptor worker
pub const GC_WORKER_INDEX: usize = 0;

// protocols that use dots have a GC worker; this reserves the first worker for
// that
pub fn dot_worker_index_reserve(dot: &Dot) -> Option<(usize, usize)> {
    worker_index_reserve(1, dot.sequence() as usize)
}

pub fn no_worker_index_reserve(index: usize) -> Option<(usize, usize)> {
    worker_index_reserve(0, index)
}

pub fn worker_index_reserve(
    reserve: usize,
    index: usize,
) -> Option<(usize, usize)> {
    Some((reserve, index))
}

// common error type
pub type RunResult<V> = Result<V, Box<dyn Error>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct ProcessHi(pub ProcessId);
#[derive(Debug, Serialize, Deserialize)]
pub struct ClientHi(pub ClientId);

#[derive(Debug, Clone)]
pub enum FromClient {
    // clients can register
    Register(ClientId, RiflAckSender, ExecutorResultSender),
    // unregister
    Unregister(ClientId),
    // register for notifications on a partial about this `Rifl`.
    WaitForRifl(Rifl),
}

// list of channels used to communicate between tasks
pub type RiflAckReceiver = ChannelReceiver<Rifl>;
pub type RiflAckSender = ChannelSender<Rifl>;
pub type ReaderReceiver<P> =
    ChannelReceiver<(ProcessId, <P as Protocol>::Message)>;
pub type WriterReceiver<P> = ChannelReceiver<<P as Protocol>::Message>;
pub type WriterSender<P> = ChannelSender<<P as Protocol>::Message>;
pub type PeriodicEventReceiver<P> = ChannelReceiver<PeriodicEventMessage<P>>;
pub type ClientReceiver = ChannelReceiver<FromClient>;
pub type CommandReceiver = ChannelReceiver<Command>;
pub type CommandSender = ChannelSender<Command>;
pub type CommandResultReceiver = ChannelReceiver<CommandResult>;
pub type CommandResultSender = ChannelSender<CommandResult>;
pub type ExecutorResultReceiver = ChannelReceiver<ExecutorResult>;
pub type ExecutorResultSender = ChannelSender<ExecutorResult>;
pub type SubmitReceiver = ChannelReceiver<(Option<Dot>, Command)>;
pub type ExecutionInfoReceiver<P> =
    ChannelReceiver<<<P as Protocol>::Executor as Executor>::ExecutionInfo>;
pub type ExecutionInfoSender<P> =
    ChannelSender<<<P as Protocol>::Executor as Executor>::ExecutionInfo>;

// 1. workers receive messages from clients
pub type ClientToWorkers = pool::ToPool<(Option<Dot>, Command)>;
impl pool::PoolIndex for (Option<Dot>, Command) {
    fn index(&self) -> Option<(usize, usize)> {
        // if there's a `Dot`, then the protocol is leaderless; otherwise, it is
        // leader-based and the command should always be forwarded to the leader
        // worker
        self.0
            .as_ref()
            .map(dot_worker_index_reserve)
            // no necessary reserve if there's a leader
            .unwrap_or(no_worker_index_reserve(LEADER_WORKER_INDEX))
    }
}

// 2. workers receive messages from readers
pub type ReaderToWorkers<P> =
    pool::ToPool<(ProcessId, <P as Protocol>::Message)>;
// The following allows e.g. (ProcessId, <P as Protocol>::Message) to be
// `ToPool::forward`
impl<A> pool::PoolIndex for (ProcessId, A)
where
    A: MessageIndex,
{
    fn index(&self) -> Option<(usize, usize)> {
        self.1.index()
    }
}

// 3. workers receive messages from the periodic-events task
// - this wrapper is here only to be able to implement this trait
// - otherwise the compiler can't figure out between
//  > A: PeriodicEventIndex
//  > A: MessageKey
#[derive(Clone)]
pub struct PeriodicEventMessage<P: Protocol>(pub P::PeriodicEvent);
impl<P> fmt::Debug for PeriodicEventMessage<P>
where
    P: Protocol,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

pub type PeriodicToWorkers<P> = pool::ToPool<PeriodicEventMessage<P>>;
// The following allows e.g. <P as Protocol>::Periodic to be `ToPool::forward`
impl<P> pool::PoolIndex for PeriodicEventMessage<P>
where
    P: Protocol,
{
    fn index(&self) -> Option<(usize, usize)> {
        self.0.index()
    }
}

// 4. executors receive messages from clients
pub type ClientToExecutors = pool::ToPool<FromClient>;
// The following allows e.g. (&Key, Rifl) to be `ToPool::forward_after`
impl pool::PoolIndex for (&Key, Rifl) {
    fn index(&self) -> Option<(usize, usize)> {
        no_worker_index_reserve(key_index(&self.0))
    }
}

// 5. executors receive messages from workers
pub type WorkerToExecutors<P> =
    pool::ToPool<<<P as Protocol>::Executor as Executor>::ExecutionInfo>;
// The following allows <<P as Protocol>::Executor as Executor>::ExecutionInfo
// to be forwarded
impl<A> pool::PoolIndex for A
where
    A: MessageKey + std::fmt::Debug,
{
    fn index(&self) -> Option<(usize, usize)> {
        match self.key() {
            Some(key) => no_worker_index_reserve(key_index(key)),
            None => None,
        }
    }
}

// The index of a key is its hash
#[allow(clippy::ptr_arg)]
fn key_index(key: &Key) -> usize {
    util::key_hash(key) as usize
}
