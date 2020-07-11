use super::pool;
use super::task::chan::{ChannelReceiver, ChannelSender};
use crate::command::{Command, CommandResult};
use crate::executor::{Executor, ExecutorMetrics, ExecutorResult, MessageKey};
use crate::id::{ClientId, Dot, ProcessId, Rifl};
use crate::kvs::Key;
use crate::protocol::{
    MessageIndex, PeriodicEventIndex, Protocol, ProtocolMetrics,
};
use crate::util;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;
use std::sync::Arc;

// the worker index that should be used by leader-based protocols
pub const LEADER_WORKER_INDEX: usize = 0;

// the worker index that should be for garbage collection:
// - it's okay to be the same as the leader index because this value is not used
//   by leader-based protocols
// - e.g. in fpaxos, the gc only runs in the acceptor worker
pub const GC_WORKER_INDEX: usize = 0;

pub const INDEXES_RESERVED: usize = 2;

pub fn worker_index_no_shift(index: usize) -> Option<(usize, usize)> {
    // when there's no shift, the index must be either 0 or 1
    assert!(index < INDEXES_RESERVED);
    Some((0, index))
}

// note: reserved indexing always reserve the first two workers
pub const fn worker_index_shift(index: usize) -> Option<(usize, usize)> {
    Some((INDEXES_RESERVED, index))
}

pub fn worker_dot_index_shift(dot: &Dot) -> Option<(usize, usize)> {
    worker_index_shift(dot.sequence() as usize)
}

// common error type
pub type RunResult<V> = Result<V, Box<dyn Error>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct ProcessHi(pub ProcessId);
#[derive(Debug, Serialize, Deserialize)]
pub struct ClientHi(pub Vec<ClientId>);

#[derive(Debug, Clone)]
pub enum FromClient {
    // clients can register
    Register(Vec<ClientId>, RiflAckSender, ExecutorResultSender),
    // unregister
    Unregister(Vec<ClientId>),
    // register for notifications on a partial about this `Rifl`.
    WaitForRifl(Rifl),
}

// list of channels used to communicate between tasks
pub type RiflAckReceiver = ChannelReceiver<Rifl>;
pub type RiflAckSender = ChannelSender<Rifl>;
pub type ReaderReceiver<P> =
    ChannelReceiver<(ProcessId, <P as Protocol>::Message)>;
pub type WriterReceiver<P> = ChannelReceiver<Arc<<P as Protocol>::Message>>;
pub type WriterSender<P> = ChannelSender<Arc<<P as Protocol>::Message>>;
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
pub type PeriodicEventReceiver<P, R> =
    ChannelReceiver<FromPeriodicMessage<P, R>>;
pub type InspectFun<P, R> = (fn(&P) -> R, ChannelSender<R>);
pub type InspectReceiver<P, R> = ChannelReceiver<InspectFun<P, R>>;
pub type SortedProcessesSender = ChannelSender<ChannelSender<Vec<ProcessId>>>;
pub type SortedProcessesReceiver =
    ChannelReceiver<ChannelSender<Vec<ProcessId>>>;
pub type ProtocolMetricsReceiver = ChannelReceiver<(usize, ProtocolMetrics)>;
pub type ProtocolMetricsSender = ChannelSender<(usize, ProtocolMetrics)>;
pub type ExecutorMetricsReceiver = ChannelReceiver<(usize, ExecutorMetrics)>;
pub type ExecutorMetricsSender = ChannelSender<(usize, ExecutorMetrics)>;

// 1. workers receive messages from clients
pub type ClientToWorkers = pool::ToPool<(Option<Dot>, Command)>;
impl pool::PoolIndex for (Option<Dot>, Command) {
    fn index(&self) -> Option<(usize, usize)> {
        // if there's a `Dot`, then the protocol is leaderless; otherwise, it is
        // leader-based and the command should always be forwarded to the leader
        // worker
        self.0
            .as_ref()
            .map(worker_dot_index_shift)
            // no necessary reserve if there's a leader
            .unwrap_or_else(|| worker_index_no_shift(LEADER_WORKER_INDEX))
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
// - this message can either be a periodic event or
// - an inspect function that takes a reference to the protocol state and
//   returns a boolean; this boolean is then sent through the `ChannelSender`
//   (this is useful for e.g. testing)
#[derive(Clone)]
pub enum FromPeriodicMessage<P: Protocol, R> {
    Event(P::PeriodicEvent),
    Inspect(fn(&P) -> R, ChannelSender<R>),
}

impl<P, R> fmt::Debug for FromPeriodicMessage<P, R>
where
    P: Protocol,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Event(e) => write!(f, "FromPeriodicMessage::Event({:?})", e),
            Self::Inspect(_, _) => write!(f, "FromPeriodicMessage::Inspect"),
        }
    }
}

pub type PeriodicToWorkers<P, R> = pool::ToPool<FromPeriodicMessage<P, R>>;
// The following allows e.g. <P as Protocol>::Periodic to be `ToPool::forward`
impl<P, R> pool::PoolIndex for FromPeriodicMessage<P, R>
where
    P: Protocol,
{
    fn index(&self) -> Option<(usize, usize)> {
        match self {
            Self::Event(e) => e.index(),
            Self::Inspect(_, _) => None, // send to all
        }
    }
}

// 4. executors receive messages from clients
pub type ClientToExecutors = pool::ToPool<FromClient>;
// The following allows e.g. (&Key, Rifl) to be `ToPool::forward_after`
impl pool::PoolIndex for (&Key, Rifl) {
    fn index(&self) -> Option<(usize, usize)> {
        Some(key_index(&self.0))
    }
}

// 5. executors receive messages from workers
pub type WorkerToExecutors<P> =
    pool::ToPool<<<P as Protocol>::Executor as Executor>::ExecutionInfo>;
// The following allows <<P as Protocol>::Executor as Executor>::ExecutionInfo
// to be forwarded
impl<A> pool::PoolIndex for A
where
    A: MessageKey,
{
    fn index(&self) -> Option<(usize, usize)> {
        self.key().map(key_index)
    }
}

// The index of a key is its hash
#[allow(clippy::ptr_arg)]
fn key_index(key: &Key) -> (usize, usize) {
    let index = util::key_hash(key) as usize;
    (0, index)
}
