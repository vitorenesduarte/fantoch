use super::chan::{ChannelReceiver, ChannelSender};
use super::pool;
use crate::command::{Command, CommandResult};
use crate::executor::{Executor, ExecutorMetrics, ExecutorResult};
use crate::id::{ClientId, Dot, ProcessId, ShardId};
use crate::load_balance::*;
use crate::protocol::{Executed, MessageIndex, Protocol, ProtocolMetrics};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize)]
pub struct ProcessHi {
    pub process_id: ProcessId,
    pub shard_id: ShardId,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClientHi(pub Vec<ClientId>);

// If the command touches a single shard, then a `Submit` will be sent to that
// shard. If the command touches more than on shard, a `Submit` will be sent to
// one targetted shard and a `Register` will be sent to the remaining shards to
// make sure that the client will eventually receive a `CommandResult` from all
// shards.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientToServer {
    Submit(Command),
    Register(Command),
}

#[derive(Debug, Clone)]
pub enum ClientToExecutor {
    // clients can register
    Register(Vec<ClientId>, ExecutorResultSender),
    // unregister
    Unregister(Vec<ClientId>),
}

#[derive(Debug, Serialize, Deserialize)]
// these bounds are explained here: https://github.com/serde-rs/serde/issues/1503#issuecomment-475059482
#[serde(bound(
    serialize = "P::Message: Serialize",
    deserialize = "P::Message: Deserialize<'de>",
))]
pub enum POEMessage<P: Protocol> {
    Protocol(<P as Protocol>::Message),
    Executor(<<P as Protocol>::Executor as Executor>::ExecutionInfo),
}

impl<P: Protocol> POEMessage<P> {
    pub fn to_executor(&self) -> bool {
        match self {
            Self::Protocol(_) => false,
            Self::Executor(_) => true,
        }
    }
}

// list of channels used to communicate between tasks
// TODO: remove most of these as it doesn't really help reading the code
pub type ReaderReceiver<P> =
    ChannelReceiver<(ProcessId, ShardId, <P as Protocol>::Message)>;
pub type WriterReceiver<P> = ChannelReceiver<Arc<POEMessage<P>>>;
pub type WriterSender<P> = ChannelSender<Arc<POEMessage<P>>>;
pub type ClientToExecutorReceiver = ChannelReceiver<ClientToExecutor>;
pub type ClientToServerReceiver = ChannelReceiver<ClientToServer>;
pub type ClientToServerSender = ChannelSender<ClientToServer>;
pub type ServerToClientReceiver = ChannelReceiver<CommandResult>;
pub type ServerToClientSender = ChannelSender<CommandResult>;
pub type ExecutorResultReceiver = ChannelReceiver<ExecutorResult>;
pub type ExecutorResultSender = ChannelSender<ExecutorResult>;
pub type ExecutedReceiver = ChannelReceiver<Executed>;
pub type SubmitReceiver = ChannelReceiver<(Option<Dot>, Command)>;
pub type ExecutionInfoReceiver<P> =
    ChannelReceiver<<<P as Protocol>::Executor as Executor>::ExecutionInfo>;
pub type ExecutionInfoSender<P> =
    ChannelSender<<<P as Protocol>::Executor as Executor>::ExecutionInfo>;
pub type PeriodicEventReceiver<P, R> =
    ChannelReceiver<FromPeriodicMessage<P, R>>;
pub type InspectFun<P, R> = (fn(&P) -> R, ChannelSender<R>);
pub type InspectReceiver<P, R> = ChannelReceiver<InspectFun<P, R>>;
pub type SortedProcessesSender =
    ChannelSender<ChannelSender<Vec<(ProcessId, ShardId)>>>;
pub type SortedProcessesReceiver =
    ChannelReceiver<ChannelSender<Vec<(ProcessId, ShardId)>>>;
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
    pool::ToPool<(ProcessId, ShardId, <P as Protocol>::Message)>;
// The following allows e.g. (ProcessId, ShardId, <P as Protocol>::Message) to
// be `ToPool::forward`
impl<A> pool::PoolIndex for (ProcessId, ShardId, A)
where
    A: MessageIndex,
{
    fn index(&self) -> Option<(usize, usize)> {
        self.2.index()
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
            Self::Event(e) => MessageIndex::index(e),
            Self::Inspect(_, _) => None, // send to all
        }
    }
}

// 4. the worker `GC_WORKER_INDEX` receives executed notification messages from
// executors
pub type ExecutorsToWorkers = pool::ToPool<Executed>;
impl pool::PoolIndex for Executed {
    fn index(&self) -> Option<(usize, usize)> {
        worker_index_no_shift(GC_WORKER_INDEX)
    }
}

// 5. executors receive messages from clients
pub type ClientToExecutors = pool::ToPool<ClientToExecutor>;

// 6. executors receive messages from workers and reader tasks
pub type ToExecutors<P> =
    pool::ToPool<<<P as Protocol>::Executor as Executor>::ExecutionInfo>;
// The following allows <<P as Protocol>::Executor as Executor>::ExecutionInfo
// to be forwarded
impl<A> pool::PoolIndex for A
where
    A: MessageIndex,
{
    fn index(&self) -> Option<(usize, usize)> {
        self.index()
    }
}
