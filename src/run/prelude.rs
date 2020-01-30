use super::pool;
use super::task::chan::{ChannelReceiver, ChannelSender};
use crate::command::{Command, CommandResult};
use crate::executor::{Executor, ExecutorResult, MessageKey};
use crate::id::{ClientId, Dot, ProcessId, Rifl};
use crate::kvs::Key;
use crate::protocol::{MessageDot, Protocol};
use crate::util;
use serde::{Deserialize, Serialize};
use std::error::Error;

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
pub type ClientReceiver = ChannelReceiver<FromClient>;
pub type CommandReceiver = ChannelReceiver<Command>;
pub type CommandSender = ChannelSender<Command>;
pub type CommandResultReceiver = ChannelReceiver<CommandResult>;
pub type CommandResultSender = ChannelSender<CommandResult>;
pub type ExecutorResultReceiver = ChannelReceiver<ExecutorResult>;
pub type ExecutorResultSender = ChannelSender<ExecutorResult>;
pub type SubmitReceiver = ChannelReceiver<(Dot, Command)>;
pub type ExecutionInfoReceiver<P> =
    ChannelReceiver<<<P as Protocol>::Executor as Executor>::ExecutionInfo>;

// 1. workers receive messages from clients
pub type ClientToWorkers = pool::ToPool<(Dot, Command)>;
impl pool::PoolIndex for (Dot, Command) {
    fn index(&self) -> Option<usize> {
        Some(dot_index(&self.0))
    }
}

// 2. workers receive messages from readers
pub type ReaderToWorkers<P> =
    pool::ToPool<(ProcessId, <P as Protocol>::Message)>;
// The following allows e.g. (ProcessId, <P as Protocol>::Message) to be
// `ToPool::forward`
impl<A, B> pool::PoolIndex for (A, B)
where
    B: MessageDot,
{
    fn index(&self) -> Option<usize> {
        self.1.dot().map(|dot| dot_index(dot))
    }
}

// 3. executors receive messages from clients
pub type ClientToExecutors = pool::ToPool<FromClient>;
// The following allows e.g. (&Key, Rifl) to be `ToPool::forward_after`
impl pool::PoolIndex for (&Key, Rifl) {
    fn index(&self) -> Option<usize> {
        Some(key_index(&self.0))
    }
}

// 4. executors receive messages from workers
pub type WorkerToExecutors<P> =
    pool::ToPool<<<P as Protocol>::Executor as Executor>::ExecutionInfo>;
// The following allows <<P as Protocol>::Executor as Executor>::ExecutionInfo
// to be forwarded
impl<A> pool::PoolIndex for A
where
    A: MessageKey,
{
    fn index(&self) -> Option<usize> {
        self.key().map(|key| key_index(key))
    }
}

// The index of a dot is its sequence
fn dot_index(dot: &Dot) -> usize {
    dot.sequence() as usize
}

// The index of a key is its hash
fn key_index(key: &Key) -> usize {
    util::key_hash(key) as usize
}
