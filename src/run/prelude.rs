use crate::command::{Command, CommandResult};
use crate::executor::Executor;
use crate::id::{ClientId, ProcessId};
use crate::protocol::{Protocol, ToSend};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

#[derive(Debug, Serialize, Deserialize)]
pub struct ProcessHi(pub ProcessId);
#[derive(Debug, Serialize, Deserialize)]
pub struct ClientHi(pub ClientId);

#[derive(Debug)]
pub enum FromClient {
    // clients can register
    Register(ClientId, CommandResultSender),
    // unregister
    Unregister(ClientId),
    // or submit new commands
    Submit(Command),
}

// list of channels used to communicate between tasks
pub type ReaderReceiver<V> = UnboundedReceiver<(ProcessId, V)>;
pub type ReaderSender<V> = UnboundedSender<(ProcessId, V)>;
pub type BroadcastWriterReceiver<V> = UnboundedReceiver<ToSend<V>>;
pub type BroadcastWriterSender<V> = UnboundedSender<ToSend<V>>;
pub type WriterReceiver = UnboundedReceiver<Bytes>;
pub type WriterSender = UnboundedSender<Bytes>;
pub type ClientReceiver = UnboundedReceiver<FromClient>;
pub type ClientSender = UnboundedSender<FromClient>;
pub type CommandReceiver = UnboundedReceiver<Command>;
pub type CommandSender = UnboundedSender<Command>;
pub type CommandResultReceiver = UnboundedReceiver<CommandResult>;
pub type CommandResultSender = UnboundedSender<CommandResult>;
pub type ExecutionInfoReceiver<P> =
    UnboundedReceiver<Vec<<<P as Protocol>::Executor as Executor>::ExecutionInfo>>;
pub type ExecutionInfoSender<P> =
    UnboundedSender<Vec<<<P as Protocol>::Executor as Executor>::ExecutionInfo>>;
