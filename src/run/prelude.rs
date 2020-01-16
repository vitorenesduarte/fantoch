use super::task::chan::{ChannelReceiver, ChannelSender};
use crate::command::{Command, CommandResult};
use crate::executor::Executor;
use crate::id::{ClientId, ProcessId};
use crate::protocol::{Protocol, ToSend};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

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
pub type ReaderReceiver<V> = ChannelReceiver<(ProcessId, V)>;
pub type ReaderSender<V> = ChannelSender<(ProcessId, V)>;
pub type BroadcastWriterReceiver<V> = ChannelReceiver<ToSend<V>>;
pub type BroadcastWriterSender<V> = ChannelSender<ToSend<V>>;
pub type WriterReceiver = ChannelReceiver<Bytes>;
pub type WriterSender = ChannelSender<Bytes>;
pub type ClientReceiver = ChannelReceiver<FromClient>;
pub type ClientSender = ChannelSender<FromClient>;
pub type CommandReceiver = ChannelReceiver<Command>;
pub type CommandSender = ChannelSender<Command>;
pub type CommandResultReceiver = ChannelReceiver<CommandResult>;
pub type CommandResultSender = ChannelSender<CommandResult>;
pub type ExecutionInfoReceiver<P> =
    ChannelReceiver<Vec<<<P as Protocol>::Executor as Executor>::ExecutionInfo>>;
pub type ExecutionInfoSender<P> =
    ChannelSender<Vec<<<P as Protocol>::Executor as Executor>::ExecutionInfo>>;
