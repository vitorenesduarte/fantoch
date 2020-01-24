use super::task::chan::{ChannelReceiver, ChannelSender};
use crate::command::{Command, CommandResult};
use crate::executor::Executor;
use crate::id::{ClientId, ProcessId};
use crate::protocol::Protocol;
use serde::{Deserialize, Serialize};
use std::error::Error;

// common error type
pub type RunResult<V> = Result<V, Box<dyn Error>>;

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
pub type ReaderReceiver<P> = ChannelReceiver<(ProcessId, <P as Protocol>::Message)>;
pub type ReaderSender<P> = ChannelSender<(ProcessId, <P as Protocol>::Message)>;
pub type WriterReceiver<P> = ChannelReceiver<<P as Protocol>::Message>;
pub type WriterSender<P> = ChannelSender<<P as Protocol>::Message>;
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
