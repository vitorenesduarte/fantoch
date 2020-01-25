// This module contains the definition of `ToWorkers`.
mod workers;

// This module contains the definition of `ToExecutors`.
mod executors;

// Re-exports.
pub use workers::ToWorkers;
// pub use executors::ToExecutors;

use crate::command::Command;
use crate::id::{Dot, ProcessId};
use crate::protocol::Protocol;

pub trait MessageDot {
    /// If `None` is returned, then the message is sent to all protocol processes.
    /// In particular, if the protocol is not parallel, the message is sent to the single protocol
    /// process.
    fn dot(&self) -> Option<&Dot> {
        None
    }
}

pub type ClientToWorkers = ToWorkers<(Dot, Command)>;
impl MessageDot for (Dot, Command) {
    fn dot(&self) -> Option<&Dot> {
        Some(&self.0)
    }
}

pub type ReaderToWorkers<P> = ToWorkers<(ProcessId, <P as Protocol>::Message)>;
// The following allows e.g. (ProcessId, <P as Protocol>::Message) to be forwarded
impl<A, B> MessageDot for (A, B)
where
    B: MessageDot,
{
    fn dot(&self) -> Option<&Dot> {
        self.1.dot()
    }
}
