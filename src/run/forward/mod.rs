// This module contains the definition of `ToPool`.
mod pool;

use crate::command::Command;
use crate::executor::MessageKey;
use crate::id::{Dot, ProcessId, Rifl};
use crate::kvs::Key;
use crate::protocol::MessageDot;
use crate::protocol::Protocol;
use std::hash::{Hash, Hasher};

// workers receive messages from:
// - clients
// - readers
pub type ClientToWorkers = pool::ToPool<(Dot, Command)>;

impl pool::Index for (Dot, Command) {
    fn index(&self) -> Option<usize> {
        Some(dot_index(&self.0))
    }
}

pub type ReaderToWorkers<P> = pool::ToPool<(ProcessId, <P as Protocol>::Message)>;
// The following allows e.g. (ProcessId, <P as Protocol>::Message) to be forwarded
impl<A, B> pool::Index for (A, B)
where
    B: MessageDot,
{
    fn index(&self) -> Option<usize> {
        self.1.dot().map(|dot| dot_index(dot))
    }
}

// executors receive messages from:
// - clients
// - workers
pub type ClientToExecutors = pool::ToPool<(Rifl, usize)>;

// The index of a dot is its sequence
fn dot_index(dot: &Dot) -> usize {
    dot.sequence() as usize
}

type DefaultHasher = ahash::AHasher;

// The index of a key is its hash
fn key_index(key: &Key) -> usize {
    let mut hasher = DefaultHasher::default();
    key.hash(&mut hasher);
    hasher.finish() as usize
}
