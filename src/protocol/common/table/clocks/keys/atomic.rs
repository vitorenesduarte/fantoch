use super::KeyClocks;
use crate::command::Command;
use crate::id::ProcessId;
use crate::protocol::common::table::ProcessVotes;

#[derive(Clone)]
pub struct AtomicKeyClocks {}

impl KeyClocks for AtomicKeyClocks {
    /// Create a new `AtomicKeyClocks` instance.
    fn new(_id: ProcessId, _key_buckets_power: usize) -> Self {
        todo!()
    }

    fn bump_and_vote(
        &mut self,
        _cmd: &Command,
        _min_clock: u64,
    ) -> (u64, ProcessVotes) {
        todo!()
    }

    fn vote(&mut self, _cmd: &Command, _clock: u64) -> ProcessVotes {
        todo!()
    }
}
