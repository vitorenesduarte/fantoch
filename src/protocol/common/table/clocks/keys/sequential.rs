use super::KeyClocks;
use crate::command::Command;
use crate::id::ProcessId;
use crate::kvs::Key;
use crate::protocol::common::table::{ProcessVotes, VoteRange};
use std::cmp;
use std::collections::HashMap;

#[derive(Clone)]
pub struct SequentialKeyClocks {
    id: ProcessId,
    clocks: HashMap<Key, u64>,
}

impl KeyClocks for SequentialKeyClocks {
    /// Create a new `SequentialKeyClocks` instance.
    fn new(id: ProcessId) -> Self {
        Self {
            id,
            clocks: HashMap::new(),
        }
    }

    fn bump_and_vote(
        &mut self,
        cmd: &Command,
        min_clock: u64,
    ) -> (u64, ProcessVotes) {
        // bump to at least `min_clock`
        let clock = cmp::max(min_clock, self.clock(cmd) + 1);

        // compute votes up to that clock
        let votes = self.vote(cmd, clock);

        // return both
        (clock, votes)
    }

    fn vote(&mut self, cmd: &Command, clock: u64) -> ProcessVotes {
        cmd.keys()
            .filter_map(|key| {
                // get a mutable reference to current clock value
                // TODO refactoring the following match block into a function
                // will not work due to limitations in the
                // borrow-checker
                // - see: https://rust-lang.github.io/rfcs/2094-nll.html#problem-case-3-conditional-control-flow-across-functions
                // - this is supposed to be fixed by polonius: http://smallcultfollowing.com/babysteps/blog/2018/06/15/mir-based-borrow-check-nll-status-update/#polonius
                let current = match self.clocks.get_mut(key) {
                    Some(value) => value,
                    None => self.clocks.entry(key.clone()).or_insert(0),
                };

                // if we should vote
                if *current < clock {
                    // vote from the current clock value + 1 until `clock`
                    let vr = VoteRange::new(self.id, *current + 1, clock);
                    // update current clock to be `clock`
                    *current = clock;
                    Some((key.clone(), vr))
                } else {
                    None
                }
            })
            .collect()
    }
}

impl SequentialKeyClocks {
    /// Retrieves the current clock for some command.
    /// If the command touches multiple keys, returns the maximum between the
    /// clocks associated with each key.
    fn clock(&self, cmd: &Command) -> u64 {
        cmd.keys()
            .map(|key| self.key_clock(key))
            .max()
            .expect("there must be at least one key in the command")
    }

    /// Retrieves the current clock for `key`.
    #[allow(clippy::ptr_arg)]
    fn key_clock(&self, key: &Key) -> u64 {
        self.clocks.get(key).cloned().unwrap_or(0)
    }
}
