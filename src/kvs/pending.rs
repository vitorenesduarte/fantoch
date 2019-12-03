use crate::client::Rifl;
use crate::kvs::command::{CommandResult, MultiCommand, MultiCommandResult};
use crate::kvs::Key;
use std::collections::HashMap;

/// Structure that tracks the progress of pending commands.
#[derive(Default)]
pub struct Pending {
    pending: HashMap<Rifl, MultiCommandResult>,
}

impl Pending {
    /// Creates a new `Pending` instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Starts tracking a command submitted by some client.
    pub fn start(&mut self, cmd: &MultiCommand) {
        // create `MultiCommandResult`
        let cmd_result = MultiCommandResult::new(cmd.rifl(), cmd.key_count());

        // add it to pending
        self.pending.insert(cmd.rifl(), cmd_result);
    }

    /// Adds a new partial command result.
    pub fn add_partial(
        &mut self,
        id: Rifl,
        key: Key,
        result: CommandResult,
    ) -> Option<MultiCommandResult> {
        // get current result:
        // - if it's not part of pending, then ignore it
        // (if it's not part of pending, it means that it is from a client
        // from another newt process, and `pending.start` has not been called)
        let cmd_result = self.pending.get_mut(&id)?;

        // add partial result:
        // - if it's complete, remove it from pending and return it
        let is_complete = cmd_result.add_partial(key, result);
        if is_complete {
            self.pending.remove(&id)
        } else {
            None
        }
    }
}
