use crate::client::Rifl;
use crate::command::{Command, CommandResult};
use crate::kvs::{KVOpResult, Key};
use std::collections::HashMap;

/// Structure that tracks the progress of pending commands.
#[derive(Default)]
pub struct Pending {
    pending: HashMap<Rifl, CommandResult>,
}

impl Pending {
    /// Creates a new `Pending` instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Starts tracking a command submitted by some client.
    pub fn start(&mut self, cmd: &Command) {
        // create `CommandResult`
        let cmd_result = CommandResult::new(cmd.rifl(), cmd.key_count());

        // add it to pending
        self.pending.insert(cmd.rifl(), cmd_result);
    }

    /// Adds a new partial command result.
    pub fn add_partial(
        &mut self,
        rifl: Rifl,
        key: Key,
        result: KVOpResult,
    ) -> Option<CommandResult> {
        // get current result:
        // - if it's not part of pending, then ignore it
        // (if it's not part of pending, it means that it is from a client
        // from another newt process, and `pending.start` has not been called)
        let cmd_result = self.pending.get_mut(&rifl)?;

        // add partial result and check if it's ready
        let is_ready = cmd_result.add_partial(key, result);
        if is_ready {
            // if it is, remove it from pending and return it
            self.pending.remove(&rifl)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kvs::{KVOp, KVStore};

    #[test]
    fn pending_flow() {
        // create pending and store
        let mut pending = Pending::new();
        let mut store = KVStore::new();

        // keys and commands
        let key_a = String::from("A");
        let key_b = String::from("B");
        let foo = String::from("foo");
        let bar = String::from("bar");

        // command put a
        let put_a_rifl = Rifl::new(1, 1);
        let put_a = Command::put(put_a_rifl, key_a.clone(), foo.clone());

        // command put b
        let put_b_rifl = Rifl::new(2, 1);
        let put_b = Command::put(put_b_rifl, key_b.clone(), bar.clone());

        // command get a and b
        let get_ab_rifl = Rifl::new(3, 1);
        let get_ab =
            Command::multi_get(get_ab_rifl, vec![key_a.clone(), key_b.clone()]);

        // register `get_ab` and `put_b`
        pending.start(&get_ab);
        pending.start(&put_b);

        // add the result of get b and assert that the command is not ready yet
        let get_b_res = store.execute(&key_b, KVOp::Get);
        let res = pending.add_partial(get_ab_rifl, key_b.clone(), get_b_res);
        assert!(res.is_none());

        // add the result of put a before being registered
        let put_a_res = store.execute(&key_a, KVOp::Put(foo.clone()));
        let res =
            pending.add_partial(put_a_rifl, key_a.clone(), put_a_res.clone());
        assert!(res.is_none());

        // register `put_a`
        pending.start(&put_a);

        // add the result of put a and assert that the command is ready
        let res =
            pending.add_partial(put_a_rifl, key_a.clone(), put_a_res.clone());
        assert!(res.is_some());

        // check that there's only one result (since the command accessed a
        // single key)
        let res = res.unwrap();
        assert_eq!(res.results().len(), 1);

        // check that there was nothing in the kvs before
        assert_eq!(res.results().get(&key_a).unwrap(), &None);

        // add the result of put b and assert that the command is ready
        let put_b_res = store.execute(&key_b, KVOp::Put(bar.clone()));
        let res = pending.add_partial(put_b_rifl, key_b.clone(), put_b_res);

        // check that there's only one result (since the command accessed a
        // single key)
        let res = res.unwrap();
        assert_eq!(res.results().len(), 1);

        // check that there was nothing in the kvs before
        assert_eq!(res.results().get(&key_b).unwrap(), &None);

        // add the result of get a and assert that the command is ready
        let get_a_res = store.execute(&key_a, KVOp::Get);
        let res = pending.add_partial(get_ab_rifl, key_a.clone(), get_a_res);
        assert!(res.is_some());

        // check that there are two results (since the command accessed two
        // keys)
        let res = res.unwrap();
        assert_eq!(res.results().len(), 2);

        // check that `get_ab` saw `put_a` but not `put_b`
        assert_eq!(res.results().get(&key_a).unwrap(), &Some(foo));
        assert_eq!(res.results().get(&key_b).unwrap(), &None);
    }
}
