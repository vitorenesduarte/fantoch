use crate::command::Command;
use crate::executor::ExecutorResult;
use crate::hash_map::{Entry, HashMap};
use crate::id::{Rifl, ShardId};
use crate::kvs::{KVOpResult, Key};

/// Structure that tracks the progress of pending commands.
#[derive(Default, Clone)]
pub struct SimplePending {
    shard_id: ShardId,
    pending: HashMap<Rifl, usize>,
}

impl SimplePending {
    /// Creates a new `Pending` instance.
    /// In this `Pending` implementation, results are returned as soon as
    /// received; this structure simply tracks if the result belongs to a client
    /// that has previously waited for such command.
    pub fn new(shard_id: ShardId) -> Self {
        Self {
            shard_id,
            pending: HashMap::new(),
        }
    }

    /// Starts tracking a command submitted by some client.
    pub fn wait_for(&mut self, cmd: &Command) -> bool {
        // get command rifl and key count
        let rifl = cmd.rifl();
        let key_count = cmd.key_count(self.shard_id);
        // add to pending
        self.pending.insert(rifl, key_count).is_none()
    }

    /// Increases the number of expected notifications on some `Rifl` by one.
    pub fn wait_for_rifl(&mut self, rifl: Rifl) {
        let key_count = self.pending.entry(rifl).or_insert(0);
        *key_count += 1;
    }

    /// Adds a new partial command result.
    pub fn add_partial<P>(
        &mut self,
        rifl: Rifl,
        partial: P,
    ) -> Option<ExecutorResult>
    where
        P: FnOnce() -> (Key, KVOpResult),
    {
        // get current value:
        // - if it's not part of pending, then ignore it
        // (if it's not part of pending, it means that it is from a client from
        // another newt process, and `pending.wait_for*` has not been
        // called)
        match self.pending.entry(rifl) {
            Entry::Vacant(_) => None,
            Entry::Occupied(mut entry) => {
                // decrement the number of occurrences
                let count = entry.get_mut();
                *count -= 1; // TODO may underflow if there's a bug?

                // remove entry if occurrences reached 0
                if *count == 0 {
                    entry.remove_entry();
                }

                // never buffer and always return partial result
                let (key, op_result) = partial();
                Some(ExecutorResult::Partial(rifl, key, op_result))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::Command;
    use crate::kvs::{KVOp, KVStore};

    #[test]
    fn pending_flow() {
        // create pending and store
        let shard_id = 0;
        let mut pending = SimplePending::new(shard_id);
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

        // wait for `get_ab` and `put_b`
        assert!(pending.wait_for(&get_ab));
        assert!(pending.wait_for(&put_b));

        // starting a command already started `false`
        assert!(!pending.wait_for(&put_b));

        // add the result of get b
        let get_b_res = store.execute(&key_b, KVOp::Get);
        let res =
            pending.add_partial(get_ab_rifl, || (key_b.clone(), get_b_res));
        // there's always (as long as previously waited for) a result when
        // configured with parallel executors
        assert!(res.is_some());

        // add the result of put a before being waited for
        let put_a_res = store.execute(&key_a, KVOp::Put(foo.clone()));
        let res = pending
            .add_partial(put_a_rifl, || (key_a.clone(), put_a_res.clone()));
        // there's not a result since the command has not been waited for
        assert!(res.is_none());

        // wait for `put_a`
        pending.wait_for(&put_a);

        // add the result of put a
        let res = pending
            .add_partial(put_a_rifl, || (key_a.clone(), put_a_res.clone()));
        assert!(res.is_some());

        // check partial output
        let (rifl, key, result) = res.unwrap().unwrap_partial();
        assert_eq!(rifl, put_a_rifl);
        assert_eq!(key, key_a);
        // there was nothing in the kvs before
        assert!(result.is_none());

        // add the result of put b
        let put_b_res = store.execute(&key_b, KVOp::Put(bar.clone()));
        let res =
            pending.add_partial(put_b_rifl, || (key_b.clone(), put_b_res));
        assert!(res.is_some());

        // check partial output
        let (rifl, key, result) = res.unwrap().unwrap_partial();
        assert_eq!(rifl, put_b_rifl);
        assert_eq!(key, key_b);
        // there was nothing in the kvs before
        assert!(result.is_none());

        // add the result of get a and assert that the command is ready
        let get_a_res = store.execute(&key_a, KVOp::Get);
        let res =
            pending.add_partial(get_ab_rifl, || (key_a.clone(), get_a_res));
        assert!(res.is_some());

        // check partial output
        let (rifl, key, result) = res.unwrap().unwrap_partial();
        assert_eq!(rifl, get_ab_rifl);
        assert_eq!(key, key_a);
        // check that `get_ab` saw `put_a`
        assert_eq!(result, Some(foo));
    }
}
