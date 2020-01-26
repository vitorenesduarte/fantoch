use crate::command::{Command, CommandResult};
use crate::executor::ExecutorResult;
use crate::id::Rifl;
use crate::kvs::{KVOpResult, Key};
use std::collections::hash_map::{Entry, HashMap};

/// Structure that tracks the progress of pending commands.
#[derive(Default)]
pub struct Pending {
    // TODO this should be a feature; with that, most conditionals below could be removed at
    // compile-time
    agggregate: bool,
    aggregated_pending: HashMap<Rifl, CommandResult>,
    pending: HashMap<Rifl, usize>,
}

impl Pending {
    /// Creates a new `Pending` instance.
    /// If configured with:
    /// - `agggregate = false`, then results are returned as soon as received; this structure simply
    ///   tracks if the result belongs to a client that has previously waited for such command.
    /// - `agggregate = true`, then results are only returned once they're the aggregation of all
    ///   partial results is complete; this also means that non-parallel executors can return the
    ///   full command result without having to return partials
    pub fn new(agggregate: bool) -> Self {
        Self {
            agggregate,
            aggregated_pending: HashMap::new(),
            pending: HashMap::new(),
        }
    }

    /// Starts tracking a command submitted by some client.
    pub fn wait_for(&mut self, cmd: &Command) -> bool {
        // get command rifl and key count
        let rifl = cmd.rifl();
        let key_count = cmd.key_count();

        if self.agggregate {
            // create `CommandResult`
            let cmd_result = CommandResult::new(rifl, key_count);

            // add it to pending
            self.aggregated_pending.insert(rifl, cmd_result).is_none()
        } else {
            self.pending.insert(rifl, key_count).is_none()
        }
    }

    /// Increases the number of expected notifications on some `Rifl` by one.
    pub fn wait_for_rifl(&mut self, rifl: Rifl) {
        if self.agggregate {
            // maybe update `CommandResult`
            let cmd_result = self
                .aggregated_pending
                .entry(rifl)
                .or_insert_with(|| CommandResult::new(rifl, 0));
            cmd_result.increment_key_count();
        } else {
            let key_count = self.pending.entry(rifl).or_insert(0);
            *key_count += 1;
        }
    }

    /// Adds a new partial command result.
    /// By getting a reference to the `Key` we only clone when it's really needed.
    pub fn add_partial<P>(&mut self, rifl: Rifl, partial: P) -> Option<ExecutorResult>
    where
        P: FnOnce() -> (Key, KVOpResult),
    {
        // get current value:
        // - if it's not part of pending, then ignore it
        // (if it's not part of pending, it means that it is from a client from another newt
        // process, and `pending.wait_for*` has not been called)
        if self.agggregate {
            let cmd_result = self.aggregated_pending.get_mut(&rifl)?;

            // add partial result and check if it's ready
            let (key, op_result) = partial();
            let is_ready = cmd_result.add_partial(key, op_result);
            if is_ready {
                // if it is, remove it from aggregated_pending and return it as ready
                self.aggregated_pending
                    .remove(&rifl)
                    .map(|command_result| ExecutorResult::Ready(command_result))
            } else {
                None
            }
        } else {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::Command;
    use crate::kvs::{KVOp, KVStore};

    #[test]
    fn aggregated_pending_flow() {
        // create aggregated_pending and store
        let agggregate = false;
        let mut aggregated_pending = Pending::new(agggregate);
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
        let get_ab = Command::multi_get(get_ab_rifl, vec![key_a.clone(), key_b.clone()]);

        // wait for `get_ab` and `put_b`
        assert!(aggregated_pending.wait_for(&get_ab));
        assert!(aggregated_pending.wait_for(&put_b));

        // starting a command already started `false`
        assert!(!aggregated_pending.wait_for(&put_b));

        // add the result of get b and assert that the command is not ready yet
        let get_b_res = store.execute(&key_b, KVOp::Get);
        let res = aggregated_pending.add_partial(get_ab_rifl, || (key_b.clone(), get_b_res));
        assert!(res.is_none());

        // add the result of put a before being waited for
        let put_a_res = store.execute(&key_a, KVOp::Put(foo.clone()));
        let res = aggregated_pending.add_partial(put_a_rifl, || (key_a.clone(), put_a_res.clone()));
        assert!(res.is_none());

        // wait for `put_a`
        aggregated_pending.wait_for(&put_a);

        // add the result of put a and assert that the command is ready
        let res = aggregated_pending.add_partial(put_a_rifl, || (key_a.clone(), put_a_res.clone()));
        assert!(res.is_some());

        // check that there's only one result (since the command accessed a
        // single key)
        let res = res.unwrap().unwrap_ready();
        assert_eq!(res.results().len(), 1);

        // check that there was nothing in the kvs before
        assert_eq!(res.results().get(&key_a).unwrap(), &None);

        // add the result of put b and assert that the command is ready
        let put_b_res = store.execute(&key_b, KVOp::Put(bar.clone()));
        let res = aggregated_pending.add_partial(put_b_rifl, || (key_b.clone(), put_b_res));

        // check that there's only one result (since the command accessed a
        // single key)
        let res = res.unwrap().unwrap_ready();
        assert_eq!(res.results().len(), 1);

        // check that there was nothing in the kvs before
        assert_eq!(res.results().get(&key_b).unwrap(), &None);

        // add the result of get a and assert that the command is ready
        let get_a_res = store.execute(&key_a, KVOp::Get);
        let res = aggregated_pending.add_partial(get_ab_rifl, || (key_a.clone(), get_a_res));
        assert!(res.is_some());

        // check that there are two results (since the command accessed two
        // keys)
        let res = res.unwrap().unwrap_ready();
        assert_eq!(res.results().len(), 2);

        // check that `get_ab` saw `put_a` but not `put_b`
        assert_eq!(res.results().get(&key_a).unwrap(), &Some(foo));
        assert_eq!(res.results().get(&key_b).unwrap(), &None);
    }

    #[test]
    fn pending_flow() {
        // create aggregated_pending and store
        let agggregate = true;
        let mut aggregated_pending = Pending::new(agggregate);
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
        let get_ab = Command::multi_get(get_ab_rifl, vec![key_a.clone(), key_b.clone()]);

        // wait for `get_ab` and `put_b`
        assert!(aggregated_pending.wait_for(&get_ab));
        assert!(aggregated_pending.wait_for(&put_b));

        // starting a command already started `false`
        assert!(!aggregated_pending.wait_for(&put_b));

        // add the result of get b
        let get_b_res = store.execute(&key_b, KVOp::Get);
        let res = aggregated_pending.add_partial(get_ab_rifl, || (key_b.clone(), get_b_res));
        // there's always (as long as previously waited for) a result when configured with parallel
        // executors
        assert!(res.is_some());

        // add the result of put a before being waited for
        let put_a_res = store.execute(&key_a, KVOp::Put(foo.clone()));
        let res = aggregated_pending.add_partial(put_a_rifl, || (key_a.clone(), put_a_res.clone()));
        // there's not a result since the command has not been waited for
        assert!(res.is_none());

        // wait for `put_a`
        aggregated_pending.wait_for(&put_a);

        // add the result of put a
        let res = aggregated_pending.add_partial(put_a_rifl, || (key_a.clone(), put_a_res.clone()));
        assert!(res.is_some());

        // check partial output
        let (rifl, key, result) = res.unwrap().unwrap_partial();
        assert_eq!(rifl, put_a_rifl);
        assert_eq!(key, key_a);
        // there was nothing in the kvs before
        assert!(result.is_none());

        // add the result of put b
        let put_b_res = store.execute(&key_b, KVOp::Put(bar.clone()));
        let res = aggregated_pending.add_partial(put_b_rifl, || (key_b.clone(), put_b_res));
        assert!(res.is_some());

        // check partial output
        let (rifl, key, result) = res.unwrap().unwrap_partial();
        assert_eq!(rifl, put_b_rifl);
        assert_eq!(key, key_b);
        // there was nothing in the kvs before
        assert!(result.is_none());

        // add the result of get a and assert that the command is ready
        let get_a_res = store.execute(&key_a, KVOp::Get);
        let res = aggregated_pending.add_partial(get_ab_rifl, || (key_a.clone(), get_a_res));
        assert!(res.is_some());

        // check partial output
        let (rifl, key, result) = res.unwrap().unwrap_partial();
        assert_eq!(rifl, get_ab_rifl);
        assert_eq!(key, key_a);
        // check that `get_ab` saw `put_a`
        assert_eq!(result, Some(foo));
    }
}
