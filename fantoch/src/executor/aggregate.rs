use crate::command::{Command, CommandResult, CommandResultBuilder};
use crate::executor::ExecutorResult;
use crate::id::{ProcessId, Rifl, ShardId};
use crate::trace;
use crate::HashMap;

/// Structure that tracks the progress of pending commands.
#[derive(Clone)]
pub struct AggregatePending {
    process_id: ProcessId,
    shard_id: ShardId,
    pending: HashMap<Rifl, CommandResultBuilder>,
}

impl AggregatePending {
    /// Creates a new `Pending` instance.
    /// In this `Pending` implementation, results are only returned once they're
    /// the aggregation of all partial results is complete; this also means that
    /// non-parallel executors can return the full command result without having
    /// to return partials
    pub fn new(process_id: ProcessId, shard_id: ShardId) -> Self {
        Self {
            process_id,
            shard_id,
            pending: HashMap::new(),
        }
    }

    /// Starts tracking a command submitted by some client.
    pub fn wait_for(&mut self, cmd: &Command) -> bool {
        // get command rifl and key count
        let rifl = cmd.rifl();
        let key_count = cmd.key_count(self.shard_id);
        trace!(
            "p{}: AggregatePending::wait_for {:?} | count = {}",
            self.process_id,
            rifl,
            key_count
        );

        // create `CommandResult`
        let cmd_result = CommandResultBuilder::new(rifl, key_count);
        // add it to pending
        self.pending.insert(rifl, cmd_result).is_none()
    }

    /// Adds a new partial command result.
    pub fn add_executor_result(
        &mut self,
        executor_result: ExecutorResult,
    ) -> Option<CommandResult> {
        let ExecutorResult {
            rifl,
            key,
            partial_results,
        } = executor_result;
        // get current value:
        // - if it's not part of pending, then ignore it
        // (if it's not part of pending, it means that it is from a client from
        // another newt process, and `pending.wait_for*` has not been
        // called)
        let cmd_result_builder = self.pending.get_mut(&rifl)?;

        // add partial result and check if it's ready
        cmd_result_builder.add_partial(key, partial_results);
        if cmd_result_builder.ready() {
            trace!(
                "p{}: AggregatePending::add_partial {:?} is ready",
                self.process_id,
                rifl
            );
            // if it is, remove it from pending
            let cmd_result_builder = self
                .pending
                .remove(&rifl)
                .expect("command result builder must exist");
            // finally, build the command result
            Some(cmd_result_builder.into())
        } else {
            trace!(
                "p{}: AggregatePending::add_partial {:?} is not ready",
                self.process_id,
                rifl
            );
            None
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
        let process_id = 1;
        let shard_id = 0;
        let mut pending = AggregatePending::new(process_id, shard_id);
        let monitor = false;
        let mut store = KVStore::new(monitor);

        // keys and commands
        let key_a = String::from("A");
        let key_b = String::from("B");
        let foo = String::from("foo");
        let bar = String::from("bar");

        // command put a
        let put_a_rifl = Rifl::new(1, 1);
        let put_a = Command::from(
            put_a_rifl,
            vec![(key_a.clone(), KVOp::Put(foo.clone()))],
        );

        // command put b
        let put_b_rifl = Rifl::new(2, 1);
        let put_b = Command::from(
            put_b_rifl,
            vec![(key_b.clone(), KVOp::Put(bar.clone()))],
        );

        // command get a and b
        let get_ab_rifl = Rifl::new(3, 1);
        let get_ab = Command::from(
            get_ab_rifl,
            vec![(key_a.clone(), KVOp::Get), (key_b.clone(), KVOp::Get)],
        );

        // wait for `get_ab` and `put_b`
        assert!(pending.wait_for(&get_ab));
        assert!(pending.wait_for(&put_b));

        // starting a command already started `false`
        assert!(!pending.wait_for(&put_b));

        // add the result of get b and assert that the command is not ready yet
        let get_b_res = store.test_execute(&key_b, KVOp::Get);
        let res = pending.add_executor_result(ExecutorResult::new(
            get_ab_rifl,
            key_b.clone(),
            vec![get_b_res],
        ));
        assert!(res.is_none());

        // add the result of put a before being waited for
        let put_a_res = store.test_execute(&key_a, KVOp::Put(foo.clone()));
        let res = pending.add_executor_result(ExecutorResult::new(
            put_a_rifl,
            key_a.clone(),
            vec![put_a_res.clone()],
        ));
        assert!(res.is_none());

        // wait for `put_a`
        pending.wait_for(&put_a);

        // add the result of put a and assert that the command is ready
        let res = pending.add_executor_result(ExecutorResult::new(
            put_a_rifl,
            key_a.clone(),
            vec![put_a_res.clone()],
        ));
        assert!(res.is_some());

        // check that there's only one result (since the command accessed a
        // single key)
        let res = res.unwrap();
        assert_eq!(res.results().len(), 1);

        // check that there was nothing in the kvs before
        assert_eq!(res.results().get(&key_a).unwrap(), &vec![None]);

        // add the result of put b and assert that the command is ready
        let put_b_res = store.test_execute(&key_b, KVOp::Put(bar.clone()));
        let res = pending.add_executor_result(ExecutorResult::new(
            put_b_rifl,
            key_b.clone(),
            vec![put_b_res],
        ));

        // check that there's only one result (since the command accessed a
        // single key)
        let res = res.unwrap();
        assert_eq!(res.results().len(), 1);

        // check that there was nothing in the kvs before
        assert_eq!(res.results().get(&key_b).unwrap(), &vec![None]);

        // add the result of get a and assert that the command is ready
        let get_a_res = store.test_execute(&key_a, KVOp::Get);
        let res = pending.add_executor_result(ExecutorResult::new(
            get_ab_rifl,
            key_a.clone(),
            vec![get_a_res],
        ));
        assert!(res.is_some());

        // check that there are two results (since the command accessed two
        // keys)
        let res = res.unwrap();
        assert_eq!(res.results().len(), 2);

        // check that `get_ab` saw `put_a` but not `put_b`
        assert_eq!(res.results().get(&key_a).unwrap(), &vec![Some(foo)]);
        assert_eq!(res.results().get(&key_b).unwrap(), &vec![None]);
    }
}
