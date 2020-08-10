use fantoch::command::Command;
use fantoch::config::Config;
use fantoch::executor::{Executor, ExecutorMetrics, ExecutorResult};
use fantoch::id::{ProcessId, ShardId};
use fantoch::kvs::KVStore;
use fantoch::protocol::MessageIndex;
use fantoch::HashMap;
use serde::{Deserialize, Serialize};

type Slot = u64;

#[derive(Clone)]
pub struct SlotExecutor {
    shard_id: ShardId,
    config: Config,
    store: KVStore,
    next_slot: Slot,
    // TODO maybe BinaryHeap
    to_execute: HashMap<Slot, Command>,
    metrics: ExecutorMetrics,
    to_clients: Vec<ExecutorResult>,
}

impl Executor for SlotExecutor {
    type ExecutionInfo = SlotExecutionInfo;

    fn new(_process_id: ProcessId, shard_id: ShardId, config: Config) -> Self {
        let store = KVStore::new();
        // the next slot to be executed is 1
        let next_slot = 1;
        // there's nothing to execute in the beginning
        let to_execute = HashMap::new();
        let metrics = ExecutorMetrics::new();
        let to_clients = Vec::new();
        Self {
            shard_id,
            config,
            store,
            next_slot,
            to_execute,
            metrics,
            to_clients,
        }
    }

    fn handle(&mut self, info: Self::ExecutionInfo) {
        let SlotExecutionInfo { slot, cmd } = info;
        // we shouldn't receive execution info about slots already executed
        // TODO actually, if recovery is involved, then this may not be
        // necessarily true
        assert!(slot >= self.next_slot);

        if self.config.execute_at_commit() {
            self.execute(cmd);
        } else {
            // add received command to the commands to be executed and try to
            // execute commands
            // TODO here we could optimize and only insert the command if it
            // isn't the command that will be executed in the next
            // slot
            let res = self.to_execute.insert(slot, cmd);
            assert!(res.is_none());
            self.try_next_slot();
        }
    }

    fn to_clients(&mut self) -> Option<ExecutorResult> {
        self.to_clients.pop()
    }

    fn max_executors() -> Option<usize> {
        Some(1)
    }

    fn metrics(&self) -> &ExecutorMetrics {
        &self.metrics
    }
}

impl SlotExecutor {
    fn try_next_slot(&mut self) {
        // gather commands while the next command to be executed exists
        while let Some(cmd) = self.to_execute.remove(&self.next_slot) {
            self.execute(cmd);
            // update the next slot to be executed
            self.next_slot += 1;
        }
    }

    fn execute(&mut self, cmd: Command) {
        // execute the command
        let results = cmd.execute(self.shard_id, &mut self.store);
        // update results if this rifl is pending
        self.to_clients.extend(results);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotExecutionInfo {
    slot: Slot,
    cmd: Command,
}

impl SlotExecutionInfo {
    pub fn new(slot: Slot, cmd: Command) -> Self {
        Self { slot, cmd }
    }
}

impl MessageIndex for SlotExecutionInfo {
    fn index(&self) -> Option<(usize, usize)> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fantoch::id::Rifl;
    use permutator::Permutation;
    use std::collections::BTreeMap;

    #[test]
    fn slot_executor_flow() {
        // create rifls
        let rifl_1 = Rifl::new(1, 1);
        let rifl_2 = Rifl::new(2, 1);
        let rifl_3 = Rifl::new(3, 1);
        let rifl_4 = Rifl::new(4, 1);
        let rifl_5 = Rifl::new(5, 1);

        // create commands
        let key = String::from("a");
        let cmd_1 = Command::put(rifl_1, key.clone(), String::from("1"));
        let cmd_2 = Command::get(rifl_2, key.clone());
        let cmd_3 = Command::put(rifl_3, key.clone(), String::from("2"));
        let cmd_4 = Command::get(rifl_4, key.clone());
        let cmd_5 = Command::put(rifl_5, key.clone(), String::from("3"));

        // create execution info
        let ei_1 = SlotExecutionInfo::new(1, cmd_1);
        let ei_2 = SlotExecutionInfo::new(2, cmd_2);
        let ei_3 = SlotExecutionInfo::new(3, cmd_3);
        let ei_4 = SlotExecutionInfo::new(4, cmd_4);
        let ei_5 = SlotExecutionInfo::new(5, cmd_5);
        let mut infos = vec![ei_1, ei_2, ei_3, ei_4, ei_5];

        // create expected results:
        // - we don't expect rifl 1 because we will not wait for it in the
        //   executor
        let mut expected_results = BTreeMap::new();
        expected_results.insert(rifl_1, None);
        expected_results.insert(rifl_2, Some(String::from("1")));
        expected_results.insert(rifl_3, Some(String::from("1")));
        expected_results.insert(rifl_4, Some(String::from("2")));
        expected_results.insert(rifl_5, Some(String::from("2")));

        // check the execution order for all possible permutations
        infos.permutation().for_each(|p| {
            // create config (that will not be used)
            let process_id = 1;
            let config = Config::new(0, 0);
            // there's a single shard
            let shard_id = 0;
            // create slot executor
            let mut executor = SlotExecutor::new(process_id, shard_id, config);

            let results: BTreeMap<_, _> = p
                .clone()
                .into_iter()
                .flat_map(|info| {
                    executor.handle(info);
                    executor
                        .to_clients_iter()
                        .map(|executor_result| {
                            assert_eq!(
                                key, executor_result.key,
                                "expected key not in partial"
                            );
                            (executor_result.rifl, executor_result.op_result)
                        })
                        .collect::<Vec<_>>()
                })
                .collect();
            assert_eq!(results, expected_results);
        });
    }
}
