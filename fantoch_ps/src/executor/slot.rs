use fantoch::command::Command;
use fantoch::config::Config;
use fantoch::executor::{Executor, ExecutorResult, MessageKey};
use fantoch::id::Rifl;
use fantoch::kvs::KVStore;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

type Slot = u64;

pub struct SlotExecutor {
    execute_at_commit: bool,
    store: KVStore,
    pending: HashSet<Rifl>,
    next_slot: Slot,
    // TODO maybe BinaryHeap
    to_execute: HashMap<Slot, Command>,
}

impl Executor for SlotExecutor {
    type ExecutionInfo = SlotExecutionInfo;

    fn new(config: Config) -> Self {
        let store = KVStore::new();
        let pending = HashSet::new();
        // the next slot to be executed is 1
        let next_slot = 1;
        // there's nothing to execute in the beginnin
        let to_execute = HashMap::new();
        Self {
            execute_at_commit: config.execute_at_commit(),
            store,
            pending,
            next_slot,
            to_execute,
        }
    }

    fn wait_for(&mut self, cmd: &Command) {
        self.wait_for_rifl(cmd.rifl());
    }

    fn wait_for_rifl(&mut self, rifl: Rifl) {
        // start command in pending
        assert!(self.pending.insert(rifl));
    }

    fn handle(&mut self, info: Self::ExecutionInfo) -> Vec<ExecutorResult> {
        let SlotExecutionInfo { slot, cmd } = info;
        // we shouldn't receive execution info about slots already executed
        // TODO actually, if recovery is involved, then this may not be
        // necessarily true
        assert!(slot >= self.next_slot);

        if self.execute_at_commit {
            self.execute(cmd)
        } else {
            // add received command to the commands to be executed and try to
            // execute commands
            // TODO here we could optimize and only insert the command if it
            // isn't the command that will be executed in the next
            // slot
            let res = self.to_execute.insert(slot, cmd);
            assert!(res.is_none());
            self.try_next_slot()
        }
    }

    fn parallel() -> bool {
        false
    }
}

impl SlotExecutor {
    fn try_next_slot(&mut self) -> Vec<ExecutorResult> {
        let mut results = Vec::new();
        // gather commands while the next command to be executed exists
        while let Some(cmd) = self.to_execute.remove(&self.next_slot) {
            results.extend(self.execute(cmd));
            // update the next slot to be executed
            self.next_slot += 1;
        }
        results
    }

    fn execute(&mut self, cmd: Command) -> Vec<ExecutorResult> {
        // get command rifl
        let rifl = cmd.rifl();
        // execute the command
        let result = cmd.execute(&mut self.store);
        // update results if this rifl is pending
        if self.pending.remove(&rifl) {
            vec![ExecutorResult::Ready(result)]
        } else {
            vec![]
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotExecutionInfo {
    slot: Slot,
    cmd: Command,
}

impl SlotExecutionInfo {
    pub fn new(slot: Slot, cmd: Command) -> Self {
        Self { slot, cmd }
    }
}

impl MessageKey for SlotExecutionInfo {}

#[cfg(test)]
mod tests {
    use super::*;
    use permutator::Permutation;

    #[test]
    fn slot_executor_flow() {
        // create rifls
        let rifl_1 = Rifl::new(1, 1);
        let rifl_2 = Rifl::new(2, 1);
        let rifl_3 = Rifl::new(3, 1);
        let rifl_4 = Rifl::new(4, 1);
        let rifl_5 = Rifl::new(5, 1);

        // create commands
        let cmd_1 = Command::put(rifl_1, String::from("a"), String::from("1"));
        let cmd_2 = Command::get(rifl_2, String::from("a"));
        let cmd_3 = Command::put(rifl_3, String::from("a"), String::from("2"));
        let cmd_4 = Command::get(rifl_4, String::from("a"));
        let cmd_5 = Command::put(rifl_5, String::from("b"), String::from("3"));

        // create execution info
        let ei_1 = SlotExecutionInfo::new(1, cmd_1);
        let ei_2 = SlotExecutionInfo::new(2, cmd_2);
        let ei_3 = SlotExecutionInfo::new(3, cmd_3);
        let ei_4 = SlotExecutionInfo::new(4, cmd_4);
        let ei_5 = SlotExecutionInfo::new(5, cmd_5);
        let mut infos = vec![ei_1, ei_2, ei_3, ei_4, ei_5];

        // create expected results:
        // - we don't expect rifl 1 before we will not wait for it in the
        //   executor
        let expected_rifl_order = vec![rifl_2, rifl_3, rifl_4, rifl_5];

        // check the execution order for all possible permutations
        infos.permutation().for_each(|p| {
            // create config (that will not be used)
            let config = Config::new(0, 0);
            // create slot executor
            let mut executor = SlotExecutor::new(config);
            // wait for all rifls with the exception of rifl 1
            executor.wait_for_rifl(rifl_2);
            executor.wait_for_rifl(rifl_3);
            executor.wait_for_rifl(rifl_4);
            executor.wait_for_rifl(rifl_5);

            let rifls: Vec<_> = p
                .clone()
                .into_iter()
                .flat_map(|info| {
                    executor
                        .handle(info)
                        .into_iter()
                        .map(|result| result.unwrap_ready().rifl())
                })
                .collect();
            assert_eq!(rifls, expected_rifl_order);
        });
    }
}
