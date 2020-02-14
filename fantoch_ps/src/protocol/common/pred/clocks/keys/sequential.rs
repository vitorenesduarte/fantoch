use super::KeyClocks;
use fantoch::command::Command;
use fantoch::id::Dot;
use fantoch::kvs::Key;
use std::collections::{BTreeMap, HashMap, HashSet};

// timestamps are unique and thus it's enough to store one command `Dot` per
// timestamp.
type CommandsPerKey = BTreeMap<u64, Dot>;

#[derive(Clone)]
pub struct SequentialKeyClocks {
    clocks: HashMap<Key, CommandsPerKey>,
}

impl KeyClocks for SequentialKeyClocks {
    /// Create a new `KeyClocks` instance.
    fn new() -> Self {
        let clocks = HashMap::new();
        Self { clocks }
    }

    /// Computes this command's set of predecessors. From this moment on, this
    /// command will be reported as a predecessor of commands with a higher
    /// timestamp.
    fn predecessors(
        &mut self,
        dot: Dot,
        cmd: &Command,
        clock: u64,
    ) -> HashSet<Dot> {
        todo!()
    }

    fn parallel() -> bool {
        false
    }
}
