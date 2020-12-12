use crate::id::Rifl;
use crate::kvs::Key;
use std::collections::BTreeMap;

/// This structure can be used to monitor the order in which commands are
/// executed, per key. It can be used to all processes have the same order per
/// key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExecutionOrderMonitor {
    order_per_key: BTreeMap<Key, Vec<Rifl>>,
}

impl ExecutionOrderMonitor {
    pub fn new() -> Self {
        Self {
            order_per_key: Default::default(),
        }
    }

    /// Adds a new command to the monitor.
    pub fn add(&mut self, key: &Key, rifl: Rifl) {
        if let Some(current) = self.order_per_key.get_mut(key) {
            current.push(rifl);
        } else {
            self.order_per_key.insert(key.clone(), vec![rifl]);
        }
    }

    /// Merge other monitor into this one. This can be used by protocols that
    /// can have multiple executors.
    pub fn merge(&mut self, other: Self) {
        for (key, rifls) in other.order_per_key {
            let result = self.order_per_key.insert(key, rifls);
            // different monitors should operate on different keys; panic if
            // that's not the case
            assert!(result.is_none());
        }
    }
}
