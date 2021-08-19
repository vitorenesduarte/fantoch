use crate::id::Rifl;
use crate::kvs::Key;
use crate::HashMap;

/// This structure can be used to monitor the order in which commands are
/// executed, per key, and then check that all processes have the same order
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionOrderMonitor {
    order_per_key: HashMap<Key, Vec<Rifl>>,
}

impl ExecutionOrderMonitor {
    pub fn new() -> Self {
        Self {
            order_per_key: Default::default(),
        }
    }

    /// Adds a new command to the monitor.
    /// Read-only commandds are ignored.
    pub fn add(&mut self, key: &Key, read_only: bool, rifl: Rifl) {
        if read_only {
            return;
        }

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

    pub fn get_order(&self, key: &Key) -> Option<&Vec<Rifl>> {
        self.order_per_key.get(key)
    }

    pub fn keys(&self) -> impl Iterator<Item = &Key> {
        self.order_per_key.keys()
    }

    pub fn len(&self) -> usize {
        self.order_per_key.len()
    }
}
