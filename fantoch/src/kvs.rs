use crate::executor::ExecutionOrderMonitor;
use crate::id::Rifl;
use crate::HashMap;
use serde::{Deserialize, Serialize};

// Definition of `Key` and `Value` types.
pub type Key = String;
pub type Value = String;

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub enum KVOp {
    Get,
    Put(Value),
    Delete,
}

pub type KVOpResult = Option<Value>;

#[derive(Default, Clone)]
pub struct KVStore {
    store: HashMap<Key, Value>,
    monitor: Option<ExecutionOrderMonitor>,
}

impl KVStore {
    /// Creates a new `KVStore` instance.
    pub fn new(monitor_execution_order: bool) -> Self {
        let monitor = if monitor_execution_order {
            Some(ExecutionOrderMonitor::new())
        } else {
            None
        };
        Self {
            store: Default::default(),
            monitor,
        }
    }

    pub fn monitor(&self) -> Option<&ExecutionOrderMonitor> {
        self.monitor.as_ref()
    }

    /// Executes `KVOp`s in the `KVStore`.
    #[cfg(test)]
    pub fn test_execute(&mut self, key: &Key, op: KVOp) -> KVOpResult {
        let mut results = self.do_execute(key, vec![op]);
        assert_eq!(results.len(), 1);
        results.pop().unwrap()
    }

    pub fn execute(
        &mut self,
        key: &Key,
        ops: Vec<KVOp>,
        rifl: Rifl,
    ) -> Vec<KVOpResult> {
        // update monitor, if we're monitoring
        if let Some(monitor) = self.monitor.as_mut() {
            monitor.add(&key, rifl);
        }
        self.do_execute(key, ops)
    }

    #[allow(clippy::ptr_arg)]
    fn do_execute(&mut self, key: &Key, ops: Vec<KVOp>) -> Vec<KVOpResult> {
        ops.into_iter()
            .map(|op| self.do_execute_op(key, op))
            .collect()
    }

    fn do_execute_op(&mut self, key: &Key, op: KVOp) -> KVOpResult {
        match op {
            KVOp::Get => self.store.get(key).cloned(),
            KVOp::Put(value) => {
                // don't return the previous value
                self.store.insert(key.clone(), value);
                None
            }
            KVOp::Delete => self.store.remove(key),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_flow() {
        // key and values
        let key_a = String::from("A");
        let key_b = String::from("B");
        let x = String::from("x");
        let y = String::from("y");
        let z = String::from("z");

        // store
        let monitor = false;
        let mut store = KVStore::new(monitor);

        // get key_a    -> none
        assert_eq!(store.test_execute(&key_a, KVOp::Get), None);
        // get key_b    -> none
        assert_eq!(store.test_execute(&key_b, KVOp::Get), None);

        // put key_a x -> none
        assert_eq!(store.test_execute(&key_a, KVOp::Put(x.clone())), None);
        // get key_a    -> some(x)
        assert_eq!(store.test_execute(&key_a, KVOp::Get), Some(x.clone()));

        // put key_b y -> none
        assert_eq!(store.test_execute(&key_b, KVOp::Put(y.clone())), None);
        // get key_b    -> some(y)
        assert_eq!(store.test_execute(&key_b, KVOp::Get), Some(y.clone()));

        // put key_a z -> some(x)
        assert_eq!(
            store.test_execute(&key_a, KVOp::Put(z.clone())),
            None,
            /*
            the following is correct if Put returns the previous value
            Some(x.clone())
             */
        );
        // get key_a    -> some(z)
        assert_eq!(store.test_execute(&key_a, KVOp::Get), Some(z.clone()));
        // get key_b    -> some(y)
        assert_eq!(store.test_execute(&key_b, KVOp::Get), Some(y.clone()));

        // delete key_a -> some(z)
        assert_eq!(store.test_execute(&key_a, KVOp::Delete), Some(z.clone()));
        // get key_a    -> none
        assert_eq!(store.test_execute(&key_a, KVOp::Get), None);
        // get key_b    -> some(y)
        assert_eq!(store.test_execute(&key_b, KVOp::Get), Some(y.clone()));

        // delete key_b -> some(y)
        assert_eq!(store.test_execute(&key_b, KVOp::Delete), Some(y.clone()));
        // get key_b    -> none
        assert_eq!(store.test_execute(&key_b, KVOp::Get), None);
        // get key_a    -> none
        assert_eq!(store.test_execute(&key_a, KVOp::Get), None);

        // put key_a x -> none
        assert_eq!(store.test_execute(&key_a, KVOp::Put(x.clone())), None);
        // get key_a    -> some(x)
        assert_eq!(store.test_execute(&key_a, KVOp::Get), Some(x.clone()));
        // get key_b    -> none
        assert_eq!(store.test_execute(&key_b, KVOp::Get), None);

        // delete key_a -> some(x)
        assert_eq!(store.test_execute(&key_a, KVOp::Delete), Some(x.clone()));
        // get key_a    -> none
        assert_eq!(store.test_execute(&key_a, KVOp::Get), None);
    }
}
