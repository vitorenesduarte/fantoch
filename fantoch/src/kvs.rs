use crate::id::Rifl;
use crate::{executor::ExecutionOrderMonitor, HashMap};
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
}

impl KVStore {
    /// Creates a new `KVStore` instance.
    pub fn new() -> Self {
        Default::default()
    }

    /// Executes a `KVOp` in the `KVStore`.
    #[cfg(test)]
    pub fn execute(&mut self, key: &Key, op: KVOp) -> KVOpResult {
        self.do_execute(key, op)
    }

    pub fn execute_with_monitor(
        &mut self,
        key: &Key,
        op: KVOp,
        rifl: Rifl,
        monitor: &mut Option<ExecutionOrderMonitor>,
    ) -> KVOpResult {
        // update monitor, if we're monitoring
        if let Some(monitor) = monitor {
            monitor.add(&key, rifl);
        }
        self.do_execute(key, op)
    }

    #[allow(clippy::ptr_arg)]
    fn do_execute(&mut self, key: &Key, op: KVOp) -> KVOpResult {
        match op {
            KVOp::Get => self.store.get(key).cloned(),
            KVOp::Put(value) => {
                // only clone key if not already in the KVS
                if let Some((key, previous_value)) =
                    self.store.remove_entry(key)
                {
                    self.store.insert(key, value);
                    Some(previous_value)
                } else {
                    self.store.insert(key.clone(), value)
                }
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
        let mut store = KVStore::new();

        // get key_a    -> none
        assert_eq!(store.execute(&key_a, KVOp::Get), None);
        // get key_b    -> none
        assert_eq!(store.execute(&key_b, KVOp::Get), None);

        // put key_a x -> none
        assert_eq!(store.execute(&key_a, KVOp::Put(x.clone())), None);
        // get key_a    -> some(x)
        assert_eq!(store.execute(&key_a, KVOp::Get), Some(x.clone()));

        // put key_b y -> none
        assert_eq!(store.execute(&key_b, KVOp::Put(y.clone())), None);
        // get key_b    -> some(y)
        assert_eq!(store.execute(&key_b, KVOp::Get), Some(y.clone()));

        // put key_a z -> some(x)
        assert_eq!(
            store.execute(&key_a, KVOp::Put(z.clone())),
            Some(x.clone())
        );
        // get key_a    -> some(z)
        assert_eq!(store.execute(&key_a, KVOp::Get), Some(z.clone()));
        // get key_b    -> some(y)
        assert_eq!(store.execute(&key_b, KVOp::Get), Some(y.clone()));

        // delete key_a -> some(z)
        assert_eq!(store.execute(&key_a, KVOp::Delete), Some(z.clone()));
        // get key_a    -> none
        assert_eq!(store.execute(&key_a, KVOp::Get), None);
        // get key_b    -> some(y)
        assert_eq!(store.execute(&key_b, KVOp::Get), Some(y.clone()));

        // delete key_b -> some(y)
        assert_eq!(store.execute(&key_b, KVOp::Delete), Some(y.clone()));
        // get key_b    -> none
        assert_eq!(store.execute(&key_b, KVOp::Get), None);
        // get key_a    -> none
        assert_eq!(store.execute(&key_a, KVOp::Get), None);

        // put key_a x -> none
        assert_eq!(store.execute(&key_a, KVOp::Put(x.clone())), None);
        // get key_a    -> some(x)
        assert_eq!(store.execute(&key_a, KVOp::Get), Some(x.clone()));
        // get key_b    -> none
        assert_eq!(store.execute(&key_b, KVOp::Get), None);

        // delete key_a -> some(x)
        assert_eq!(store.execute(&key_a, KVOp::Delete), Some(x.clone()));
        // get key_a    -> none
        assert_eq!(store.execute(&key_a, KVOp::Get), None);
    }
}
