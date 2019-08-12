use crate::command::Command;
use std::collections::HashMap;

pub type Key = String;
pub type Value = String;

#[derive(Default)]
pub struct KVStore {
    store: HashMap<Key, Value>,
}

impl KVStore {
    /// Creates a new `KVStore` instance.
    pub fn new() -> Self {
        KVStore::default()
    }

    /// Executes a `Command` on the `KVStore`.
    pub fn execute(&mut self, key: Key, cmd: Command) -> Option<Value> {
        match cmd {
            Command::Get => self.store.get(&key).cloned(),
            Command::Put(value) => self.store.insert(key, value),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::command::Command;
    use crate::store::KVStore;

    #[test]
    fn store_flow() {
        // key and values
        let key = String::from("K");
        let v1 = String::from("V1");
        let v2 = String::from("V2");

        // store
        let mut store = KVStore::new();

        // get key    -> none
        assert_eq!(store.execute(key.clone(), Command::Get), None);
        // put key v1 -> none
        assert_eq!(store.execute(key.clone(), Command::Put(v1.clone())), None);
        // get key    -> some(v1)
        assert_eq!(store.execute(key.clone(), Command::Get), Some(v1.clone()));
        // put key v2 -> some(v1)
        assert_eq!(
            store.execute(key.clone(), Command::Put(v2.clone())),
            Some(v1.clone())
        );
    }
}
