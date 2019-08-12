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
            Command::Delete => self.store.remove(&key),
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
        let key_a = String::from("A");
        let key_b = String::from("B");
        let x = String::from("x");
        let y = String::from("y");
        let z = String::from("z");

        // store
        let mut store = KVStore::new();

        // get key_a    -> none
        assert_eq!(store.execute(key_a.clone(), Command::Get), None);
        // get key_b    -> none
        assert_eq!(store.execute(key_b.clone(), Command::Get), None);

        // put key_a x -> none
        assert_eq!(store.execute(key_a.clone(), Command::Put(x.clone())), None);
        // get key_a    -> some(x)
        assert_eq!(store.execute(key_a.clone(), Command::Get), Some(x.clone()));

        // put key_b y -> none
        assert_eq!(store.execute(key_b.clone(), Command::Put(y.clone())), None);
        // get key_b    -> some(y)
        assert_eq!(store.execute(key_b.clone(), Command::Get), Some(y.clone()));

        // put key_a z -> some(x)
        assert_eq!(
            store.execute(key_a.clone(), Command::Put(z.clone())),
            Some(x.clone())
        );
        // get key_a    -> some(z)
        assert_eq!(store.execute(key_a.clone(), Command::Get), Some(z.clone()));
        // get key_b    -> some(y)
        assert_eq!(store.execute(key_b.clone(), Command::Get), Some(y.clone()));

        // delete key_a -> some(z)
        assert_eq!(
            store.execute(key_a.clone(), Command::Delete),
            Some(z.clone())
        );
        // get key_a    -> none
        assert_eq!(store.execute(key_a.clone(), Command::Get), None);
        // get key_b    -> some(y)
        assert_eq!(store.execute(key_b.clone(), Command::Get), Some(y.clone()));

        // delete key_b -> some(y)
        assert_eq!(
            store.execute(key_b.clone(), Command::Delete),
            Some(y.clone())
        );
        // get key_b    -> none
        assert_eq!(store.execute(key_b.clone(), Command::Get), None);
        // get key_a    -> none
        assert_eq!(store.execute(key_a.clone(), Command::Get), None);

        // put key_a x -> none
        assert_eq!(store.execute(key_a.clone(), Command::Put(x.clone())), None);
        // get key_a    -> some(x)
        assert_eq!(store.execute(key_a.clone(), Command::Get), Some(x.clone()));
        // get key_b    -> none
        assert_eq!(store.execute(key_b.clone(), Command::Get), None);

        // delete key_a -> some(x)
        assert_eq!(
            store.execute(key_a.clone(), Command::Delete),
            Some(x.clone())
        );
        // get key_a    -> none
        assert_eq!(store.execute(key_a.clone(), Command::Get), None);
    }
}
