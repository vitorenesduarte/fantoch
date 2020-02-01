use crate::kvs::Key;
use dashmap::mapref::one::Ref;
use dashmap::DashMap;
use parking_lot::Mutex;
use std::sync::Arc;

#[derive(Clone)]
pub struct SharedClocks<V> {
    insert_lock: Arc<Mutex<()>>,
    clocks: DashMap<Key, V>,
}

impl<V> SharedClocks<V>
where
    V: Default,
{
    // Create a `SharedClocks` instance.
    pub fn new() -> Self {
        // create insert lock
        let insert_lock = Arc::new(Mutex::new(()));
        // create clocks
        let clocks = DashMap::new();
        Self {
            insert_lock,
            clocks,
        }
    }

    // Tries to retrive the current value one `key`. If there's no associated
    // value, an entry will be created by first acquiring a lock before
    // trying to insert. When the lock is grabed, if the entry still doesn't
    // exist then it is created. This ensures that if two threads see that
    // there's no entry, only one will create it.
    pub fn get(&self, key: &Key) -> Ref<Key, V> {
        match self.clocks.get(key) {
            Some(value) => value,
            None => {
                self.maybe_insert(key);
                self.get(key)
            }
        }
    }

    fn maybe_insert(&self, key: &Key) {
        // acquire the write lock
        let _lock = self.insert_lock.lock();
        // insert entry if it doesn't yet exist:
        // - maybe another thread tried to `maybe_insert` and was able to insert
        //   before us
        self.clocks.entry(key.clone()).or_default();
    }
}
