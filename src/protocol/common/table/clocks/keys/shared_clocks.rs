use crate::kvs::Key;
use dashmap::mapref::one::Ref;
use dashmap::DashMap;

#[derive(Clone)]
pub struct SharedClocks<V> {
    clocks: DashMap<Key, V>,
}

impl<V> SharedClocks<V>
where
    V: Default,
{
    // Create a `SharedClocks` instance.
    pub fn new() -> Self {
        // create clocks
        let clocks = DashMap::new();
        Self { clocks }
    }

    // Tries to retrieve the current value associated with `key`. If there's no
    // associated value, an entry will be created.
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
        // insert entry only if it doesn't yet exist:
        // - maybe another thread tried to `maybe_insert` and was able to insert
        //   before us
        // - replacing the following line with what follows should make the
        //   tests fail (blindly inserting means that we could lose updates)
        // `self.clocks.insert(key.clone(), V::default());`
        self.clocks.entry(key.clone()).or_default();
    }
}
