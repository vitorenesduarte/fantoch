use ahash::RandomState;
use dashmap::iter::Iter;
use dashmap::mapref::entry::Entry;
use dashmap::mapref::one::Ref;
use dashmap::DashMap;
use std::collections::BTreeSet;
use std::hash::Hash;

type SharedIter<'a, K, V> =
    Iter<'a, K, V, RandomState, DashMap<K, V, RandomState>>;

#[derive(Debug, Clone)]
pub struct Shared<K: Eq + Hash + Clone, V> {
    shared: DashMap<K, V>,
}

impl<K, V> Shared<K, V>
where
    K: Eq + Hash + Clone,
{
    // Create a `Shared` instance.
    pub fn new() -> Self {
        // create shared
        let shared = DashMap::new();
        Self { shared }
    }

    pub fn get(&self, key: &K) -> Option<Ref<'_, K, V>> {
        self.shared.get(key)
    }

    // Tries to retrieve the current value associated with `key`. If there's no
    // associated value, an entry will be created.
    pub fn get_or<F>(&self, key: &K, value: F) -> Ref<'_, K, V>
    where
        F: Fn() -> V + Copy,
    {
        match self.shared.get(key) {
            Some(value) => value,
            None => {
                self.maybe_insert(key, value);
                return self.get_or(key, value);
            }
        }
    }

    // Tries to retrieve the current value associated with `keys`. An entry will
    // be created for each of the non-existing keys.
    pub fn get_or_all<'k, 'd, F>(
        &'d self,
        keys: &BTreeSet<&'k K>,
        refs: &mut Vec<(&'k K, Ref<'d, K, V>)>,
        value: F,
    ) where
        F: Fn() -> V + Copy,
    {
        for key in keys {
            match self.shared.get(*key) {
                Some(value) => {
                    refs.push((key, value));
                }
                None => {
                    // clear any previous references to the map (since
                    // `self.shared.entry` can deadlock if we hold any
                    // references to `self.shared`)
                    refs.clear();
                    // make sure key exits, and start again
                    self.maybe_insert(key, value);
                    return self.get_or_all(keys, refs, value);
                }
            }
        }
    }

    pub fn insert(&self, key: K, value: V) -> Option<V> {
        self.shared.insert(key, value)
    }

    pub fn remove(&self, key: &K) -> Option<(K, V)> {
        self.shared.remove(key)
    }

    pub fn iter(&self) -> SharedIter<'_, K, V> {
        self.shared.iter()
    }

    fn maybe_insert<F>(&self, key: &K, value: F)
    where
        F: Fn() -> V,
    {
        // insert entry only if it doesn't yet exist:
        // - maybe another thread tried to `maybe_insert` and was able to insert
        //   before us
        // - replacing this function with what follows should make the tests
        //   fail (blindly inserting means that we could lose updates)
        // `self.shared.insert(key.clone(), value());`
        // - `Entry::or_*` methods from `dashmap` ensure that we don't lose any
        //   updates. See: https://github.com/xacrimon/dashmap/issues/47
        self.shared.entry(key.clone()).or_insert_with(value);
    }
}
