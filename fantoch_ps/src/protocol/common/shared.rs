use ahash::RandomState;
use dashmap::iter::Iter;
use dashmap::mapref::one::Ref;
use dashmap::DashMap;
use fantoch::kvs::Key;
use std::collections::BTreeSet;

type SharedIter<'a, V> =
    Iter<'a, Key, V, RandomState, DashMap<Key, V, RandomState>>;

#[derive(Debug, Clone)]
pub struct Shared<V> {
    clocks: DashMap<Key, V>,
}

impl<V> Shared<V>
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
    pub fn get(&self, key: &Key) -> Ref<'_, Key, V> {
        match self.clocks.get(key) {
            Some(value) => value,
            None => {
                self.maybe_insert(key);
                return self.get(key);
            }
        }
    }

    // Tries to retrieve the current value associated with `keys`. An entry will
    // be created for each of the non-existing keys.
    pub fn get_all<'k, 'd>(
        &'d self,
        keys: &BTreeSet<&'k Key>,
        refs: &mut Vec<(&'k Key, Ref<'d, Key, V>)>,
    ) {
        for key in keys {
            match self.clocks.get(*key) {
                Some(value) => {
                    refs.push((key, value));
                }
                None => {
                    // clear any previous references to the map (since
                    // `self.clocks.entry` can deadlock if we hold any
                    // references to `self.clocks`)
                    refs.clear();
                    // make sure key exits, and start again
                    self.maybe_insert(key);
                    return self.get_all(keys, refs);
                }
            }
        }
    }

    pub fn iter(&self) -> SharedIter<'_, V> {
        self.clocks.iter()
    }

    pub fn len(&self) -> usize {
        self.clocks.len()
    }

    fn maybe_insert(&self, key: &Key) {
        // insert entry only if it doesn't yet exist:
        // - maybe another thread tried to `maybe_insert` and was able to insert
        //   before us
        // - replacing this function with what follows should make the tests
        //   fail (blindly inserting means that we could lose updates)
        // `self.clocks.insert(key.clone(), V::default());`
        // - `Entry::or_*` methods from `dashmap` ensure that we don't lose any
        //   updates. See: https://github.com/xacrimon/dashmap/issues/47
        self.clocks.entry(key.clone()).or_default();
    }
}
