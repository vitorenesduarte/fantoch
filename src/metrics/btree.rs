use std::cmp::Ordering;
use std::collections::BTreeMap;

pub fn merge<K, V, F>(map: &mut BTreeMap<K, V>, other: BTreeMap<K, V>, merge_fun: F)
where
    K: Ord + Eq,
    F: Fn(&mut V, V),
{
    // create iterators for `map` and `other`
    let mut map_iter = map.iter_mut();
    let mut other_iter = other.into_iter();

    // variables to hold the "current value" of each iterator
    let mut map_current = map_iter.next();
    let mut other_current = other_iter.next();

    // create vec where we'll store all entries with keys that are in `map`, are smaller than the
    // larger key in `map`, but can't be inserted when interating `map`
    let mut absent = Vec::new();

    loop {
        match (map_current, other_current) {
            (Some((map_key, map_value)), Some((other_key, other_value))) => {
                match map_key.cmp(&other_key) {
                    Ordering::Less => {
                        // simply advance `map` iterator
                        map_current = map_iter.next();
                        other_current = Some((other_key, other_value));
                    }
                    Ordering::Greater => {
                        // save entry to added later
                        absent.push((other_key, other_value));
                        // advance `other` iterator
                        map_current = Some((map_key, map_value));
                        other_current = other_iter.next();
                    }
                    Ordering::Equal => {
                        // merge values
                        merge_fun(map_value, other_value);
                        // advance both iterators
                        map_current = map_iter.next();
                        other_current = other_iter.next();
                    }
                }
            }
            (None, Some(entry)) => {
                // the key in `entry` is the first key from `other` that is larger than the larger
                // key in `map`; save entry and break out of the loop
                absent.push(entry);
                break;
            }
            (_, None) => {
                // there's nothing else to do here as in these (two) cases we have already
                // incorporated all entries from `other`
                break;
            }
        };
    }

    // extend `map` with keys from `other` that are not in `map`:
    // - `absent`: keys from `other` that are smaller than the larger key in `map`
    // - `other_iter`: keys from `other` that are larger than the larger key in `map`
    map.extend(absent);
    map.extend(other_iter);
}

#[cfg(test)]
mod proptests {
    use super::*;
    use crate::elapsed;
    use quickcheck_macros::quickcheck;
    use std::collections::HashMap;
    use std::hash::Hash;
    use std::iter::FromIterator;

    fn hash_merge<K, V, F>(map: &mut HashMap<K, V>, other: HashMap<K, V>, merge_fun: F)
    where
        K: Hash + Eq,
        F: Fn(&mut V, V),
    {
        other.into_iter().for_each(|(k, v)| match map.get_mut(&k) {
            Some(m) => merge_fun(m, v),
            None => {
                map.entry(k).or_insert(v);
            }
        });
    }

    type K = u64;
    type V = Vec<u64>;
    fn merge_fun(m: &mut V, v: V) {
        m.extend(v)
    }

    #[quickcheck]
    fn merge_check(map: Vec<(K, V)>, other: Vec<(K, V)>) -> bool {
        // create hashmaps and merge them
        let mut hashmap = HashMap::from_iter(map.clone());
        let other_hashmap = HashMap::from_iter(other.clone());
        let (naive_time, _) = elapsed!(hash_merge(&mut hashmap, other_hashmap, merge_fun));

        // create btreemaps and merge them
        let mut btreemap = BTreeMap::from_iter(map.clone());
        let other_btreemap = BTreeMap::from_iter(other.clone());
        let (time, _) = elapsed!(merge(&mut btreemap, other_btreemap, merge_fun));

        // show merge times
        println!("{} {}", naive_time.as_nanos(), time.as_nanos());

        btreemap == BTreeMap::from_iter(hashmap)
    }
}
