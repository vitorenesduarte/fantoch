use std::cmp::Ordering;
use std::collections::BTreeMap;

pub fn merge<K, V, F>(map: &mut BTreeMap<K, V>, other: BTreeMap<K, V>, merge_fun: F)
where
    K: Ord + PartialEq + Eq,
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

    type Map = BTreeMap<u64, Vec<u64>>;

    fn merge_fun(m: &mut Vec<u64>, v: Vec<u64>) {
        m.extend(v)
    }

    fn naive_merge(mut map: Map, other: Map) -> Map {
        other.into_iter().for_each(|(k, v)| {
            let m = map.entry(k).or_insert_with(Vec::new);
            merge_fun(m, v)
        });
        map
    }

    #[quickcheck]
    fn merge_check(mut map: Map, other: Map) -> bool {
        let (naive_time, expected) = elapsed!(naive_merge(map.clone(), other.clone()));
        let (time, ()) = elapsed!(merge(&mut map, other, merge_fun));
        println!("{} {}", naive_time.as_nanos(), time.as_nanos());
        map == expected
    }
}
