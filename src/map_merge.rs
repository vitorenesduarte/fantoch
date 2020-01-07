use std::cmp::Ordering;
use std::collections::BTreeMap;

pub fn merge<K, V, F>(map: &mut BTreeMap<K, V>, other: BTreeMap<K, V>, merge_fun: F)
where
    K: Ord + PartialEq + Eq,
    F: Fn(&mut V, V),
{
    // create a peekable iterator of `map`
    let mut map_iter = map.iter_mut().peekable();

    // create an iterator of `other`
    let mut other_iter = other.into_iter();

    // variables to hold the current value of each iterator
    let mut map_current = map_iter.next();
    let mut other_current = other_iter.next();

    // create vec where we'll store all entries that can't be inserted when interating `map`
    let mut extras = Vec::new();

    loop {
        match (map_current, other_current) {
            (Some((map_key, map_value)), Some((other_key, other_value))) => {
                match map_key.cmp(&other_key) {
                    Ordering::Equal => {
                        // merge values
                        merge_fun(map_value, other_value);
                        // advance both iterators
                        map_current = map_iter.next();
                        other_current = other_iter.next();
                    }
                    Ordering::Greater => {
                        // save entry to added later
                        extras.push((other_key, other_value));
                        // only advance `other` iterator
                        map_current = Some((map_key, map_value));
                        other_current = other_iter.next();
                    }
                    Ordering::Less => {
                        // only advance `map` iterator
                        map_current = map_iter.next();
                        other_current = Some((other_key, other_value));
                    }
                }
            }
            (None, Some(entry)) => {
                // the current key from in `entry` is the first key from `other` that is larger than
                // the larger key in `map`; save entry and break out of the loop
                extras.push(entry);
                break;
            }
            (_, None) => {
                // there's nothing else to do here as in these (two) cases we have already
                // incorporated all values from `other`
                break;
            }
        };
    }

    // extend `map` with values that are not in map:
    // - `extras`: values from `other` that are smaller than the larger key in `map`
    // - `other`:  values from `other` that are larger than the larger key in `map`
    map.extend(extras);
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
