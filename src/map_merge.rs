use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::mem;

// TODO maybe this can be implemented more efficiently once API's like `BTreeMap.first_entry` are
// stabilized.
pub fn merge<K, V, F>(map: &mut BTreeMap<K, V>, mut other: BTreeMap<K, V>, merge_fun: F)
where
    K: Ord + PartialEq + Eq,
    K: std::fmt::Debug,
    V: std::fmt::Debug,
    F: Fn(&mut V, V),
{
    // create a peekable iterator of `map`
    let mut map_iter = map.iter_mut().peekable();

    let smaller = if let Some((first_key, _)) = map_iter.peek() {
        // remove all keys from `other` that are smaller than the first key in `map`
        let mut remaining = other.split_off(first_key);
        mem::swap(&mut remaining, &mut other);
        remaining
    } else {
        // in this case, `map` is empty and we should simply replace `map` by `other`
        mem::swap(map, &mut other);
        return;
    };
    println!("other {:?} | smaller {:?}", other, smaller);
    // at this point, the first key of `other` is at least a large as the first key of `map`

    // create a peekable iterator of `other`
    let mut other_iter = other.into_iter();

    // hack to get around mutable usage in loop limitation as of rust 1.40 (as seen in `rio`)
    let mut mc_holder = Some(map_iter.next());
    let mut oc_holder = Some(other_iter.next());

    // create vec where we'll store all entries that can't be inserted when interating `map`
    let mut extras = Vec::new();

    loop {
        // get current pointers from their holders
        let map_current = mc_holder.unwrap();
        let other_current = oc_holder.unwrap();

        println!(
            "map current {:?} | other current {:?} | extras {:?}",
            map_current, other_current, extras
        );

        let (map_current, other_current) = match (map_current, other_current) {
            (Some((map_key, map_value)), Some((other_key, other_value))) => {
                match map_key.cmp(&other_key) {
                    Ordering::Equal => {
                        // merge values
                        merge_fun(map_value, other_value);
                        // advance both iterators
                        (map_iter.next(), other_iter.next())
                    }
                    Ordering::Greater => {
                        // save entry
                        let entry = (other_key, other_value);
                        extras.push(entry);
                        // only advance `other` iterator
                        let map_current = (map_key, map_value);
                        (Some(map_current), other_iter.next())
                    }
                    Ordering::Less => {
                        // only advance `map` iterator
                        let other_current = (other_key, other_value);
                        (map_iter.next(), Some(other_current))
                    }
                }
            }
            (None, Some(entry)) => {
                // the current key from `other` is the first key from `other` that is larger than
                // the larger key in `map`:
                // - save entry and break out of the loop
                extras.push(entry);
                break;
            }
            (_, None) => {
                // in the remaining cases (some, none) and (none, none) as we have incorporated all
                // values from `other`
                break;
            }
        };

        // put back pointers
        mc_holder = Some(map_current);
        oc_holder = Some(other_current);
    }

    // extend `map` with:
    // - `smaller`: values from `other` that are smaller than the smaller key in `map`
    // - `extras`: values from `other` that are larger than the smaller and smaller than the larger
    //   but are not in `map`
    // - `other`: values from `other` that are larger than the larger key in `map`
    map.extend(smaller);
    map.extend(extras);
    map.extend(other_iter);
}

#[cfg(test)]
mod proptests {
    use super::*;
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
        println!("input: {:?} {:?}", map, other);
        let expected = naive_merge(map.clone(), other.clone());
        merge(&mut map, other, merge_fun);
        println!("expected: {:?}", expected);
        println!("result: {:?}", map);
        map == expected
    }
}
