use crate::base::ProcId;
use crate::planet::{Planet, Region};
use std::collections::HashMap;

/// Zips two `Option`s.
pub fn option_zip<L, R>(left: Option<L>, right: Option<R>) -> Option<(L, R)> {
    let left = left.into_iter();
    let right = right.into_iter();
    left.zip(right).next()
}

/// Updates the processes known by this process.
pub fn sort_procs_by_distance(region: &Region, planet: &Planet, procs: &mut Vec<(ProcId, Region)>) {
    // TODO the following computation could be cached on `planet`
    let indexes: HashMap<_, _> = planet
        // get all regions sorted by distance from `region`
        .sorted(region)
        .expect("region should be part of planet")
        .iter()
        // create a mapping from region to its index
        .enumerate()
        .map(|(index, (_distance, region))| (region, index))
        .collect();

    // use the region order index (based on distance) to order `procs`
    // - if two `procs` are from the same region, they're sorted by id
    procs.sort_unstable_by(|(id_a, a), (id_b, b)| {
        if a == b {
            id_a.cmp(id_b)
        } else {
            let index_a = indexes.get(a).expect("region should exist");
            let index_b = indexes.get(b).expect("region should exist");
            index_a.cmp(index_b)
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn option_zip_test() {
        assert_eq!(option_zip::<usize, String>(None, None), None);
        assert_eq!(option_zip::<usize, String>(Some(10), None), None);
        assert_eq!(
            option_zip::<usize, String>(None, Some(String::from("A"))),
            None
        );
        assert_eq!(
            option_zip(Some(10), Some(String::from("A"))),
            Some((10, String::from("A")))
        );
    }
}
