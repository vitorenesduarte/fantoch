use crate::base::ProcId;
use crate::planet::{Planet, Region};
use std::collections::HashMap;

/// Zips two `Option`s.
pub fn option_zip<L, R>(left: Option<L>, right: Option<R>) -> Option<(L, R)> {
    match (left, right) {
        (Some(left), Some(right)) => Some((left, right)),
        _ => None
    }
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

    #[test]
    fn sort_procs_by_distance_test() {
        // procs
        let mut procs = vec![
            (0, Region::new("asia-east1")),
            (1, Region::new("asia-northeast1")),
            (2, Region::new("asia-south1")),
            (3, Region::new("asia-southeast1")),
            (4, Region::new("australia-southeast1")),
            (5, Region::new("europe-north1")),
            (6, Region::new("europe-west1")),
            (7, Region::new("europe-west2")),
            (8, Region::new("europe-west3")),
            (9, Region::new("europe-west4")),
            (10, Region::new("northamerica-northeast1")),
            (11, Region::new("southamerica-east1")),
            (12, Region::new("us-central1")),
            (13, Region::new("us-east1")),
            (14, Region::new("us-east4")),
            (15, Region::new("us-west1")),
            (16, Region::new("us-west2")),
        ];

        // sort procs
        let region = Region::new("europe-west3");
        let planet = Planet::new("latency/");
        sort_procs_by_distance(&region, &planet, &mut procs);

        // get only processes ids
        let proc_ids: Vec<_> = procs.into_iter().map(|(proc_id, _)| proc_id).collect();

        assert_eq!(
            vec![8, 9, 6, 7, 5, 14, 10, 13, 12, 15, 16, 11, 1, 0, 4, 3, 2],
            proc_ids
        );
    }
}
