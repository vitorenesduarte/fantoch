use crate::id::ProcessId;
use crate::planet::{Planet, Region};
use std::collections::HashMap;

// Debug version
#[cfg(debug_assertions)]
#[macro_export]
macro_rules! log {
    ($( $args:expr ),*) => { println!( $( $args ),* ); }
}

// Non-debug version
#[cfg(not(debug_assertions))]
#[macro_export]
macro_rules! log {
    ($( $args:expr ),*) => {
        ()
    };
}

/// Returns an iterator with all process identifiers in a system with `n` processes.
pub fn process_ids(n: usize) -> impl Iterator<Item = ProcessId> {
    // compute process identifiers, making sure ids are non-zero
    (1..=n).map(|id| id as u64)
}

/// Updates the processes known by this process.
pub fn sort_processes_by_distance(
    region: &Region,
    planet: &Planet,
    processes: &mut Vec<(ProcessId, Region)>,
) {
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

    // use the region order index (based on distance) to order `processes`
    // - if two `processes` are from the same region, they're sorted by id
    processes.sort_unstable_by(|(id_a, a), (id_b, b)| {
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
    fn process_ids_test() {
        let n = 3;
        assert_eq!(process_ids(n).collect::<Vec<_>>(), vec![1, 2, 3]);

        let n = 5;
        assert_eq!(process_ids(n).collect::<Vec<_>>(), vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn sort_processes_by_distance_test() {
        // processes
        let mut processes = vec![
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

        // sort processes
        let region = Region::new("europe-west3");
        let planet = Planet::new("latency/");
        sort_processes_by_distance(&region, &planet, &mut processes);

        // get only processes ids
        let process_ids: Vec<_> = processes
            .into_iter()
            .map(|(process_id, _)| process_id)
            .collect();

        assert_eq!(
            vec![8, 9, 6, 7, 5, 14, 10, 13, 12, 15, 16, 11, 1, 0, 4, 3, 2],
            process_ids
        );
    }
}
