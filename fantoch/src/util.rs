use crate::id::{Dot, ProcessId, ShardId};
use crate::kvs::Key;
use crate::planet::{Planet, Region};
use crate::HashMap;
use std::hash::{Hash, Hasher};

/// create a singleton hash set
#[macro_export]
macro_rules! singleton {
    ( $x:expr ) => {{
        let mut set = HashSet::with_capacity(1);
        set.insert($x);
        set
    }};
}

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

#[macro_export]
macro_rules! elapsed {
    ( $x:expr ) => {{
        use std::time::Instant;
        let start = Instant::now();
        let result = $x;
        let time = start.elapsed();
        (time, result)
    }};
}

type DefaultHasher = ahash::AHasher;

/// Compute the hash of a key.
#[allow(clippy::ptr_arg)]
pub fn key_hash(key: &Key) -> u64 {
    let mut hasher = DefaultHasher::default();
    key.hash(&mut hasher);
    hasher.finish()
}

/// Returns an iterator with all process identifiers in a system with `n`
/// processes.
pub fn process_ids(
    shard_id: ShardId,
    n: usize,
) -> impl Iterator<Item = ProcessId> {
    // compute process identifiers, making sure ids are non-zero
    (1..=n).map(move |id| (id * (shard_id as usize + 1)) as ProcessId)
}

/// Converts a reprentation of dots to the actual dots.
pub fn dots(repr: Vec<(ProcessId, u64, u64)>) -> impl Iterator<Item = Dot> {
    repr.into_iter().flat_map(|(process_id, start, end)| {
        (start..=end).map(move |event| Dot::new(process_id, event))
    })
}

/// Updates the processes known by this process.
pub fn sort_processes_by_distance(
    region: &Region,
    planet: &Planet,
    mut processes: Vec<(ProcessId, ShardId, Region)>,
) -> Vec<(ProcessId, ShardId)> {
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
    processes.sort_unstable_by(|(id_a, _, a), (id_b, _, b)| {
        if a == b {
            id_a.cmp(id_b)
        } else {
            let index_a = indexes.get(a).expect("region should exist");
            let index_b = indexes.get(b).expect("region should exist");
            index_a.cmp(index_b)
        }
    });

    processes
        .into_iter()
        .map(|(id, shard_id, _)| (id, shard_id))
        .collect()
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[test]
    fn process_ids_test() {
        let n = 3;
        assert_eq!(process_ids(n, 0).collect::<Vec<_>>(), vec![1, 2, 3]);
        assert_eq!(process_ids(n, 1).collect::<Vec<_>>(), vec![4, 5, 6]);
        assert_eq!(process_ids(n, 3).collect::<Vec<_>>(), vec![10, 11, 12]);

        let n = 5;
        assert_eq!(process_ids(n, 0).collect::<Vec<_>>(), vec![1, 2, 3, 4, 5]);
        assert_eq!(
            process_ids(n, 2).collect::<Vec<_>>(),
            vec![11, 12, 13, 14, 15]
        );
    }

    #[test]
    fn sort_processes_by_distance_test() {
        // processes
        let processes = vec![
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

        let shard_id = 0;
        // map them all to the same shard
        let processes = processes
            .into_iter()
            .map(|(process_id, region)| (process_id, shard_id, region))
            .collect();

        // sort processes
        let region = Region::new("europe-west3");
        let planet = Planet::new();
        let sorted = sort_processes_by_distance(&region, &planet, processes);

        let expected =
            vec![8, 9, 6, 7, 5, 14, 10, 13, 12, 15, 16, 11, 1, 0, 4, 3, 2];
        // map them all to the same shard
        let expected: Vec<_> = expected
            .into_iter()
            .map(|process_id| (process_id, shard_id))
            .collect();

        assert_eq!(expected, sorted);
    }
}
