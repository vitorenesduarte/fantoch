use crate::id::{Dot, ProcessId, ShardId};
use crate::kvs::Key;
use crate::planet::{Planet, Region};
use crate::HashMap;
use std::hash::{Hash, Hasher};

// warn and info logs are always enabled!
// - debug is enabled if `max_level_debug` or `max_level_trace`
// - trace is enabled if `max_level_trace`

#[macro_export]
macro_rules! warn {
    ($( $args:expr ),*) => { tracing::warn!( $( $args ),* ) }
}

#[macro_export]
macro_rules! info {
    ($( $args:expr ),*) => { tracing::info!( $( $args ),* ) }
}

#[macro_export]
#[cfg(any(feature = "max_level_debug", feature = "max_level_trace"))]
macro_rules! debug {
    ($( $args:expr ),*) => { tracing::debug!( $( $args ),* ) }
}

#[macro_export]
#[cfg(not(any(feature = "max_level_debug", feature = "max_level_trace")))]
macro_rules! debug {
    ($( $args:expr ),*) => {
        ()
        // println!( $( $args ), *)
    };
}

#[macro_export]
#[cfg(feature = "max_level_trace")]
macro_rules! trace {
    ($( $args:expr ),*) => { tracing::trace!( $( $args ),* ) }
}

#[macro_export]
#[cfg(not(feature = "max_level_trace"))]
macro_rules! trace {
    ($( $args:expr ),*) => {
        ()
        // println!( $( $args ), *)
    };
}

/// create a singleton hash set
#[macro_export]
macro_rules! singleton {
    ( $x:expr ) => {{
        let mut set = HashSet::with_capacity(1);
        set.insert($x);
        set
    }};
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

#[must_use]
pub fn init_tracing_subscriber(
    log_file: Option<impl AsRef<std::path::Path> + std::fmt::Debug>,
    tracing_directives: Option<&'static str>,
) -> tracing_appender::non_blocking::WorkerGuard {
    // create log format
    let format = tracing_subscriber::fmt::format()
        .without_time()
        .with_target(false)
        .with_level(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_ansi(false);

    // create env filter
    let env_filter = match tracing_directives {
        Some(dirs) => tracing_subscriber::EnvFilter::new(dirs),
        None => tracing_subscriber::EnvFilter::from_default_env(),
    };

    println!("log_file: {:?}", log_file);
    println!("env_filter: {}", env_filter);

    // create writer
    let builder = tracing_appender::non_blocking::NonBlockingBuilder::default()
        .lossy(false);
    let (non_blocking_appender, guard) = match log_file {
        Some(log_file) => {
            builder.finish(tracing_appender::rolling::never(".", log_file))
        }
        None => builder.finish(std::io::stdout()),
    };

    tracing_subscriber::fmt()
        .event_format(format)
        .with_writer(non_blocking_appender)
        .with_env_filter(env_filter)
        .init();

    guard
}

type DefaultHasher = ahash::AHasher;

/// Compute the hash of a key.
#[allow(clippy::ptr_arg)]
pub fn key_hash(key: &Key) -> u64 {
    let mut hasher = DefaultHasher::default();
    key.hash(&mut hasher);
    hasher.finish()
}

/// Returns an iterator with all process identifiers in this shard in a system
/// with `n` processes.
pub fn process_ids(
    shard_id: ShardId,
    n: usize,
) -> impl Iterator<Item = ProcessId> {
    // compute process identifiers, making sure ids are non-zero
    let shift = n * shard_id as usize;
    (1..=n).map(move |id| (id + shift) as ProcessId)
}

pub fn all_process_ids(
    shard_count: usize,
    n: usize,
) -> impl Iterator<Item = (ProcessId, ShardId)> {
    (0..shard_count).flat_map(move |shard_id| {
        let shard_id = shard_id as ShardId;
        process_ids(shard_id, n).map(move |process_id| (process_id, shard_id))
    })
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

/// Returns a mapping from shard id to the closest process on that shard.
pub fn closest_process_per_shard(
    region: &Region,
    planet: &Planet,
    processes: Vec<(ProcessId, ShardId, Region)>,
) -> HashMap<ShardId, ProcessId> {
    let sorted = sort_processes_by_distance(region, planet, processes);
    let mut processes = HashMap::new();
    for (process_id, shard_id) in sorted {
        if !processes.contains_key(&shard_id) {
            processes.insert(shard_id, process_id);
        }
    }
    processes
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[test]
    fn process_ids_test() {
        let n = 3;
        assert_eq!(process_ids(0, n).collect::<Vec<_>>(), vec![1, 2, 3]);
        assert_eq!(process_ids(1, n).collect::<Vec<_>>(), vec![4, 5, 6]);
        assert_eq!(process_ids(3, n).collect::<Vec<_>>(), vec![10, 11, 12]);

        let n = 5;
        assert_eq!(process_ids(0, n).collect::<Vec<_>>(), vec![1, 2, 3, 4, 5]);
        assert_eq!(
            process_ids(2, n).collect::<Vec<_>>(),
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
