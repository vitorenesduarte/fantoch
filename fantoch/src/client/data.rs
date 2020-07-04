use crate::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Clone, PartialEq, Deserialize, Serialize)]
pub struct ClientData {
    // raw values: we have "100%" precision as all values are stored
    // - mapping from operation end time to all latencies registered at that
    //   end time
    data: HashMap<u64, Vec<u64>>,
}

impl ClientData {
    /// Creates an empty `ClientData`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Merges two histograms.
    pub fn merge(&mut self, other: &Self) {
        data_merge(&mut self.data, &other.data)
    }

    /// Records a more mata.
    pub fn record(&mut self, latency: u64, end_time: u64) {
        let latencies = self.data.entry(end_time).or_insert_with(Vec::new);
        latencies.push(latency);
    }

    pub fn latency_data(&self) -> impl Iterator<Item = u64> + '_ {
        self.data.values().flat_map(|v| v.into_iter()).cloned()
    }

    pub fn throughput_data(&self) -> impl Iterator<Item = (u64, usize)> + '_ {
        self.data
            .iter()
            .map(|(time, latencies)| (time.clone(), latencies.len()))
    }

    /// Compute start and end times for this client.
    pub fn start_and_end(&self) -> Option<(u64, u64)> {
        let mut times: Vec<_> = self.data.keys().collect();
        times.sort();
        times.first().map(|first| {
            // if there's a first, there's a last
            let last = times.last().unwrap();
            (**first, **last)
        })
    }

    /// Prune events that are before `start` or after `end`.
    pub fn prune(&mut self, start: u64, end: u64) {
        self.data.retain(|&time, _| {
            // retain if within the given bounds
            time >= start && time <= end
        })
    }
}

pub fn data_merge(
    map: &mut HashMap<u64, Vec<u64>>,
    other: &HashMap<u64, Vec<u64>>,
) {
    other.into_iter().for_each(|(k, v2)| match map.get_mut(&k) {
        Some(v1) => {
            v1.extend(v2);
        }
        None => {
            map.entry(k.clone()).or_insert(v2.clone());
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_data_test() {
        let mut data = ClientData::new();
        assert_eq!(data.start_and_end(), None);
        // at time 10, an operation with latency 1 ended
        data.record(1, 10);
        assert_eq!(data.start_and_end(), Some((10, 10)));

        // at time 10, another operation but with latency 2 ended
        data.record(2, 10);
        assert_eq!(data.start_and_end(), Some((10, 10)));

        // at time 11, an operation with latency 5 ended
        data.record(5, 11);
        assert_eq!(data.start_and_end(), Some((10, 11)));

        // check latency and throughput data
        let mut latency: Vec<_> = data.latency_data().collect();
        latency.sort();
        assert_eq!(latency, vec![1, 2, 5]);
        let mut throughput: Vec<_> = data.throughput_data().collect();
        throughput.sort();
        assert_eq!(throughput, vec![(10, 2), (11, 1)]);

        // check merge
        let mut other = ClientData::new();
        // at time 2, an operation with latency 5 ended
        other.record(5, 2);

        data.merge(&other);
        assert_eq!(data.start_and_end(), Some((2, 11)));
        let mut latency: Vec<_> = data.latency_data().collect();
        latency.sort();
        assert_eq!(latency, vec![1, 2, 5, 5]);
        let mut throughput: Vec<_> = data.throughput_data().collect();
        throughput.sort();
        assert_eq!(throughput, vec![(2, 1), (10, 2), (11, 1)]);

        // check prune: if all events are within bounds, then no pruning happens
        data.prune(1, 20);
        let mut throughput: Vec<_> = data.throughput_data().collect();
        throughput.sort();
        assert_eq!(throughput, vec![(2, 1), (10, 2), (11, 1)]);

        // prune event 2 out
        data.prune(5, 20);
        let mut throughput: Vec<_> = data.throughput_data().collect();
        throughput.sort();
        assert_eq!(throughput, vec![(10, 2), (11, 1)]);

        // prune event 10 out
        data.prune(11, 20);
        let mut throughput: Vec<_> = data.throughput_data().collect();
        throughput.sort();
        assert_eq!(throughput, vec![(11, 1)]);

        // prune event 11 out
        data.prune(15, 20);
        let mut throughput: Vec<_> = data.throughput_data().collect();
        throughput.sort();
        assert_eq!(throughput, vec![]);
    }
}
