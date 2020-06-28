use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Default, Clone, PartialEq, Deserialize, Serialize)]
pub struct ClientData {
    // raw values: we have "100%" precision as all values are stored
    data: HashMap<u64, Vec<u64>>,
}

impl ClientData {
    /// Creates an empty `ClientData`.
    pub fn new() -> Self {
        Self::default()
    }

    // /// Creates an histogram from a list of values.
    // pub fn from<T: IntoIterator<Item = u64>>(values: T) -> Self {
    //     let mut stats = Self::new();
    //     values.into_iter().for_each(|value| stats.increment(value));
    //     stats
    // }

    // /// Create string with values in the histogram.
    // pub fn all_values(&self) -> String {
    //     self.values
    //         .iter()
    //         .map(|(value, count)| format!("{} {}\n", value, count))
    //         .collect()
    // }

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
