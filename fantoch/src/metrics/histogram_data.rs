use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::Hash;

#[derive(Debug, Default, Clone, PartialEq, Deserialize, Serialize)]
pub struct HistogramData {
    // raw values: we have "100%" precision as all values are stored
    values: HashMap<u64, usize>,
}

impl HistogramData {
    /// Creates an empty histogram.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an histogram from a list of values.
    pub fn from<T: IntoIterator<Item = u64>>(values: T) -> Self {
        let mut stats = Self::new();
        values.into_iter().for_each(|value| stats.increment(value));
        stats
    }

    /// Create string with values in the histogram.
    pub fn all_values(&self) -> String {
        self.values
            .iter()
            .map(|(value, count)| format!("{} {}\n", value, count))
            .collect()
    }

    /// Merges two histograms.
    pub fn merge(&mut self, other: &Self) {
        histogram_merge(&mut self.values, &other.values)
    }

    /// Increments the occurrence of some value in the histogram.
    pub fn increment(&mut self, value: u64) {
        // register another occurrence of `value`
        let count = self.values.entry(value).or_insert(0);
        *count += 1;
    }
}

pub fn histogram_merge<K>(
    map: &mut HashMap<K, usize>,
    other: &HashMap<K, usize>,
) where
    K: Hash + Eq + Clone,
{
    other.into_iter().for_each(|(k, v)| match map.get_mut(&k) {
        Some(m) => {
            *m += *v;
        }
        None => {
            map.entry(k.clone()).or_insert(*v);
        }
    });
}
