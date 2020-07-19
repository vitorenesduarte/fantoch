use crate::metrics::F64;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;

pub enum Stats {
    Mean,
    COV,  // coefficient of variation
    MDTM, // mean distance to mean
}

// TODO maybe use https://docs.rs/hdrhistogram/7.0.0/hdrhistogram/
#[derive(Default, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Histogram {
    // raw values: we have "100%" precision as all values are stored
    values: BTreeMap<u64, usize>,
}

impl Histogram {
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
    pub fn values(&self) -> impl Iterator<Item = u64> + '_ {
        self.values
            .iter()
            .flat_map(|(value, count)| (0..*count).map(move |_| *value))
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

    pub fn mean(&self) -> F64 {
        let (mean, _) = self.compute_mean_and_count();
        F64::new(mean)
    }

    pub fn stddev(&self) -> F64 {
        let (mean, count) = self.compute_mean_and_count();
        let stddev = self.compute_stddev(mean, count);
        F64::new(stddev)
    }

    pub fn cov(&self) -> F64 {
        let cov = self.compute_cov();
        F64::new(cov)
    }

    pub fn mdtm(&self) -> F64 {
        let mdtm = self.compute_mdtm();
        F64::new(mdtm)
    }

    pub fn mean_improv(&self, other: &Self) -> F64 {
        self.mean() - other.mean()
    }

    pub fn cov_improv(&self, other: &Self) -> F64 {
        self.cov() - other.cov()
    }

    pub fn mdtm_improv(&self, other: &Self) -> F64 {
        self.mdtm() - other.mdtm()
    }

    pub fn min(&self) -> F64 {
        self.values
            .iter()
            .next()
            .map(|(min, _)| F64::new(*min as f64))
            .unwrap_or_else(F64::nan)
    }

    pub fn max(&self) -> F64 {
        self.values
            .iter()
            .next_back()
            .map(|(min, _)| F64::new(*min as f64))
            .unwrap_or_else(F64::nan)
    }

    // Computes a given percentile.
    pub fn percentile(&self, percentile: f64) -> F64 {
        assert!(percentile >= 0.0 && percentile <= 1.0);

        if self.values.is_empty() {
            return F64::zero();
        }

        // compute the number of elements in the histogram
        let count = self.count() as f64;
        let index = percentile * count;
        let index_rounded = index.round();
        // check if index is a whole number
        let is_whole_number = (index - index_rounded).abs() == 0.0;

        // compute final index
        let mut index = index_rounded as usize;

        // create data iterator of values in the histogram
        let mut data = self.values.iter();

        // compute left and right value that will be used to compute the
        // percentile
        let left_value;
        let right_value;

        loop {
            let (value, count) =
                data.next().expect("there should a next histogram value");

            match index.cmp(&count) {
                Ordering::Equal => {
                    // if it's the same, this is the left value and the next
                    // histogram value is the right value
                    left_value = *value as f64;
                    right_value = data.next().map(|(value, _)| *value as f64);
                    break;
                }
                Ordering::Less => {
                    // if index is smaller, this value is both the left and the
                    // right value
                    left_value = *value as f64;
                    right_value = Some(left_value);
                    break;
                }
                Ordering::Greater => {
                    // if greater, keep going
                    index -= count;
                }
            }
        }

        let value = if is_whole_number {
            (left_value + right_value.expect("there should be a right value"))
                / 2.0
        } else {
            left_value
        };

        F64::new(value)
    }

    fn compute_mean_and_count(&self) -> (f64, f64) {
        let (sum, count) = self.sum_and_count();
        // cast them to floats
        let sum = sum as f64;
        let count = count as f64;
        // compute mean
        let mean = sum / count;
        (mean, count)
    }

    fn sum_and_count(&self) -> (u64, usize) {
        self.values.iter().fold(
            (0, 0),
            |(sum_acc, count_acc), (value, count)| {
                // compute the actual sum for this value
                let sum = value * (*count as u64);
                (sum_acc + sum, count_acc + count)
            },
        )
    }

    fn count(&self) -> usize {
        self.values.iter().map(|(_, count)| count).sum::<usize>()
    }

    fn compute_cov(&self) -> f64 {
        let (mean, count) = self.compute_mean_and_count();
        let stddev = self.compute_stddev(mean, count);
        stddev / mean
    }

    fn compute_stddev(&self, mean: f64, count: f64) -> f64 {
        let variance = self.compute_variance(mean, count);
        variance.sqrt()
    }

    fn compute_variance(&self, mean: f64, count: f64) -> f64 {
        let sum = self
            .values
            .iter()
            .map(|(x, x_count)| (*x as f64, *x_count as f64))
            .map(|(x, x_count)| {
                let diff = mean - x;
                // as `x` was reported `x_count` times, we multiply the squared
                // diff by it
                (diff * diff) * x_count
            })
            .sum::<f64>();
        // we divide by (count - 1) to have the corrected version of variance
        // - https://en.wikipedia.org/wiki/Standard_deviation#Corrected_sample_standard_deviation
        sum / (count - 1.0)
    }

    fn compute_mdtm(&self) -> f64 {
        let (mean, count) = self.compute_mean_and_count();
        let distances_sum = self
            .values
            .iter()
            .map(|(x, x_count)| (*x as f64, *x_count as f64))
            .map(|(x, x_count)| {
                let diff = mean - x;
                // as `x` was reported `x_count` times, we multiply the absolute
                // value by it
                diff.abs() * x_count
            })
            .sum::<f64>();
        distances_sum / count
    }
}

impl fmt::Debug for Histogram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "min={:<6} max={:<6} avg={:<6} p95={:<6} p99={:<6} p99.9={:<6} p99.99={:<6}",
            self.min().value().round(),
            self.max().value().round(),
            self.mean().value().round(),
            self.percentile(0.95).value().round(),
            self.percentile(0.99).value().round(),
            self.percentile(0.999).value().round(),
            self.percentile(0.9999).value().round()
        )
    }
}

pub fn histogram_merge<K>(
    map: &mut BTreeMap<K, usize>,
    other: &BTreeMap<K, usize>,
) where
    K: Ord + Eq + Clone,
{
    // create iterators for `map` and `other`
    let mut map_iter = map.iter_mut();
    let mut other_iter = other.iter();

    // variables to hold the "current value" of each iterator
    let mut map_current = map_iter.next();
    let mut other_current = other_iter.next();

    // create vec where we'll store all entries with keys that are in `map`, are
    // smaller than the larger key in `map`, but can't be inserted when
    // interating `map`
    let mut absent = Vec::new();

    loop {
        match (map_current, other_current) {
            (Some((map_key, map_value)), Some((other_key, other_value))) => {
                match map_key.cmp(&other_key) {
                    Ordering::Less => {
                        // simply advance `map` iterator
                        map_current = map_iter.next();
                        other_current = Some((other_key, other_value));
                    }
                    Ordering::Greater => {
                        // save entry to added later
                        absent.push((other_key, other_value));
                        // advance `other` iterator
                        map_current = Some((map_key, map_value));
                        other_current = other_iter.next();
                    }
                    Ordering::Equal => {
                        // merge values
                        *map_value += other_value;
                        // advance both iterators
                        map_current = map_iter.next();
                        other_current = other_iter.next();
                    }
                }
            }
            (None, Some(entry)) => {
                // the key in `entry` is the first key from `other` that is
                // larger than the larger key in `map`; save
                // entry and break out of the loop
                absent.push(entry);
                break;
            }
            (_, None) => {
                // there's nothing else to do here as in these (two) cases we
                // have already incorporated all entries from
                // `other`
                break;
            }
        };
    }

    // extend `map` with keys from `other` that are not in `map`:
    // - `absent`: keys from `other` that are smaller than the larger key in
    //   `map`
    // - `other_iter`: keys from `other` that are larger than the larger key in
    //   `map`
    map.extend(absent.into_iter().map(|(key, value)| (key.clone(), *value)));
    map.extend(other_iter.map(|(key, value)| (key.clone(), *value)));
}

#[cfg(test)]
mod proptests {
    use super::*;
    use crate::elapsed;
    use crate::HashMap;
    use quickcheck_macros::quickcheck;
    use std::hash::Hash;
    use std::iter::FromIterator;

    fn hash_merge<K>(map: &mut HashMap<K, usize>, other: &HashMap<K, usize>)
    where
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

    type K = u64;

    #[quickcheck]
    fn merge_check(map: Vec<(K, usize)>, other: Vec<(K, usize)>) -> bool {
        // create hashmaps and merge them
        let mut hashmap = HashMap::from_iter(map.clone());
        let other_hashmap = HashMap::from_iter(other.clone());
        let (naive_time, _) =
            elapsed!(hash_merge(&mut hashmap, &other_hashmap));

        // create btreemaps and merge them
        let mut btreemap = BTreeMap::from_iter(map.clone());
        let other_btreemap = BTreeMap::from_iter(other.clone());
        let (time, _) =
            elapsed!(histogram_merge(&mut btreemap, &other_btreemap));

        // show merge times
        println!("{} {}", naive_time.as_nanos(), time.as_nanos());

        btreemap == BTreeMap::from_iter(hashmap)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats() {
        let stats = Histogram::from(vec![1, 1, 1]);
        assert_eq!(stats.mean(), F64::new(1.0));
        assert_eq!(stats.cov(), F64::new(0.0));
        assert_eq!(stats.mdtm(), F64::new(0.0));
        assert_eq!(stats.min(), F64::new(1.0));
        assert_eq!(stats.max(), F64::new(1.0));

        let stats = Histogram::from(vec![10, 20, 30]);
        assert_eq!(stats.mean(), F64::new(20.0));
        assert_eq!(stats.cov(), F64::new(0.5));
        assert_eq!(stats.min(), F64::new(10.0));
        assert_eq!(stats.max(), F64::new(30.0));

        let stats = Histogram::from(vec![10, 20]);
        assert_eq!(stats.mean(), F64::new(15.0));
        assert_eq!(stats.mdtm(), F64::new(5.0));
        assert_eq!(stats.min(), F64::new(10.0));
        assert_eq!(stats.max(), F64::new(20.0));
    }

    #[test]
    fn stats_show() {
        let stats = Histogram::from(vec![1, 1, 1]);
        assert_eq!(stats.mean().round(), "1.0");
        assert_eq!(stats.cov().round(), "0.0");
        assert_eq!(stats.mdtm().round(), "0.0");

        let stats = Histogram::from(vec![10, 20, 30]);
        assert_eq!(stats.mean().round(), "20.0");
        assert_eq!(stats.cov().round(), "0.5");
        assert_eq!(stats.mdtm().round(), "6.7");

        let stats = Histogram::from(vec![10, 20]);
        assert_eq!(stats.mean().round(), "15.0");
        assert_eq!(stats.cov().round(), "0.5");
        assert_eq!(stats.mdtm().round(), "5.0");

        let stats = Histogram::from(vec![10, 20, 40, 10]);
        assert_eq!(stats.mean().round(), "20.0");
        assert_eq!(stats.cov().round(), "0.7");
        assert_eq!(stats.mdtm().round(), "10.0");
    }

    #[test]
    fn stats_improv() {
        let stats_a = Histogram::from(vec![1, 1, 1]);
        let stats_b = Histogram::from(vec![10, 20]);
        assert_eq!(stats_a.mean_improv(&stats_b), F64::new(-14.0));

        let stats_a = Histogram::from(vec![1, 1, 1]);
        let stats_b = Histogram::from(vec![10, 20, 30]);
        assert_eq!(stats_a.cov_improv(&stats_b), F64::new(-0.5));

        let stats_a = Histogram::from(vec![1, 1, 1]);
        let stats_b = Histogram::from(vec![10, 20]);
        assert_eq!(stats_a.mdtm_improv(&stats_b), F64::new(-5.0));
    }

    #[test]
    fn percentile() {
        let data = vec![
            43, 54, 56, 61, 62, 66, 68, 69, 69, 70, 71, 72, 77, 78, 79, 85, 87,
            88, 89, 93, 95, 96, 98, 99, 99,
        ];
        let stats = Histogram::from(data);

        assert_eq!(stats.min(), F64::new(43.0));
        assert_eq!(stats.max(), F64::new(99.0));
        assert_eq!(stats.percentile(0.9), F64::new(98.0));
        assert_eq!(stats.percentile(0.5), F64::new(77.0));
        assert_eq!(stats.percentile(0.2), F64::new(64.0));
    }
}
