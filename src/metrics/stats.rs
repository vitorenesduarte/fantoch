use crate::metrics::{btree, F64};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;

pub enum StatsKind {
    Mean,
    COV,  // coefficient of variation
    MDTM, // mean distance to mean
}

#[derive(Default, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub struct Stats {
    // raw values: we have "100%" precision as all values are stored
    values: BTreeMap<u64, usize>,
}

impl Stats {
    /// Creates an empty histogram.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an histogram from a list of values.
    pub fn from(values: Vec<u64>) -> Self {
        let mut stats = Self::new();
        values.into_iter().for_each(|value| stats.increment(value));
        stats
    }

    /// Merges two histograms.
    pub fn merge(&mut self, other: Self) {
        btree::merge(&mut self.values, other.values, Self::merge_count);
    }

    /// Increments the occurrence of some value in the histogram.
    pub fn increment(&mut self, value: u64) {
        // register another occurrence of `value`
        let mut count = self.values.entry(value).or_insert(0);
        Self::merge_count(&mut count, 1);
    }

    pub fn mean(&self) -> F64 {
        let (mean, _) = self.compute_mean_and_count();
        F64::new(mean)
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

    // Computes a given percentile.
    pub fn percentile(&self, percentile: f64) -> F64 {
        assert!(percentile >= 0.0 && percentile <= 1.0);

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

        // compute left and right value that will be used to compute the percentile
        let left_value;
        let right_value;

        loop {
            let (value, count) = data.next().expect("there should a next histogram value");

            match index.cmp(&count) {
                Ordering::Equal => {
                    // if it's the same, this is the left value and the next histogram value is the
                    // right value
                    left_value = *value as f64;
                    right_value = data.next().map(|(value, _)| *value as f64);
                    break;
                }
                Ordering::Less => {
                    // if index is smaller, this value is both the left and the right value
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
            (left_value + right_value.expect("there should be a right value")) / 2.0
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
        self.values
            .iter()
            .fold((0, 0), |(sum_acc, count_acc), (value, count)| {
                // compute the actual sum for this value
                let sum = value * (*count as u64);
                (sum_acc + sum, count_acc + count)
            })
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
                // as `x` was reported `x_count` times, we multiply the squared diff by it
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
                // as `x` was reported `x_count` times, we multiply the absolute value by it
                diff.abs() * x_count
            })
            .sum::<f64>();
        distances_sum / count
    }

    fn merge_count(value: &mut usize, other: usize) {
        *value += other;
    }
}

impl fmt::Debug for Stats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({:.0}, {:.2})", self.mean().value(), self.cov().value())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats() {
        let stats = Stats::from(vec![1, 1, 1]);
        assert_eq!(stats.mean(), F64::new(1.0));
        assert_eq!(stats.cov(), F64::new(0.0));
        assert_eq!(stats.mdtm(), F64::new(0.0));

        let stats = Stats::from(vec![10, 20, 30]);
        assert_eq!(stats.mean(), F64::new(20.0));
        assert_eq!(stats.cov(), F64::new(0.5));

        let stats = Stats::from(vec![10, 20]);
        assert_eq!(stats.mean(), F64::new(15.0));
        assert_eq!(stats.mdtm(), F64::new(5.0));
    }

    #[test]
    fn stats_show() {
        let stats = Stats::from(vec![1, 1, 1]);
        assert_eq!(stats.mean().round(), "1.0");
        assert_eq!(stats.cov().round(), "0.0");
        assert_eq!(stats.mdtm().round(), "0.0");

        let stats = Stats::from(vec![10, 20, 30]);
        assert_eq!(stats.mean().round(), "20.0");
        assert_eq!(stats.cov().round(), "0.5");
        assert_eq!(stats.mdtm().round(), "6.7");

        let stats = Stats::from(vec![10, 20]);
        assert_eq!(stats.mean().round(), "15.0");
        assert_eq!(stats.cov().round(), "0.5");
        assert_eq!(stats.mdtm().round(), "5.0");

        let stats = Stats::from(vec![10, 20, 40, 10]);
        assert_eq!(stats.mean().round(), "20.0");
        assert_eq!(stats.cov().round(), "0.7");
        assert_eq!(stats.mdtm().round(), "10.0");
    }

    #[test]
    fn stats_improv() {
        let stats_a = Stats::from(vec![1, 1, 1]);
        let stats_b = Stats::from(vec![10, 20]);
        assert_eq!(stats_a.mean_improv(&stats_b), F64::new(-14.0));

        let stats_a = Stats::from(vec![1, 1, 1]);
        let stats_b = Stats::from(vec![10, 20, 30]);
        assert_eq!(stats_a.cov_improv(&stats_b), F64::new(-0.5));

        let stats_a = Stats::from(vec![1, 1, 1]);
        let stats_b = Stats::from(vec![10, 20]);
        assert_eq!(stats_a.mdtm_improv(&stats_b), F64::new(-5.0));
    }

    #[test]
    fn percentile() {
        let data = vec![
            43, 54, 56, 61, 62, 66, 68, 69, 69, 70, 71, 72, 77, 78, 79, 85, 87, 88, 89, 93, 95, 96,
            98, 99, 99,
        ];
        let stats = Stats::from(data);

        assert_eq!(stats.percentile(0.9), F64::new(98.0));
        assert_eq!(stats.percentile(0.5), F64::new(77.0));
        assert_eq!(stats.percentile(0.2), F64::new(64.0));
    }
}
