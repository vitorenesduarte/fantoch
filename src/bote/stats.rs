use crate::bote::float::F64;
use crate::bote::protocol::{ClientPlacement, Protocol};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

pub enum StatsSortBy {
    Mean,
    COV,
    MDTM,
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Deserialize, Serialize)]
pub struct Stats {
    mean: F64,
    cov: F64,  // coefficient of variation
    mdtm: F64, // mean distance to mean
}

impl fmt::Debug for Stats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "({:.0}, {:.2}, {:.2})",
            self.mean.value(),
            self.cov.value(),
            self.mdtm.value()
        )
    }
}

impl Stats {
    pub fn from(latencies: &Vec<usize>) -> Self {
        let (mean, cov, mdtm) = Stats::compute_stats(&latencies);
        Stats { mean, cov, mdtm }
    }

    pub fn mean_improv(&self, other: &Self) -> F64 {
        self.mean - other.mean
    }

    pub fn cov_improv(&self, other: &Self) -> F64 {
        self.cov - other.cov
    }

    pub fn mdtm_improv(&self, other: &Self) -> F64 {
        self.mdtm - other.mdtm
    }

    pub fn mean(&self) -> F64 {
        self.mean
    }

    pub fn cov(&self) -> F64 {
        self.cov
    }

    pub fn mdtm(&self) -> F64 {
        self.mdtm
    }

    pub fn show_mean(&self) -> String {
        self.mean().round()
    }

    pub fn show_cov(&self) -> String {
        self.cov().round()
    }

    pub fn show_mdtm(&self) -> String {
        self.mdtm().round()
    }

    fn compute_stats(xs: &Vec<usize>) -> (F64, F64, F64) {
        // transform `usize`s in `f64`s
        let xs: Vec<f64> = xs.into_iter().map(|&x| x as f64).collect();

        // compute mean
        let mean = Self::compute_mean(&xs);

        // compute coefficient of variation
        let cov = Self::compute_cov(&xs, mean);

        // compute mean distance to mean
        let mdtm = Self::compute_mdtm(&xs, mean);

        // return the 3 stats
        (F64::new(mean), F64::new(cov), F64::new(mdtm))
    }

    // from https://rust-lang-nursery.github.io/rust-cookbook/science/mathematics/statistics.html
    fn compute_mean(data: &[f64]) -> f64 {
        let sum = data.into_iter().sum::<f64>();
        let count = data.len() as f64;
        // assumes `count > 0`
        sum / count
    }

    fn compute_cov(data: &[f64], mean: f64) -> f64 {
        let stddev = Self::compute_stddev(data, mean);
        stddev / mean
    }

    fn compute_mdtm(data: &[f64], mean: f64) -> f64 {
        let count = data.len() as f64;
        let distances_sum = data
            .into_iter()
            .map(|x| {
                let diff = mean - x;
                diff.abs()
            })
            .sum::<f64>();
        distances_sum / count
    }

    fn compute_stddev(data: &[f64], mean: f64) -> f64 {
        let variance = Self::compute_variance(data, mean);
        variance.sqrt()
    }

    fn compute_variance(data: &[f64], mean: f64) -> f64 {
        let count = data.len() as f64;
        let sum = data
            .into_iter()
            .map(|x| {
                let diff = mean - x;
                diff * diff
            })
            .sum::<f64>();
        // we divide by (count - 1) to have the corrected version of variance
        // - https://en.wikipedia.org/wiki/Standard_deviation#Corrected_sample_standard_deviation
        sum / (count - 1.0)
    }
}

/// Mapping from protocol name to its stats.
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Deserialize, Serialize)]
pub struct AllStats(BTreeMap<String, Stats>);

impl AllStats {
    pub fn new() -> AllStats {
        AllStats(BTreeMap::new())
    }

    pub fn get(
        &self,
        protocol: Protocol,
        f: usize,
        placement: ClientPlacement,
    ) -> &Stats {
        let key = Self::key(protocol, f, placement);
        self.get_and_check_unwrap(&key)
    }

    fn get_and_check_unwrap(&self, key: &String) -> &Stats {
        let stats = self.0.get(key);
        assert!(stats.is_some(), "stats with key {} not found", key);
        stats.unwrap()
    }

    pub fn insert(
        &mut self,
        protocol: Protocol,
        f: usize,
        placement: ClientPlacement,
        stats: Stats,
    ) {
        let key = Self::key(protocol, f, placement);
        self.0.insert(key, stats);
    }

    pub fn fmt(
        &self,
        protocol: Protocol,
        f: usize,
        placement: ClientPlacement,
    ) -> String {
        let key = Self::key(protocol, f, placement);
        let stats = self.get_and_check_unwrap(&key);
        format!("{}={:?}", key, stats)
    }

    fn key(protocol: Protocol, f: usize, placement: ClientPlacement) -> String {
        let prefix = match protocol {
            Protocol::EPaxos => String::from(protocol.short_name()),
            _ => format!("{}f{}", protocol.short_name(), f),
        };
        let suffix = placement.short_name();
        format!("{}{}", prefix, suffix)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn stats() {
        let stats = Stats::from(&vec![1, 1, 1]);
        assert_eq!(stats.mean(), F64::new(1.0));
        assert_eq!(stats.cov(), F64::new(0.0));
        assert_eq!(stats.mdtm(), F64::new(0.0));

        let stats = Stats::from(&vec![10, 20, 30]);
        assert_eq!(stats.mean(), F64::new(20.0));
        assert_eq!(stats.cov(), F64::new(0.5));

        let stats = Stats::from(&vec![10, 20]);
        assert_eq!(stats.mean(), F64::new(15.0));
        assert_eq!(stats.mdtm(), F64::new(5.0));
    }

    #[test]
    fn stats_show() {
        let stats = Stats::from(&vec![1, 1, 1]);
        assert_eq!(stats.show_mean(), "1.0");
        assert_eq!(stats.show_cov(), "0.0");
        assert_eq!(stats.show_mdtm(), "0.0");

        let stats = Stats::from(&vec![10, 20, 30]);
        assert_eq!(stats.show_mean(), "20.0");
        assert_eq!(stats.show_cov(), "0.5");
        assert_eq!(stats.show_mdtm(), "6.7");

        let stats = Stats::from(&vec![10, 20]);
        assert_eq!(stats.show_mean(), "15.0");
        assert_eq!(stats.show_cov(), "0.5");
        assert_eq!(stats.show_mdtm(), "5.0");

        let stats = Stats::from(&vec![10, 20, 40, 10]);
        assert_eq!(stats.show_mean(), "20.0");
        assert_eq!(stats.show_cov(), "0.7");
        assert_eq!(stats.show_mdtm(), "10.0");
    }

    #[test]
    fn stats_improv() {
        let stats_a = Stats::from(&vec![1, 1, 1]);
        let stats_b = Stats::from(&vec![10, 20]);
        assert_eq!(stats_a.mean_improv(&stats_b), F64::new(-14.0));

        let stats_a = Stats::from(&vec![1, 1, 1]);
        let stats_b = Stats::from(&vec![10, 20, 30]);
        assert_eq!(stats_a.cov_improv(&stats_b), F64::new(-0.5));

        let stats_a = Stats::from(&vec![1, 1, 1]);
        let stats_b = Stats::from(&vec![10, 20]);
        assert_eq!(stats_a.mdtm_improv(&stats_b), F64::new(-5.0));
    }

    #[test]
    fn all_stats() {
        let stats = Stats::from(&vec![10, 20, 40, 10]);
        let f = 1;
        let placement = ClientPlacement::Colocated;
        let mut all_stats = AllStats::new();
        all_stats.insert(Protocol::Atlas, f, placement, stats.clone());
        assert_eq!(all_stats.get(Protocol::Atlas, f, placement), &stats);
    }

    #[test]
    #[should_panic]
    fn all_stats_panic() {
        let f = 1;
        let placement = ClientPlacement::Colocated;
        let all_stats = AllStats::new();
        all_stats.get(Protocol::Atlas, f, placement);
    }
}
