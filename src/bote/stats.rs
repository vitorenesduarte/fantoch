use crate::bote::float::F64;
use serde::{Deserialize, Serialize};
use statrs::statistics::Statistics;
use std::collections::BTreeMap;
use std::fmt;

#[derive(Ord, PartialOrd, Eq, PartialEq, Deserialize, Serialize)]
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
        self.mean.round()
    }

    pub fn show_cov(&self) -> String {
        self.cov.round()
    }

    pub fn show_mdtm(&self) -> String {
        self.mdtm.round()
    }

    fn compute_stats(xs: &Vec<usize>) -> (F64, F64, F64) {
        // transform input in a `Vec<f64>`
        let xs: Vec<f64> = xs.into_iter().map(|&x| x as f64).collect();

        // compute mean
        // TODO maybe find a different library that does not consume the `Vec`
        let mean = xs.clone().mean();

        // compute coefficient of variation
        let cov = xs.clone().std_dev() / mean;

        // compute mean distance to mean
        let mdtm = xs
            .into_iter()
            .map(|x| (x - mean).abs())
            .collect::<Vec<_>>()
            .mean();

        // return the 3 stats
        (F64::new(mean), F64::new(cov), F64::new(mdtm))
    }
}

/// Mapping from protocol name to its stats.
#[derive(Ord, PartialOrd, Eq, PartialEq, Deserialize, Serialize)]
pub struct AllStats(BTreeMap<String, Stats>);

impl AllStats {
    pub fn new() -> AllStats {
        AllStats(BTreeMap::new())
    }

    pub fn get(&self, prefix: &str, f: usize) -> &Stats {
        let key = Self::key(prefix, f);
        self.0.get(&key).unwrap()
    }

    pub fn insert(&mut self, prefix: &str, f: usize, stats: Stats) {
        let key = Self::key(prefix, f);
        self.0.insert(key, stats);
    }

    fn key(prefix: &str, f: usize) -> String {
        match prefix {
            "epaxos" => String::from("epaxos"),
            _ => format!("{}f{}", prefix, f),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn stats() {
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
}
