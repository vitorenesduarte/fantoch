// This module contains the definition of `F64`.
pub mod float;

// This module contains the definition of `Histogram`.
mod histogram;

// Re-exports.
pub use float::F64;
pub use histogram::{Histogram, Stats};

use crate::HashMap;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::Hash;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Metrics<K: Eq + Hash> {
    collected: HashMap<K, Histogram>,
    aggregated: HashMap<K, u64>,
}

impl<K> Metrics<K>
where
    K: Eq + Hash + Copy,
{
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            collected: HashMap::new(),
            aggregated: HashMap::new(),
        }
    }

    pub fn collect(&mut self, kind: K, value: u64) {
        let stats = match self.collected.get_mut(&kind) {
            Some(current) => current,
            None => self.collected.entry(kind).or_insert_with(Histogram::new),
        };
        stats.increment(value);
    }

    pub fn aggregate(&mut self, kind: K, by: u64) {
        let current = match self.aggregated.get_mut(&kind) {
            Some(current) => current,
            None => self.aggregated.entry(kind).or_default(),
        };
        *current += by;
    }

    pub fn get_collected(&self, kind: K) -> Option<&Histogram> {
        self.collected.get(&kind)
    }

    pub fn get_aggregated(&self, kind: K) -> Option<&u64> {
        self.aggregated.get(&kind)
    }

    pub fn merge(&mut self, other: &Self) {
        for (k, hist) in other.collected.iter() {
            let current = self.collected.entry(*k).or_default();
            current.merge(hist);
        }
        for (k, v) in other.aggregated.iter() {
            let current = self.aggregated.entry(*k).or_default();
            *current += v;
        }
    }
}

impl<K> fmt::Debug for Metrics<K>
where
    K: Eq + Hash + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (kind, histogram) in self.collected.iter() {
            writeln!(f, "{:?}: {:?}", kind, histogram)?;
        }
        for (kind, value) in self.aggregated.iter() {
            writeln!(f, "{:?}: {:?}", kind, value)?;
        }
        Ok(())
    }
}
