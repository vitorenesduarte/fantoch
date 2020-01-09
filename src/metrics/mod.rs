// This module contains the definition of `F64`.
pub mod float;

// This module contains the definition of `Histogram`.
mod histogram;

// Re-exports.
pub use float::F64;
pub use histogram::{Histogram, Stats};

use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

pub struct Metrics<K, V> {
    collected: HashMap<K, Histogram>,
    aggregated: HashMap<K, V>,
}

impl<K, V> Metrics<K, V>
where
    K: Hash + Eq,
    V: Default,
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

    pub fn aggregate<F>(&mut self, kind: K, update: F)
    where
        F: FnOnce(&mut V),
    {
        let current = match self.aggregated.get_mut(&kind) {
            Some(current) => current,
            None => self.aggregated.entry(kind).or_insert_with(V::default),
        };
        update(current);
    }
}

impl<K, V> Metrics<K, V>
where
    K: Debug,
    V: Debug,
{
    pub fn show(&self) {
        self.collected.iter().for_each(|(kind, stats)| {
            println!(
                "{:?}: avg={} p95={} p99={}",
                kind,
                stats.mean().round(),
                stats.percentile(0.95).round(),
                stats.percentile(0.99).round(),
            );
        });
        self.aggregated.iter().for_each(|(kind, value)| {
            println!("{:?}: {:?}", kind, value);
        });
    }
}
