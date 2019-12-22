// This module contains the definition of `F64`.
pub mod float;

// This module contains the definition of `Stats`.
pub mod stats;

// Re-exports.
pub use float::F64;
pub use stats::{Stats, StatsKind};

use num_traits::PrimInt;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

pub struct Metrics<K, V> {
    metrics: HashMap<K, Vec<V>>,
}

impl<K, V> Metrics<K, V>
where
    K: Debug + Clone + Hash + Eq,
    V: PrimInt,
{
    pub fn new() -> Self {
        Self {
            metrics: HashMap::new(),
        }
    }

    pub fn add(&mut self, kind: K, value: V) {
        let current = match self.metrics.get_mut(&kind) {
            Some(current) => current,
            None => self.metrics.entry(kind.clone()).or_insert_with(Vec::new),
        };
        current.push(value);
    }

    pub fn show_stats(&self) {
        self.metrics.iter().for_each(|(kind, values)| {
            // TODO can we avoid cloning here?
            let stats = Stats::from(values.clone());

            println!(
                "{:?}: avg={} p95={} p99={}",
                kind,
                stats.mean().round(),
                stats.percentile(0.95).round(),
                stats.percentile(0.99).round(),
            );
        });
    }
}
