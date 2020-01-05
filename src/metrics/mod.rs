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
    collected: HashMap<K, Vec<V>>,
    aggregated: HashMap<K, V>,
}

impl<K, V> Metrics<K, V>
where
    K: Hash + Eq + Debug,
    V: Default + PrimInt + Debug,
{
    pub fn new() -> Self {
        Self {
            collected: HashMap::new(),
            aggregated: HashMap::new(),
        }
    }

    pub fn add(&mut self, kind: K, value: V) {
        let current = match self.collected.get_mut(&kind) {
            Some(current) => current,
            None => self.collected.entry(kind).or_insert_with(Vec::new),
        };
        current.push(value);
    }

    // TODO unfortunately, given trait bounds, we can only aggregate `PrimInt` types; find a way to
    // remove that limitation
    pub fn update<F>(&mut self, kind: K, update: F)
    where
        F: FnOnce(&mut V),
    {
        let current = match self.aggregated.get_mut(&kind) {
            Some(current) => current,
            None => self.aggregated.entry(kind).or_insert(V::default()),
        };
        update(current);
    }

    pub fn show_stats(&self) {
        self.collected.iter().for_each(|(kind, values)| {
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
        self.aggregated.iter().for_each(|(kind, value)| {
            println!("{:?}: {:?}", kind, value);
        });
    }
}
