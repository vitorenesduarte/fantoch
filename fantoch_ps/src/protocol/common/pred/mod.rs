// This module contains the definition of `KeyClocks` and `QuorumClocks`.
mod clocks;

// Re-exports.
pub use clocks::{
    Clock, KeyClocks, LockedKeyClocks, QuorumClocks, QuorumRetries,
};

use fantoch::id::{Dot, ProcessId};
use fantoch::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use std::iter::FromIterator;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompressedDots {
    pub deps: HashMap<ProcessId, HashSet<u64>>,
}

impl CompressedDots {
    pub fn new() -> Self {
        Self {
            deps: Default::default(),
        }
    }

    pub fn insert(&mut self, dep: Dot) {
        self.deps
            .entry(dep.source())
            .or_default()
            .insert(dep.sequence());
    }

    pub fn remove(&mut self, dep: &Dot) {
        if let Some(seqs) = self.deps.get_mut(&dep.source()) {
            seqs.remove(&dep.sequence());
        }
    }

    pub fn contains(&self, dep: &Dot) -> bool {
        if let Some(seqs) = self.deps.get(&dep.source()) {
            seqs.contains(&dep.sequence())
        } else {
            false
        }
    }

    pub fn merge(&mut self, other: Self) {
        for (process_id, seqs) in other.deps {
            self.deps.entry(process_id).or_default().extend(seqs);
        }
    }

    pub fn len(&self) -> usize {
        self.deps.values().map(|seqs| seqs.len()).sum()
    }

    pub fn iter(&self) -> impl Iterator<Item = Dot> + '_ {
        self.deps.iter().flat_map(|(process_id, seqs)| {
            seqs.into_iter().map(move |seq| Dot::new(*process_id, *seq))
        })
    }
}

impl FromIterator<Dot> for CompressedDots {
    fn from_iter<T: IntoIterator<Item = Dot>>(iter: T) -> Self {
        let mut compressed_dots = Self::new();
        for dot in iter {
            compressed_dots.insert(dot);
        }
        compressed_dots
    }
}
