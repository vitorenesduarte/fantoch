// This module contains the definition of `KeyClocks` and `QuorumClocks`.
mod clocks;

// Re-exports.
pub use clocks::{
    Clock, KeyClocks, LockedKeyClocks, QuorumClocks, QuorumRetries,
};

use fantoch::id::Dot;
use fantoch::HashSet;
use serde::{Deserialize, Serialize};
use std::iter::FromIterator;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaesarDeps {
    pub deps: HashSet<Dot>,
}

impl CaesarDeps {
    pub fn new() -> Self {
        Self {
            deps: Default::default(),
        }
    }

    pub fn insert(&mut self, dep: Dot) {
        self.deps.insert(dep);
    }

    pub fn remove(&mut self, dep: &Dot) {
        self.deps.remove(dep);
    }

    pub fn contains(&self, dep: &Dot) -> bool {
        self.deps.contains(dep)
    }

    pub fn merge(&mut self, other: Self) {
        for dep in other.deps {
            self.insert(dep);
        }
    }

    pub fn len(&self) -> usize {
        self.deps.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Dot> + '_ {
        self.deps.iter()
    }
}

impl FromIterator<Dot> for CaesarDeps {
    fn from_iter<T: IntoIterator<Item = Dot>>(iter: T) -> Self {
        let mut compressed_dots = Self::new();
        for dot in iter {
            compressed_dots.insert(dot);
        }
        compressed_dots
    }
}
