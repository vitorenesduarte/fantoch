use crate::bote::protocol::{ClientPlacement, Protocol};
use crate::stats::Stats;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Mapping from protocol name to its stats.
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Deserialize, Serialize, Default)]
pub struct AllStats(BTreeMap<String, Stats>);

impl AllStats {
    pub fn new() -> AllStats {
        Default::default()
    }

    pub fn get(&self, protocol: Protocol, f: usize, placement: ClientPlacement) -> &Stats {
        let key = Self::key(protocol, f, placement);
        self.get_and_unwrap(&key)
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

    pub fn fmt(&self, protocol: Protocol, f: usize, placement: ClientPlacement) -> String {
        let key = Self::key(protocol, f, placement);
        let stats = self.get_and_unwrap(&key);
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

    fn get_and_unwrap(&self, key: &str) -> &Stats {
        self.0.get(key).unwrap_or_else(|| {
            panic!("stats with key {} not found", key);
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
