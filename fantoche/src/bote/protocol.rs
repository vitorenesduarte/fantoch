use crate::metrics::Histogram;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub enum Protocol {
    FPaxos,
    EPaxos,
    Atlas,
}

impl Protocol {
    pub fn short_name(&self) -> &str {
        match self {
            Protocol::FPaxos => "f",
            Protocol::EPaxos => "e",
            Protocol::Atlas => "a",
        }
    }

    pub fn quorum_size(&self, n: usize, f: usize) -> usize {
        // since EPaxos always tolerates a minority of failures, we ignore the f
        // passed as argument, and compute f to be a minority of n processes
        match self {
            Protocol::FPaxos => f + 1,
            Protocol::EPaxos => {
                let f = Self::minority(n);
                f + ((f + 1) / 2 as usize)
            }
            Protocol::Atlas => Self::minority(n) + f,
        }
    }

    fn minority(n: usize) -> usize {
        n / 2
    }
}

#[derive(Clone, Copy)]
pub enum ClientPlacement {
    Input,
    Colocated,
}

impl ClientPlacement {
    pub fn short_name(&self) -> &str {
        match self {
            ClientPlacement::Input => "",
            ClientPlacement::Colocated => "C",
        }
    }

    pub fn all() -> impl Iterator<Item = ClientPlacement> {
        vec![ClientPlacement::Input, ClientPlacement::Colocated].into_iter()
    }
}

/// Mapping from protocol name to its stats.
#[derive(Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct ProtocolStats(BTreeMap<String, Histogram>);

impl ProtocolStats {
    pub fn new() -> ProtocolStats {
        Default::default()
    }

    pub fn get(
        &self,
        protocol: Protocol,
        f: usize,
        placement: ClientPlacement,
    ) -> &Histogram {
        let key = Self::key(protocol, f, placement);
        self.get_and_unwrap(&key)
    }

    pub fn insert(
        &mut self,
        protocol: Protocol,
        f: usize,
        placement: ClientPlacement,
        stats: Histogram,
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

    fn get_and_unwrap(&self, key: &str) -> &Histogram {
        self.0.get(key).unwrap_or_else(|| {
            panic!("stats with key {} not found", key);
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quorum_size() {
        assert_eq!(Protocol::FPaxos.quorum_size(3, 1), 2);
        assert_eq!(Protocol::FPaxos.quorum_size(5, 1), 2);
        assert_eq!(Protocol::FPaxos.quorum_size(5, 2), 3);
        assert_eq!(Protocol::EPaxos.quorum_size(3, 0), 2);
        assert_eq!(Protocol::EPaxos.quorum_size(5, 0), 3);
        assert_eq!(Protocol::EPaxos.quorum_size(7, 0), 5);
        assert_eq!(Protocol::EPaxos.quorum_size(9, 0), 6);
        assert_eq!(Protocol::EPaxos.quorum_size(11, 0), 8);
        assert_eq!(Protocol::EPaxos.quorum_size(13, 0), 9);
        assert_eq!(Protocol::EPaxos.quorum_size(15, 0), 11);
        assert_eq!(Protocol::EPaxos.quorum_size(17, 0), 12);
        assert_eq!(Protocol::Atlas.quorum_size(3, 1), 2);
        assert_eq!(Protocol::Atlas.quorum_size(5, 1), 3);
        assert_eq!(Protocol::Atlas.quorum_size(5, 2), 4);
    }

    #[test]
    fn protocol_stats() {
        let stats = Histogram::from(vec![10, 20, 40, 10]);
        let f = 1;
        let placement = ClientPlacement::Colocated;
        let mut all_stats = ProtocolStats::new();
        all_stats.insert(Protocol::Atlas, f, placement, stats.clone());
        assert_eq!(all_stats.get(Protocol::Atlas, f, placement), &stats);
    }

    #[test]
    #[should_panic]
    fn protocol_stats_panic() {
        let f = 1;
        let placement = ClientPlacement::Colocated;
        let all_stats = ProtocolStats::new();
        // should panic!
        all_stats.get(Protocol::Atlas, f, placement);
    }
}
