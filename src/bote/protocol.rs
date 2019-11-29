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
        // for Paxos and EPaxos, we ignore the f passed as argument, and compute
        // f to be a minority of n processes
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
        (n / 2)
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
}
