pub enum Protocol {
    Atlas,
    FPaxos,
}

impl Protocol {
    pub fn quorum_size(&self, n: usize, f: usize) -> usize {
        match self {
            Protocol::Atlas => {
                let half = n / 2 as usize;
                half + f
            }
            Protocol::FPaxos => f + 1,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn quorum_size() {
        assert_eq!(Protocol::Atlas.quorum_size(3, 1), 2);
        assert_eq!(Protocol::Atlas.quorum_size(5, 1), 3);
        assert_eq!(Protocol::Atlas.quorum_size(5, 2), 4);
        assert_eq!(Protocol::FPaxos.quorum_size(3, 1), 2);
        assert_eq!(Protocol::FPaxos.quorum_size(5, 1), 2);
        assert_eq!(Protocol::FPaxos.quorum_size(5, 2), 3);
    }
}
