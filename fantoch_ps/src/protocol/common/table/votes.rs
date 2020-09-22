use fantoch::hash_map::{self, HashMap};
use fantoch::id::ProcessId;
use fantoch::kvs::Key;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Votes are all Votes on some command.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Votes {
    votes: HashMap<Key, Vec<VoteRange>>,
}

impl Votes {
    /// Creates an empty `Votes` instance.
    pub fn new() -> Self {
        Self {
            votes: HashMap::new(),
        }
    }

    /// Creates an empty `Votes` instance.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            votes: HashMap::with_capacity(capacity),
        }
    }

    /// Add new vote range to `Votes`.
    #[allow(clippy::ptr_arg)]
    pub fn add(&mut self, key: &Key, vote: VoteRange) {
        // add new vote to current set of votes
        let current_votes = match self.votes.get_mut(key) {
            Some(current_votes) => current_votes,
            None => self.votes.entry(key.clone()).or_insert_with(Vec::new),
        };

        // if there's a last vote, try to compress with that one
        if let Some(last) = current_votes.last_mut() {
            if let Some(vote) = last.try_compress(vote) {
                // if here, then we couldn't compress
                current_votes.push(vote);
            }
        } else {
            current_votes.push(vote);
        }
    }

    /// Sets the votes on some `Key`.
    #[allow(clippy::ptr_arg)]
    pub fn set(&mut self, key: Key, key_votes: Vec<VoteRange>) {
        let res = self.votes.insert(key, key_votes);
        assert!(res.is_none());
    }

    /// Merge with another `Votes`.
    /// Performance should be better if `self.votes.len() > remote_votes.len()`
    /// than with the opposite.
    pub fn merge(&mut self, remote_votes: Votes) {
        remote_votes.into_iter().for_each(|(key, key_votes)| {
            // add new votes to current set of votes
            let current_votes = self.votes.entry(key).or_insert_with(Vec::new);
            current_votes.extend(key_votes);
        });
    }

    /// Gets the current votes on some key.
    #[allow(clippy::ptr_arg)]
    pub fn get(&self, key: &Key) -> Option<&Vec<VoteRange>> {
        self.votes.get(key)
    }

    /// Removes the votes on some key.
    #[allow(clippy::ptr_arg)]
    pub fn remove(&mut self, key: &Key) -> Option<Vec<VoteRange>> {
        self.votes.remove(key)
    }

    /// Get the number of votes.
    pub fn len(&self) -> usize {
        self.votes.len()
    }

    /// Checks if `Votes` is empty.
    pub fn is_empty(&self) -> bool {
        self.votes.is_empty()
    }
}

impl IntoIterator for Votes {
    type Item = (Key, Vec<VoteRange>);
    type IntoIter = hash_map::IntoIter<Key, Vec<VoteRange>>;

    /// Returns a `Votes` into-iterator.
    fn into_iter(self) -> Self::IntoIter {
        self.votes.into_iter()
    }
}

// `VoteRange` encodes a set of votes performed by some processed:
// - this will be used to fill the `VotesTable`
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct VoteRange {
    by: ProcessId,
    start: u64,
    end: u64,
}

impl VoteRange {
    /// Create a new `VoteRange` instance.
    pub fn new(by: ProcessId, start: u64, end: u64) -> Self {
        assert!(start <= end);
        Self { by, start, end }
    }

    /// Get which process voted.
    pub fn voter(&self) -> ProcessId {
        self.by
    }

    /// Get range start.
    pub fn start(&self) -> u64 {
        self.start
    }

    /// Get range end.
    pub fn end(&self) -> u64 {
        self.end
    }

    /// Compress the `VoteRange` passed as argument into self if both form a
    /// contiguous sequence of votes.
    #[must_use]
    pub fn try_compress(&mut self, other: Self) -> Option<Self> {
        // check that we have the same voters
        assert_eq!(self.by, other.by);

        // check if we can compress
        if self.end + 1 == other.start {
            // in this case we can:
            // - update `self.end` to be `other.end`
            self.end = other.end;
            None
        } else {
            // in this case we can't
            Some(other)
        }
    }

    /// Get all votes in this range.
    pub fn votes(&self) -> Vec<u64> {
        (self.start..=self.end).collect()
    }
}

impl fmt::Debug for VoteRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.start == self.end {
            write!(f, "<{}: {}>", self.by, self.start)
        } else {
            write!(f, "<{}: {}-{}>", self.by, self.start, self.end)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::common::table::{KeyClocks, SequentialKeyClocks};
    use fantoch::command::Command;
    use fantoch::id::Rifl;
    use fantoch::kvs::KVOp;

    #[test]
    fn vote_range_compress() {
        let a = VoteRange::new(1, 1, 1);
        let b = VoteRange::new(1, 2, 2);
        let c = VoteRange::new(1, 3, 6);
        let d = VoteRange::new(1, 7, 8);

        let mut a_with_b = a.clone();
        let res = a_with_b.try_compress(b.clone());
        assert_eq!(a_with_b, VoteRange::new(1, 1, 2),);
        assert_eq!(res, None);

        let mut a_with_c = a.clone();
        let res = a_with_c.try_compress(c.clone());
        assert_eq!(a_with_c, a);
        assert_eq!(res, Some(c.clone()));

        let mut c_with_d = c.clone();
        let res = c_with_d.try_compress(d.clone());
        assert_eq!(c_with_d, VoteRange::new(1, 3, 8));
        assert_eq!(res, None);
    }

    #[test]
    fn votes_flow() {
        // create clocks
        let shard_id = 0;
        let mut clocks_p0 = SequentialKeyClocks::new(0, shard_id);
        let mut clocks_p1 = SequentialKeyClocks::new(1, shard_id);

        // keys
        let key_a = String::from("A");
        let key_b = String::from("B");

        // command a
        let cmd_a_rifl = Rifl::new(100, 1); // client 100, 1st op
        let cmd_a = Command::from(cmd_a_rifl, vec![(key_a.clone(), KVOp::Get)]);
        let mut votes_a = Votes::new();

        // command b
        let cmd_ab_rifl = Rifl::new(101, 1); // client 101, 1st op
        let cmd_ab = Command::from(
            cmd_ab_rifl,
            vec![(key_a.clone(), KVOp::Get), (key_b.clone(), KVOp::Get)],
        );
        let mut votes_ab = Votes::new();

        // orders on each process:
        // - p0: Submit(a),  MCommit(a),  MCollect(ab)
        // - p1: Submit(ab), MCollect(a), MCommit(ab)

        // ------------------------
        // submit command a by p0 AND
        // (local) MCollect handle by p0 (command a)
        let (clock_a_p0, process_votes_a_p0) =
            clocks_p0.bump_and_vote(&cmd_a, 0);
        assert_eq!(clock_a_p0, 1);

        // -------------------------
        // submit command ab by p1 AND
        // (local) MCollect handle by p1 (command ab)
        let (clock_ab_p1, process_votes_ab_p1) =
            clocks_p1.bump_and_vote(&cmd_ab, 0);
        assert_eq!(clock_ab_p1, 1);

        // -------------------------
        // (remote) MCollect handle by p1 (command a)
        let (clock_a_p1, process_votes_a_p1) =
            clocks_p1.bump_and_vote(&cmd_a, clock_a_p0);
        assert_eq!(clock_a_p1, 2);

        // -------------------------
        // (remote) MCollect handle by p0 (command ab)
        let (clock_ab_p0, process_votes_ab_p0) =
            clocks_p0.bump_and_vote(&cmd_ab, clock_ab_p1);
        assert_eq!(clock_ab_p0, 2);

        // -------------------------
        // MCollectAck handles by p0 (command a)
        votes_a.merge(process_votes_a_p0);
        votes_a.merge(process_votes_a_p1);

        // there's a single key
        assert_eq!(votes_a.votes.len(), 1);

        // there are two voters
        let key_votes = votes_a.votes.get(&key_a).unwrap();
        assert_eq!(key_votes.len(), 2);

        // p0 voted with 1
        let mut key_votes = key_votes.into_iter();
        let key_votes_by_p0 = key_votes.next().unwrap();
        assert_eq!(key_votes_by_p0.voter(), 0);
        assert_eq!(key_votes_by_p0.votes(), vec![1]);

        // p1 voted with 2
        let key_votes_by_p1 = key_votes.next().unwrap();
        assert_eq!(key_votes_by_p1.voter(), 1);
        assert_eq!(key_votes_by_p1.votes(), vec![2]);

        // -------------------------
        // MCollectAck handles by p1 (command ab)
        votes_ab.merge(process_votes_ab_p1);
        votes_ab.merge(process_votes_ab_p0);

        // there are two keys
        assert_eq!(votes_ab.votes.len(), 2);

        // key a:
        // there are two voters
        let key_votes = votes_ab.votes.get(&key_a).unwrap();
        assert_eq!(key_votes.len(), 2);

        // p1 voted with 1
        let mut key_votes = key_votes.into_iter();
        let key_votes_by_p1 = key_votes.next().unwrap();
        assert_eq!(key_votes_by_p1.voter(), 1);
        assert_eq!(key_votes_by_p1.votes(), vec![1]);

        // p0 voted with 2
        let key_votes_by_p0 = key_votes.next().unwrap();
        assert_eq!(key_votes_by_p0.voter(), 0);
        assert_eq!(key_votes_by_p0.votes(), vec![2]);

        // key b:
        // there are two voters
        let key_votes = votes_ab.votes.get(&key_b).unwrap();
        assert_eq!(key_votes.len(), 2);

        // p1 voted with 1
        let mut key_votes = key_votes.into_iter();
        let key_votes_by_p1 = key_votes.next().unwrap();
        assert_eq!(key_votes_by_p1.voter(), 1);
        assert_eq!(key_votes_by_p1.votes(), vec![1]);

        // p0 voted with 1 and 2
        let key_votes_by_p0 = key_votes.next().unwrap();
        assert_eq!(key_votes_by_p0.voter(), 0);
        assert_eq!(key_votes_by_p0.votes(), vec![1, 2]);
    }
}
