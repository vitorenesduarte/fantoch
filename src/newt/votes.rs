use crate::base::ProcId;
use crate::command::{Command, Object};
use std::collections::BTreeMap;

/// ProcVotes are the Votes by some Process on some command.
pub type ProcVotes = BTreeMap<Object, VoteRange>;

/// Votes are all Votes on some command.
#[derive(Debug, Clone, PartialEq)]
pub struct Votes {
    votes: BTreeMap<Object, Vec<VoteRange>>,
}

impl Votes {
    /// Creates an uninitialized `Votes` instance.
    pub fn uninit() -> Self {
        Votes {
            votes: BTreeMap::new(),
        }
    }

    /// Creates an initialized `Votes` instance.
    pub fn from(cmd: &Command) -> Self {
        // create empty votes
        let votes = cmd
            .objects_clone()
            .into_iter()
            // map each object into tuple (object, empty_votes)
            .map(|object| (object, vec![]))
            .collect();

        // return new `Votes`
        Votes { votes }
    }

    /// Add `ProcVotes` to `Votes`.
    pub fn add(&mut self, proc_votes: ProcVotes) {
        // create proc_votes iterator
        let mut proc_votes = proc_votes.into_iter();

        // while we iterate self
        for (object, object_votes) in self.votes.iter_mut() {
            // the next in proc_votes must be about the same object
            let (next_object, vote) = proc_votes.next().unwrap();
            assert_eq!(*object, next_object);

            // add vote to this object's votes
            object_votes.push(vote);
        }

        // check there's nothing else in the proc votes iterator
        assert!(proc_votes.next().is_none());
    }
}

// `VoteRange` encodes a set of votes performed by some processed:
// - this will be used to fill the `VotesTable`
#[derive(Debug, Clone, PartialEq)]
pub struct VoteRange {
    by: ProcId,
    start: usize,
    end: usize,
}

impl VoteRange {
    /// Create a new `VoteRange` instance.
    pub fn new(by: ProcId, start: usize, end: usize) -> Self {
        VoteRange { by, start, end }
    }

    /// Get which process voted.
    pub fn voter(&self) -> ProcId {
        self.by
    }

    /// Get all votes in this range.
    pub fn votes(&self) -> Vec<usize> {
        (self.start..self.end + 1).collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::command::{Command, Object};
    use crate::newt::clocks::Clocks;
    use crate::newt::votes::Votes;
    use std::cmp::max;

    #[test]
    fn votes_flow() {
        // create clocks
        let mut clocks_p0 = Clocks::new(0);
        let mut clocks_p1 = Clocks::new(1);

        // objects
        let object_a = Object::new("A");
        let object_b = Object::new("B");

        // command a
        let command_a = Command::new(vec![object_a.clone()]);
        let mut votes_a = Votes::from(&command_a);

        // command b
        let command_ab = Command::new(vec![object_a.clone(), object_b.clone()]);
        let mut votes_ab = Votes::from(&command_ab);

        // orders on each process:
        // - p0: Submit(a),  MCommit(a),  MCollect(ab)
        // - p1: Submit(ab), MCollect(a), MCommit(ab)

        // -------------------------
        // submit command a by p0
        let clock_a = clocks_p0.clock(&command_a) + 1;
        assert_eq!(clock_a, 1);

        // -------------------------
        // (local) MCollect handle by p0 (command a)
        let clock_a_p0 = max(clock_a, clocks_p0.clock(&command_a) + 1);
        let proc_votes_a_p0 = clocks_p0.proc_votes(&command_a, clock_a_p0);
        clocks_p0.bump_to(&command_a, clock_a_p0);

        // -------------------------
        // submit command ab by p1
        let clock_ab = clocks_p1.clock(&command_ab) + 1;
        assert_eq!(clock_ab, 1);

        // -------------------------
        // (local) MCollect handle by p1 (command ab)
        let clock_ab_p1 = max(clock_ab, clocks_p1.clock(&command_ab) + 1);
        let proc_votes_ab_p1 = clocks_p1.proc_votes(&command_ab, clock_ab_p1);
        clocks_p1.bump_to(&command_ab, clock_ab_p1);

        // -------------------------
        // (remote) MCollect handle by p1 (command a)
        let clock_a_p1 = max(clock_a, clocks_p1.clock(&command_a) + 1);
        let proc_votes_a_p1 = clocks_p1.proc_votes(&command_a, clock_a_p1);
        clocks_p1.bump_to(&command_a, clock_a_p1);

        // -------------------------
        // (remote) MCollect handle by p0 (command ab)
        let clock_ab_p0 = max(clock_ab, clocks_p0.clock(&command_ab) + 1);
        let proc_votes_ab_p0 = clocks_p0.proc_votes(&command_ab, clock_ab_p0);
        clocks_p0.bump_to(&command_ab, clock_ab_p0);

        // -------------------------
        // MCollectAck handles by p0 (command a)
        votes_a.add(proc_votes_a_p0);
        votes_a.add(proc_votes_a_p1);

        // there's a single object
        assert_eq!(votes_a.votes.len(), 1);

        // there are two voters
        let object_votes = votes_a.votes.get(&object_a).unwrap();
        assert_eq!(object_votes.len(), 2);

        // p0 voted with 1
        println!("{:?}", object_votes);
        let mut object_votes = object_votes.into_iter();
        let object_votes_by_p0 = object_votes.next().unwrap();
        assert_eq!(object_votes_by_p0.voter(), 0);
        assert_eq!(object_votes_by_p0.votes(), vec![1]);

        // p1 voted with 2
        let object_votes_by_p1 = object_votes.next().unwrap();
        assert_eq!(object_votes_by_p1.voter(), 1);
        assert_eq!(object_votes_by_p1.votes(), vec![2]);

        // -------------------------
        // MCollectAck handles by p1 (command ab)
        votes_ab.add(proc_votes_ab_p1);
        votes_ab.add(proc_votes_ab_p0);

        // there are two objects
        assert_eq!(votes_ab.votes.len(), 2);

        // object a:
        // there are two voters
        let object_votes = votes_ab.votes.get(&object_a).unwrap();
        assert_eq!(object_votes.len(), 2);

        // p1 voted with 1
        let mut object_votes = object_votes.into_iter();
        let object_votes_by_p1 = object_votes.next().unwrap();
        assert_eq!(object_votes_by_p1.voter(), 1);
        assert_eq!(object_votes_by_p1.votes(), vec![1]);

        // p0 voted with 2
        let object_votes_by_p0 = object_votes.next().unwrap();
        assert_eq!(object_votes_by_p0.voter(), 0);
        assert_eq!(object_votes_by_p0.votes(), vec![2]);

        // object b:
        // there are two voters
        let object_votes = votes_ab.votes.get(&object_b).unwrap();
        assert_eq!(object_votes.len(), 2);

        // p1 voted with 1
        let mut object_votes = object_votes.into_iter();
        let object_votes_by_p1 = object_votes.next().unwrap();
        assert_eq!(object_votes_by_p1.voter(), 1);
        assert_eq!(object_votes_by_p1.votes(), vec![1]);

        // p0 voted with 1 and 2
        let object_votes_by_p0 = object_votes.next().unwrap();
        assert_eq!(object_votes_by_p0.voter(), 0);
        assert_eq!(object_votes_by_p0.votes(), vec![1, 2]);
    }
}
