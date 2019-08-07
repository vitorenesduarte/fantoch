use crate::base::ProcId;
use crate::command::{Command, Object};
use std::collections::BTreeMap;

/// ProcVotes are the Votes by some Process on some command.
pub type ProcVotes = BTreeMap<Object, VoteRange>;

/// Votes are all Votes on some command.
#[derive(Debug, Clone)]
pub struct Votes {
    votes: BTreeMap<Object, Vec<VoteRange>>,
}

impl Votes {
    /// Creates a `Votes` instance.
    pub fn new(cmd: &Command) -> Self {
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
    }
}

// `VoteRange` encodes a set of votes performed by some processed:
// - this will be used to fill the `VotesTable`
#[derive(Debug, Clone)]
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

    /// Get all votes in this range.
    pub fn votes(&self) -> Vec<usize> {
        (self.start..self.end + 1).collect()
    }
}
