use crate::base::{ProcId, Dot};
use crate::command::{Key, MultiCommand};
use std::collections::{BTreeMap, HashMap};
use threshold::AEClock;

/// ProcVotes are the Votes by some Process on some command.
pub type ProcVotes = BTreeMap<Key, VoteRange>;

/// Votes are all Votes on some command.
#[derive(Debug, Clone, PartialEq)]
pub struct Votes {
    votes: BTreeMap<Key, Vec<VoteRange>>,
}

impl Votes {
    /// Creates an uninitialized `Votes` instance.
    pub fn uninit() -> Self {
        Votes {
            votes: BTreeMap::new(),
        }
    }

    /// Creates an initialized `Votes` instance.
    pub fn from(cmd: &MultiCommand) -> Self {
        // create empty votes
        let votes = cmd
            .keys()
            .into_iter()
            // map each key tuple (key, empty_votes)
            .map(|key| (key.clone(), vec![]))
            .collect();

        // return new `Votes`
        Votes { votes }
    }

    /// Add `ProcVotes` to `Votes`.
    pub fn add(&mut self, proc_votes: ProcVotes) {
        // create proc_votes iterator
        let mut proc_votes = proc_votes.into_iter();

        // while we iterate self
        for (key, key_votes) in self.votes.iter_mut() {
            // the next in proc_votes must be about the same key
            let (next_key, vote) = proc_votes.next().unwrap();
            assert_eq!(*key, next_key);

            // add vote to this key's votes
            key_votes.push(vote);
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
    start: u64,
    end: u64,
}

impl VoteRange {
    /// Create a new `VoteRange` instance.
    pub fn new(by: ProcId, start: u64, end: u64) -> Self {
        VoteRange { by, start, end }
    }

    /// Get which process voted.
    pub fn voter(&self) -> ProcId {
        self.by
    }

    /// Get all votes in this range.
    pub fn votes(&self) -> Vec<u64> {
        (self.start..self.end + 1).collect()
    }
}

pub struct VotesTable {
    stability_threshold: usize,
    votes: HashMap<Key, AEClock<ProcId>>,
    cmds: HashMap<Key, BTreeMap<SortId, MultiCommand>>,
}

pub struct SortId {
    seq: u64,
    proc_id: ProcId,
}

impl VotesTable {
    /// Create a new `VotesTable` instance given the stability threshold.
    pub fn new(stability_threshold: usize) -> Self {
        VotesTable {
            stability_threshold,
            votes: HashMap::new(),
            cmds: HashMap::new(),
        }
    }

    /// Add a new command, its clock and votes to the votes table.
    pub fn add(&mut self, dot: Dot, cmd: Option<MultiCommand>, clock: u64, votes: Votes) {
        let stable_clocks = self.add_votes(votes);

        if let Some(cmd) = cmd {
            // if it's not a noOp
        }
    }

    fn add_votes(&mut self, votes: Votes) -> Vec<(Key, u64)> {
        votes
            .votes
            .into_iter()
            .map(|(key, vote_ranges)| {
                // get the clock representing the votes on this key
                let key_votes = self
                    .votes
                    .entry(key.clone())
                    .or_insert_with(|| AEClock::new());

                // for each new vote range
                vote_ranges.into_iter().for_each(|vote_range| {
                    // TODO this step could be more efficient if
                    // `threshold::Clock` supports adding
                    // ranges to the clock
                    vote_range.votes().into_iter().for_each(|vote| {
                        key_votes.add(&vote_range.voter(), vote);
                    })
                });

                // compute the stable clock for this key
                let stable_clock = key_votes
                    .frontier_threshold(self.stability_threshold)
                    .unwrap();
                (key, stable_clock)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::command::{Command, MultiCommand};
    use crate::newt::clocks::Clocks;
    use crate::newt::votes::Votes;
    use std::cmp::max;

    #[test]
    fn votes_flow() {
        // create clocks
        let mut clocks_p0 = Clocks::new(0);
        let mut clocks_p1 = Clocks::new(1);

        // keys
        let key_a = String::from("A");
        let key_b = String::from("B");
        
        // key command
        let get_key_a = Command::Get(key_a.clone());
        let get_key_b = Command::Get(key_b.clone());

        // command a
        let cmd_a = MultiCommand::new(vec![get_key_a.clone()]);
        let mut votes_a = Votes::from(&cmd_a);

        // command b
        let cmd_ab = MultiCommand::new(vec![get_key_a.clone(), get_key_b.clone()]);
        let mut votes_ab = Votes::from(&cmd_ab);

        // orders on each process:
        // - p0: Submit(a),  MCommit(a),  MCollect(ab)
        // - p1: Submit(ab), MCollect(a), MCommit(ab)

        // -------------------------
        // submit command a by p0
        let clock_a = clocks_p0.clock(&cmd_a) + 1;
        assert_eq!(clock_a, 1);

        // -------------------------
        // (local) MCollect handle by p0 (command a)
        let clock_a_p0 = max(clock_a, clocks_p0.clock(&cmd_a) + 1);
        let proc_votes_a_p0 = clocks_p0.proc_votes(&cmd_a, clock_a_p0);
        clocks_p0.bump_to(&cmd_a, clock_a_p0);

        // -------------------------
        // submit command ab by p1
        let clock_ab = clocks_p1.clock(&cmd_ab) + 1;
        assert_eq!(clock_ab, 1);

        // -------------------------
        // (local) MCollect handle by p1 (command ab)
        let clock_ab_p1 = max(clock_ab, clocks_p1.clock(&cmd_ab) + 1);
        let proc_votes_ab_p1 = clocks_p1.proc_votes(&cmd_ab, clock_ab_p1);
        clocks_p1.bump_to(&cmd_ab, clock_ab_p1);

        // -------------------------
        // (remote) MCollect handle by p1 (command a)
        let clock_a_p1 = max(clock_a, clocks_p1.clock(&cmd_a) + 1);
        let proc_votes_a_p1 = clocks_p1.proc_votes(&cmd_a, clock_a_p1);
        clocks_p1.bump_to(&cmd_a, clock_a_p1);

        // -------------------------
        // (remote) MCollect handle by p0 (command ab)
        let clock_ab_p0 = max(clock_ab, clocks_p0.clock(&cmd_ab) + 1);
        let proc_votes_ab_p0 = clocks_p0.proc_votes(&cmd_ab, clock_ab_p0);
        clocks_p0.bump_to(&cmd_ab, clock_ab_p0);

        // -------------------------
        // MCollectAck handles by p0 (command a)
        votes_a.add(proc_votes_a_p0);
        votes_a.add(proc_votes_a_p1);

        // there's a single key
        assert_eq!(votes_a.votes.len(), 1);

        // there are two voters
        let key_votes = votes_a.votes.get(&key_a).unwrap();
        assert_eq!(key_votes.len(), 2);

        // p0 voted with 1
        println!("{:?}", key_votes);
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
        votes_ab.add(proc_votes_ab_p1);
        votes_ab.add(proc_votes_ab_p0);

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
