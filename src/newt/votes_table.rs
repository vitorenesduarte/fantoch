use crate::newt::votes::Votes;
use crate::base::ProcId;
use crate::command::{Command, Key, MultiCommand};
use std::collections::{BTreeMap, HashMap};
use threshold::AEClock;

pub struct VotesTable {
    stability_threshold: usize,
    votes: HashMap<Key, AEClock<ProcId>>,
    cmds: HashMap<Key, BTreeMap<SortId, Command>>,
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
    pub fn add(
        &mut self,
        proc_id: ProcId,
        cmd: Option<MultiCommand>,
        clock: u64,
        votes: Votes,
    ) {
        // create sort identifier:
        // - if two commands got assigned the same clock, they will be ordered
        //   by the process id
        let sort_id = (clock, proc_id);
        let stable_clocks = self.add_votes(votes);

        if let Some(cmd) = cmd {
            // if it's not a noOp
        }
    }

    fn add_votes(&mut self, votes: Votes) -> Vec<(Key, u64)> {
        votes
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
