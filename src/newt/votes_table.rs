use crate::base::ProcId;
use crate::client::Rifl;
use crate::kvs::command::{Command, MultiCommand};
use crate::kvs::Key;
use crate::newt::votes::{VoteRange, Votes};
use std::collections::{BTreeMap, HashMap};
use threshold::AEClock;

pub struct MultiVotesTable {
    n: usize,
    stability_threshold: usize,
    tables: HashMap<Key, VotesTable>,
}

impl MultiVotesTable {
    /// Create a new `MultiVotesTable` instance given the stability threshold.
    pub fn new(n: usize, stability_threshold: usize) -> Self {
        Self {
            n,
            stability_threshold,
            tables: HashMap::new(),
        }
    }

    /// Add a new command, its clock and votes to the votes table.
    /// TODO Here we can't return a `MultiCommand` because it assumes one
    /// command per key. Also, we don't really need the order enforced by
    /// `MultiCommand` internal data structure.
    pub fn add(
        &mut self,
        proc_id: ProcId,
        cmd: Option<MultiCommand>,
        clock: u64,
        votes: Votes,
    ) -> Option<HashMap<Key, Vec<(Rifl, Command)>>> {
        // if noOp, do nothing; else, get its id and create an iterator of (key,
        // command action)
        // TODO if noOp, should we add `Votes` to the table, or there will be no
        // votes?
        let cmd = cmd?;
        let rifl = cmd.rifl();

        // create sort identifier:
        // - if two commands got assigned the same clock, they will be ordered
        //   by the process id
        let sort_id = (clock, proc_id);

        // add commands and votes to the votes tables, and at the same time
        // compute which commands are safe to be executed
        let to_execute = votes
            .into_iter()
            .zip(cmd.into_iter())
            .map(|((key, vote_ranges), (cmd_key, cmd_action))| {
                // each item from zip should be about the same key
                assert_eq!(key, cmd_key);

                // TODO the borrow checker complains if `self.n` or
                // `self.stability_threshold` is passed to `VotesTable::new`
                let n = self.n;
                let stability_threshold = self.stability_threshold;

                // get this key's table
                let table = self
                    .tables
                    .entry(key)
                    .or_insert_with(|| VotesTable::new(n, stability_threshold));

                // add command and votes to the table
                table.add(sort_id, rifl, cmd_action, vote_ranges);

                // get new commands to be executed
                let stable = table.stable_commands().collect();
                (cmd_key, stable)
            })
            .collect();

        // return commands to be executed
        Some(to_execute)
    }
}

type SortId = (u64, ProcId);

struct VotesTable {
    n: usize,
    stability_threshold: usize,
    // `votes` collects all votes seen until now so that we can compute which
    // timestamp is stable
    votes: AEClock<ProcId>,
    cmds: BTreeMap<SortId, (Rifl, Command)>,
}

impl VotesTable {
    fn new(n: usize, stability_threshold: usize) -> Self {
        // compute process identifiers, making sure ids are non-zero
        let ids = (1..=n).map(|id| id as u64);
        let votes = AEClock::with(ids);
        assert_eq!(votes.len(), n);
        Self {
            n,
            stability_threshold,
            votes,
            cmds: BTreeMap::new(),
        }
    }

    fn add(
        &mut self,
        sort_id: SortId,
        rifl: Rifl,
        cmd_action: Command,
        vote_ranges: Vec<VoteRange>,
    ) {
        // add command to the sorted list of commands to be executed
        let res = self.cmds.insert(sort_id, (rifl, cmd_action));
        // and check there was nothing there for this exact same position
        assert!(res.is_none());

        // update votes with the votes used on this command
        vote_ranges.into_iter().for_each(|range| {
            // assert there's at least one new vote
            assert!(self.votes.add_range(
                &range.voter(),
                range.start(),
                range.end()
            ))
        });
    }

    fn stable_commands(&mut self) -> impl Iterator<Item = (Rifl, Command)> {
        // ensure the clock size equals to n
        assert_eq!(self.votes.len(), self.n);

        // compute the (potentially) new stable clock for this key
        let stable_clock = self
            .votes
            .frontier_threshold(self.stability_threshold)
            .expect("stability threshold must always be smaller than the number of processes");

        // compute stable sort id:
        // - if clock 10 is stable, then we can execute all commands with an id
        //   smaller than `(11,0)`
        // - if id with `(11,0)` is also part of this local structure, we can
        //   also execute it without 11 being stable, because, once 11 is
        //   stable, it will be the first to be executed either way
        let stable_sort_id = (stable_clock + 1, 0);

        // in fact, in the above example, if `(11,0)` is executed, we can also
        // execute `(11,1)`, and with that, execute `(11,2)` and so on
        // TODO loop while the previous flow is true and also return those
        // commands

        // compute the list of commands that can be executed now
        let stable = {
            let mut unstable = self.cmds.split_off(&stable_sort_id);
            // swap unstable with self.cmds
            std::mem::swap(&mut unstable, &mut self.cmds);
            // now unstable contains in fact the stable
            unstable
        };

        // return stable commands
        stable.into_iter().map(|(_, id_and_action)| id_and_action)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use permutator::Permutation;

    #[test]
    fn votes_table_flow() {
        // procs ids
        let proc_id_1 = 1;
        let proc_id_2 = 2;
        let proc_id_3 = 3;
        let proc_id_4 = 4;
        let proc_id_5 = 5;

        // let's consider that n = 5 and q = 3
        // so the threshold should be n - q + 1 = 3
        let n = 5;
        let stability_threshold = 3;
        let mut table = VotesTable::new(n, stability_threshold);

        // in this example we'll use the dot as rifl

        // a1
        let a1 = Command::Put(String::from("A1"));
        // assumes a single client per process that has the same id as the
        // process
        let a1_rifl = Rifl::new(proc_id_1, 1);
        // p1, final clock = 1
        let a1_sort_id = (1, proc_id_1);
        // p1, p2 and p3 voted with 1
        let a1_votes = vec![
            VoteRange::new(proc_id_1, 1, 1),
            VoteRange::new(proc_id_2, 1, 1),
            VoteRange::new(proc_id_3, 1, 1),
        ];

        // c1
        let c1 = Command::Put(String::from("C1"));
        let c1_rifl = Rifl::new(proc_id_3, 1);
        // p3, final clock = 3
        let c1_sort_id = (3, proc_id_3);
        // p1 voted with 2, p2 voted with 3 and p3 voted with 2
        let c1_votes = vec![
            VoteRange::new(proc_id_1, 2, 2),
            VoteRange::new(proc_id_2, 3, 3),
            VoteRange::new(proc_id_3, 2, 2),
        ];

        // d1
        let d1 = Command::Put(String::from("D1"));
        let d1_rifl = Rifl::new(proc_id_4, 1);
        // p4, final clock = 3
        let d1_sort_id = (3, proc_id_4);
        // p2 voted with 2, p3 voted with 3 and p4 voted with 1-3
        let d1_votes = vec![
            VoteRange::new(proc_id_2, 2, 2),
            VoteRange::new(proc_id_3, 3, 3),
            VoteRange::new(proc_id_4, 1, 3),
        ];

        // e1
        let e1 = Command::Put(String::from("E1"));
        let e1_rifl = Rifl::new(proc_id_5, 1);
        // p5, final clock = 4
        let e1_sort_id = (4, proc_id_5);
        // p1 voted with 3, p4 voted with 4 and p5 voted with 1-4
        let e1_votes = vec![
            VoteRange::new(proc_id_1, 3, 3),
            VoteRange::new(proc_id_4, 4, 4),
            VoteRange::new(proc_id_5, 1, 4),
        ];

        // e2
        let e2 = Command::Put(String::from("E2"));
        let e2_rifl = Rifl::new(proc_id_5, 2);
        // p5, final clock = 5
        let e2_sort_id = (5, proc_id_5);
        // p1 voted with 4-5, p4 voted with 5 and p5 voted with 5
        let e2_votes = vec![
            VoteRange::new(proc_id_1, 4, 5),
            VoteRange::new(proc_id_4, 5, 5),
            VoteRange::new(proc_id_5, 5, 5),
        ];

        // add a1 to table
        table.add(a1_sort_id, a1_rifl, a1.clone(), a1_votes.clone());
        // get stable: a1
        let stable: Vec<_> = table.stable_commands().collect();
        assert_eq!(stable, vec![(a1_rifl, a1.clone())]);

        // add d1 to table
        table.add(d1_sort_id, d1_rifl, d1.clone(), d1_votes.clone());
        // get stable: none
        let stable: Vec<_> = table.stable_commands().collect();
        assert_eq!(stable, vec![]);

        // add c1 to table
        table.add(c1_sort_id, c1_rifl, c1.clone(), c1_votes.clone());
        // get stable: c1 then d1
        let stable: Vec<_> = table.stable_commands().collect();
        assert_eq!(stable, vec![(c1_rifl, c1.clone()), (d1_rifl, d1.clone())]);

        // add e2 to table
        table.add(e2_sort_id, e2_rifl, e2.clone(), e2_votes.clone());
        // get stable: none
        let stable: Vec<_> = table.stable_commands().collect();
        assert_eq!(stable, vec![]);

        // add e1 to table
        table.add(e1_sort_id, e1_rifl, e1.clone(), e1_votes.clone());
        // get stable: none
        let stable: Vec<_> = table.stable_commands().collect();
        assert_eq!(stable, vec![(e1_rifl, e1.clone()), (e2_rifl, e2.clone())]);

        // run all the permutations of the above and check that the final total
        // order is the same
        let total_order = vec![
            (a1_rifl, a1.clone()),
            (c1_rifl, c1.clone()),
            (d1_rifl, d1.clone()),
            (e1_rifl, e1.clone()),
            (e2_rifl, e2.clone()),
        ];
        let mut all_ops = vec![
            (a1_sort_id, a1_rifl, a1, a1_votes),
            (c1_sort_id, c1_rifl, c1, c1_votes),
            (d1_sort_id, d1_rifl, d1, d1_votes),
            (e1_sort_id, e1_rifl, e1, e1_votes),
            (e2_sort_id, e2_rifl, e2, e2_votes),
        ];

        all_ops.permutation().for_each(|p| {
            let mut table = VotesTable::new(n, stability_threshold);
            let permutation_total_order: Vec<_> = p
                .clone()
                .into_iter()
                .flat_map(|(sort_id, dot, cmd, votes)| {
                    table.add(sort_id, dot, cmd, votes);
                    table.stable_commands()
                })
                .collect();
            assert_eq!(total_order, permutation_total_order);
        });
    }
}
