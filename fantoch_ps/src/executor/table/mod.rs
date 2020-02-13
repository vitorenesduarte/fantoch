/// This modules contains the definition of `TableExecutor` and
/// `TableExecutionInfo`.
mod executor;

// Re-exports.
pub use executor::{TableExecutionInfo, TableExecutor};

use fantoch::id::{Dot, ProcessId, Rifl};
use fantoch::kvs::{KVOp, Key};
use crate::protocol::common::table::VoteRange;
use fantoch::util;
use std::collections::{BTreeMap, HashMap};
use std::mem;
use threshold::AEClock;

type SortId = (u64, Dot);

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
    #[must_use]
    pub fn add_votes(
        &mut self,
        dot: Dot,
        clock: u64,
        rifl: Rifl,
        key: &Key,
        op: KVOp,
        votes: Vec<VoteRange>,
    ) -> impl Iterator<Item = (Rifl, KVOp)> {
        // add ops and votes to the votes tables, and at the same time
        // compute which ops are safe to be executed
        self.update_table(key, |table| {
            table.add(dot, clock, rifl, op, votes);
            table.stable_ops()
        })
    }

    /// Adds phantom votes to the votes table.
    #[must_use]
    pub fn add_phantom_votes(
        &mut self,
        key: &Key,
        votes: Vec<VoteRange>,
    ) -> impl Iterator<Item = (Rifl, KVOp)> {
        // add phantom votes to the votes tables, and at the same time compute
        // which ops are safe to be executed
        self.update_table(key, |table| {
            table.add_votes(votes);
            table.stable_ops()
        })
    }

    // Generic function to be used when updating some votes table.
    #[must_use]
    fn update_table<F, I>(&mut self, key: &Key, update: F) -> I
    where
        F: FnOnce(&mut VotesTable) -> I,
        I: Iterator<Item = (Rifl, KVOp)>,
    {
        let table = match self.tables.get_mut(key) {
            Some(table) => table,
            None => {
                // table does not exist, let's create a new one and insert it
                let table = VotesTable::new(self.n, self.stability_threshold);
                self.tables.entry(key.clone()).or_insert(table)
            }
        };
        // update table
        update(table)
    }
}

struct VotesTable {
    n: usize,
    stability_threshold: usize,
    // `votes_clock` collects all votes seen until now so that we can compute
    // which timestamp is stable
    votes_clock: AEClock<ProcessId>,
    ops: BTreeMap<SortId, (Rifl, KVOp)>,
}

impl VotesTable {
    fn new(n: usize, stability_threshold: usize) -> Self {
        let ids = util::process_ids(n);
        let votes_clock = AEClock::with(ids);
        Self {
            n,
            stability_threshold,
            votes_clock,
            ops: BTreeMap::new(),
        }
    }

    fn add(
        &mut self,
        dot: Dot,
        clock: u64,
        rifl: Rifl,
        op: KVOp,
        votes: Vec<VoteRange>,
    ) {
        // create sort identifier:
        // - if two ops got assigned the same clock, they will be ordered by
        //   their dot
        let sort_id = (clock, dot);

        // add op to the sorted list of ops to be executed
        let res = self.ops.insert(sort_id, (rifl, op));
        // and check there was nothing there for this exact same position
        assert!(res.is_none());

        // update votes with the votes used on this command
        self.add_votes(votes);
    }

    // TODO optimize me: `threshold` has a really inefficient implementation of
    // `add_range`
    fn add_votes(&mut self, votes: Vec<VoteRange>) {
        votes.into_iter().for_each(|vote_range| {
            // assert there's at least one new vote
            assert!(self.votes_clock.add_range(
                &vote_range.voter(),
                vote_range.start(),
                vote_range.end()
            ));
            // assert that the clock size didn't change
            assert_eq!(self.votes_clock.len(), self.n);
        });
    }

    fn stable_ops(&mut self) -> impl Iterator<Item = (Rifl, KVOp)> {
        // compute *next* stable sort id:
        // - if clock 10 is stable, then we can execute all ops with an id
        //   smaller than `(11,0)`
        // - if id with `(11,0)` is also part of this local structure, we can
        //   also execute it without 11 being stable, because, once 11 is
        //   stable, it will be the first to be executed either way
        let stable_clock = self.stable_clock();
        let first_dot = Dot::new(1, 1);
        let next_stable = (stable_clock + 1, first_dot);

        // in fact, in the above example, if `(11,0)` is executed, we can also
        // execute `(11,1)`, and with that, execute `(11,2)` and so on
        // TODO loop while the previous flow is true and also return those ops
        // ACTUALLY maybe we can't since now we need to use dots (and not
        // process ids) to break ties

        // compute the list of ops that can be executed now
        let stable = {
            // remove from `self.ops` ops higher than `next_stable`, including
            // `next_stable`
            let mut remaining = self.ops.split_off(&next_stable);
            // swap remaining with `self.ops`
            mem::swap(&mut remaining, &mut self.ops);
            // now remaingin contains what's the stable
            remaining
        };

        // return stable ops
        stable.into_iter().map(|(_, id_and_action)| id_and_action)
    }

    // Computes the (potentially) new stable clock in this table.
    fn stable_clock(&self) -> u64 {
        self.votes_clock
            .frontier_threshold(self.stability_threshold)
            .expect("stability threshold must always be smaller than the number of processes")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use permutator::Permutation;

    #[test]
    fn votes_table_majority_quorums() {
        // process ids
        let process_id_1 = 1;
        let process_id_2 = 2;
        let process_id_3 = 3;
        let process_id_4 = 4;
        let process_id_5 = 5;

        // let's consider that n = 5 and q = 3
        // so the threshold should be n - q + 1 = 3
        let n = 5;
        let stability_threshold = 3;
        let mut table = VotesTable::new(n, stability_threshold);

        // in this example we'll use the dot as rifl

        // a1
        let a1 = KVOp::Put(String::from("A1"));
        // assumes a single client per process that has the same id as the
        // process
        // p1, final clock = 1
        let a1_dot = Dot::new(process_id_1, 1);
        let a1_clock = 1;
        let a1_rifl = Rifl::new(process_id_1, 1);
        // p1, p2 and p3 voted with 1
        let a1_votes = vec![
            VoteRange::new(process_id_1, 1, 1),
            VoteRange::new(process_id_2, 1, 1),
            VoteRange::new(process_id_3, 1, 1),
        ];

        // c1
        let c1 = KVOp::Put(String::from("C1"));
        // p3, final clock = 3
        let c1_dot = Dot::new(process_id_3, 1);
        let c1_clock = 3;
        let c1_rifl = Rifl::new(process_id_3, 1);
        // p1 voted with 2, p2 voted with 3 and p3 voted with 2
        let c1_votes = vec![
            VoteRange::new(process_id_1, 2, 2),
            VoteRange::new(process_id_2, 3, 3),
            VoteRange::new(process_id_3, 2, 2),
        ];

        // d1
        let d1 = KVOp::Put(String::from("D1"));
        // p4, final clock = 3
        let d1_dot = Dot::new(process_id_4, 1);
        let d1_clock = 3;
        let d1_rifl = Rifl::new(process_id_4, 1);
        // p2 voted with 2, p3 voted with 3 and p4 voted with 1-3
        let d1_votes = vec![
            VoteRange::new(process_id_2, 2, 2),
            VoteRange::new(process_id_3, 3, 3),
            VoteRange::new(process_id_4, 1, 3),
        ];

        // e1
        let e1 = KVOp::Put(String::from("E1"));
        // p5, final clock = 4
        let e1_dot = Dot::new(process_id_5, 1);
        let e1_clock = 4;
        let e1_rifl = Rifl::new(process_id_5, 1);
        // p1 voted with 3, p4 voted with 4 and p5 voted with 1-4
        let e1_votes = vec![
            VoteRange::new(process_id_1, 3, 3),
            VoteRange::new(process_id_4, 4, 4),
            VoteRange::new(process_id_5, 1, 4),
        ];

        // e2
        let e2 = KVOp::Put(String::from("E2"));
        // p5, final clock = 5
        let e2_dot = Dot::new(process_id_5, 2);
        let e2_clock = 5;
        let e2_rifl = Rifl::new(process_id_5, 2);
        // p1 voted with 4-5, p4 voted with 5 and p5 voted with 5
        let e2_votes = vec![
            VoteRange::new(process_id_1, 4, 5),
            VoteRange::new(process_id_4, 5, 5),
            VoteRange::new(process_id_5, 5, 5),
        ];

        // add a1 to table
        table.add(a1_dot, a1_clock, a1_rifl, a1.clone(), a1_votes.clone());
        // get stable: a1
        let stable = table.stable_ops().collect::<Vec<_>>();
        assert_eq!(stable, vec![(a1_rifl, a1.clone())]);

        // add d1 to table
        table.add(d1_dot, d1_clock, d1_rifl, d1.clone(), d1_votes.clone());
        // get stable: none
        let stable = table.stable_ops().collect::<Vec<_>>();
        assert_eq!(stable, vec![]);

        // add c1 to table
        table.add(c1_dot, c1_clock, c1_rifl, c1.clone(), c1_votes.clone());
        // get stable: c1 then d1
        let stable = table.stable_ops().collect::<Vec<_>>();
        assert_eq!(stable, vec![(c1_rifl, c1.clone()), (d1_rifl, d1.clone())]);

        // add e2 to table
        table.add(e2_dot, e2_clock, e2_rifl, e2.clone(), e2_votes.clone());
        // get stable: none
        let stable = table.stable_ops().collect::<Vec<_>>();
        assert_eq!(stable, vec![]);

        // add e1 to table
        table.add(e1_dot, e2_clock, e1_rifl, e1.clone(), e1_votes.clone());
        // get stable: none
        let stable = table.stable_ops().collect::<Vec<_>>();
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
            (a1_dot, a1_clock, a1_rifl, a1, a1_votes),
            (c1_dot, c1_clock, c1_rifl, c1, c1_votes),
            (d1_dot, d1_clock, d1_rifl, d1, d1_votes),
            (e1_dot, e1_clock, e1_rifl, e1, e1_votes),
            (e2_dot, e2_clock, e2_rifl, e2, e2_votes),
        ];

        all_ops.permutation().for_each(|p| {
            let mut table = VotesTable::new(n, stability_threshold);
            let permutation_total_order: Vec<_> = p
                .clone()
                .into_iter()
                .flat_map(|(dot, clock, rifl, cmd, votes)| {
                    table.add(dot, clock, rifl, cmd, votes);
                    table.stable_ops()
                })
                .collect();
            assert_eq!(total_order, permutation_total_order);
        });
    }

    #[test]
    fn votes_table_tiny_quorums() {
        // process ids
        let process_id_1 = 1;
        let process_id_2 = 2;
        let process_id_3 = 3;
        let process_id_4 = 4;
        let process_id_5 = 5;

        // let's consider that n = 5 and f = 1 and we're using write quorums of
        // size f + 1 so the threshold should be n - f = 4;
        let n = 5;
        let f = 1;
        let stability_threshold = n - f;
        let mut table = VotesTable::new(n, stability_threshold);

        // in this example we'll use the dot as rifl

        // a1
        let a1 = KVOp::Put(String::from("A1"));
        // p1, final clock = 1
        let a1_dot = Dot::new(process_id_1, 1);
        let a1_clock = 1;
        let a1_rifl = Rifl::new(process_id_1, 1);
        // p1, p2 voted with  1
        let a1_votes = vec![
            VoteRange::new(process_id_1, 1, 1),
            VoteRange::new(process_id_2, 1, 1),
        ];

        // add a1 to table
        table.add(a1_dot, a1_clock, a1_rifl, a1.clone(), a1_votes.clone());
        // get stable: none
        let stable = table.stable_ops().collect::<Vec<_>>();
        assert_eq!(stable, vec![]);

        // c1
        let c1 = KVOp::Put(String::from("C1"));
        // p3, final clock = 2
        let c1_dot = Dot::new(process_id_3, 1);
        let c1_clock = 2;
        let c1_rifl = Rifl::new(process_id_3, 1);
        // p2 voted with 2, p3 voted with 1-2
        let c1_votes = vec![
            VoteRange::new(process_id_3, 1, 1),
            VoteRange::new(process_id_2, 2, 2),
            VoteRange::new(process_id_3, 2, 2),
        ];

        // add c1 to table
        table.add(c1_dot, c1_clock, c1_rifl, c1.clone(), c1_votes.clone());
        // get stable: none
        let stable = table.stable_ops().collect::<Vec<_>>();
        assert_eq!(stable, vec![]);

        // e1
        let e1 = KVOp::Put(String::from("E1"));
        // p5, final clock = 1
        let e1_dot = Dot::new(process_id_5, 1);
        let e1_clock = 1;
        let e1_rifl = Rifl::new(process_id_5, 1);
        // p5 and p4 voted with 1
        let e1_votes = vec![
            VoteRange::new(process_id_5, 1, 1),
            VoteRange::new(process_id_4, 1, 1),
        ];

        // add e1 to table
        table.add(e1_dot, e1_clock, e1_rifl, e1.clone(), e1_votes.clone());
        // get stable: a1 and e1
        let stable = table.stable_ops().collect::<Vec<_>>();
        assert_eq!(stable, vec![(a1_rifl, a1.clone()), (e1_rifl, e1.clone())]);

        // a2
        let a2 = KVOp::Put(String::from("A2"));
        // p1, final clock = 3
        let a2_dot = Dot::new(process_id_1, 2);
        let a2_clock = 3;
        let a2_rifl = Rifl::new(process_id_1, 2);
        // p1 voted with 2-3 and p2 voted with 3
        let a2_votes = vec![
            VoteRange::new(process_id_1, 2, 2),
            VoteRange::new(process_id_2, 3, 3),
            VoteRange::new(process_id_1, 3, 3),
        ];

        // add a2 to table
        table.add(a2_dot, a2_clock, a2_rifl, a2.clone(), a2_votes.clone());
        // get stable: none
        let stable = table.stable_ops().collect::<Vec<_>>();
        assert_eq!(stable, vec![]);

        // d1
        let d1 = KVOp::Put(String::from("D1"));
        // p4, final clock = 3
        let d1_dot = Dot::new(process_id_4, 1);
        let d1_clock = 3;
        let d1_rifl = Rifl::new(process_id_4, 1);
        // p4 voted with 2-3 and p3 voted with 3
        let d1_votes = vec![
            VoteRange::new(process_id_4, 2, 2),
            VoteRange::new(process_id_3, 3, 3),
            VoteRange::new(process_id_4, 3, 3),
        ];

        // add d1 to table
        table.add(d1_dot, d1_clock, d1_rifl, d1.clone(), d1_votes.clone());
        // get stable
        let stable = table.stable_ops().collect::<Vec<_>>();
        assert_eq!(
            stable,
            vec![
                (c1_rifl, c1.clone()),
                (a2_rifl, a2.clone()),
                (d1_rifl, d1.clone())
            ]
        );
    }

    #[test]
    fn phantom_votes() {
        // create table
        let n = 5;
        let stability_threshold = 3;
        let mut table = MultiVotesTable::new(n, stability_threshold);

        // create keys
        let key_a = String::from("A");
        let key_b = String::from("B");

        // closure to compute the stable clock for some key
        let stable_clock = |table: &MultiVotesTable, key: &Key| {
            table
                .tables
                .get(key)
                .expect("table for this key should exist")
                .stable_clock()
        };

        // p1 votes on key A
        let process_id = 1;
        let stable = table
            .add_phantom_votes(&key_a, vec![VoteRange::new(process_id, 1, 1)])
            .collect::<Vec<_>>();
        assert!(stable.is_empty());
        // check stable clocks
        assert_eq!(stable_clock(&table, &key_a), 0);

        // p1 votes on key b
        let stable = table
            .add_phantom_votes(&key_b, vec![VoteRange::new(process_id, 1, 1)])
            .collect::<Vec<_>>();
        assert!(stable.is_empty());
        // check stable clocks
        assert_eq!(stable_clock(&table, &key_a), 0);
        assert_eq!(stable_clock(&table, &key_b), 0);

        // p2 votes on key A
        let process_id = 2;
        let stable = table
            .add_phantom_votes(&key_a, vec![VoteRange::new(process_id, 1, 1)])
            .collect::<Vec<_>>();
        assert!(stable.is_empty());
        // check stable clocks
        assert_eq!(stable_clock(&table, &key_a), 0);
        assert_eq!(stable_clock(&table, &key_b), 0);

        // p3 votes on key A
        let process_id = 3;
        let stable = table
            .add_phantom_votes(&key_a, vec![VoteRange::new(process_id, 1, 1)])
            .collect::<Vec<_>>();
        assert!(stable.is_empty());
        // check stable clocks
        assert_eq!(stable_clock(&table, &key_a), 1);
        assert_eq!(stable_clock(&table, &key_b), 0);

        // p3 votes on key B
        let stable = table
            .add_phantom_votes(&key_b, vec![VoteRange::new(process_id, 1, 1)])
            .collect::<Vec<_>>();
        assert!(stable.is_empty());
        // check stable clocks
        assert_eq!(stable_clock(&table, &key_a), 1);
        assert_eq!(stable_clock(&table, &key_b), 0);

        // p4 votes on key B
        let process_id = 4;
        let stable = table
            .add_phantom_votes(&key_b, vec![VoteRange::new(process_id, 1, 1)])
            .collect::<Vec<_>>();
        assert!(stable.is_empty());
        // check stable clocks
        assert_eq!(stable_clock(&table, &key_a), 1);
        assert_eq!(stable_clock(&table, &key_b), 1);
    }
}
