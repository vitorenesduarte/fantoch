/// This modules contains the definition of `TableExecutor` and
/// `TableExecutionInfo`.
mod executor;

// Re-exports.
pub use executor::{TableExecutionInfo, TableExecutor};

use crate::protocol::common::table::VoteRange;
use executor::Pending;
use fantoch::id::{Dot, ProcessId, ShardId};
use fantoch::kvs::Key;
use fantoch::trace;
use fantoch::util;
use fantoch::HashMap;
use std::collections::BTreeMap;
use std::mem;
use threshold::{ARClock, EventSet};

type SortId = (u64, Dot);

#[derive(Clone)]
pub struct MultiVotesTable {
    process_id: ProcessId,
    shard_id: ShardId,
    n: usize,
    stability_threshold: usize,
    tables: HashMap<Key, VotesTable>,
}

impl MultiVotesTable {
    /// Create a new `MultiVotesTable` instance given the stability threshold.
    pub fn new(
        process_id: ProcessId,
        shard_id: ShardId,
        n: usize,
        stability_threshold: usize,
    ) -> Self {
        Self {
            process_id,
            shard_id,
            n,
            stability_threshold,
            tables: HashMap::new(),
        }
    }

    /// Add a new command, its clock and votes to the votes table.
    pub fn add_attached_votes(
        &mut self,
        dot: Dot,
        clock: u64,
        key: &Key,
        pending: Pending,
        votes: Vec<VoteRange>,
    ) -> impl Iterator<Item = Pending> {
        // add ops and votes to the votes tables, and at the same time
        // compute which ops are safe to be executed
        self.update_table(key, |table| {
            table.add_attached_votes(dot, clock, pending, votes);
            table.stable_ops()
        })
    }

    /// Adds detached votes to the votes table.
    pub fn add_detached_votes(
        &mut self,
        key: &Key,
        votes: Vec<VoteRange>,
    ) -> impl Iterator<Item = Pending> {
        // add detached votes to the votes tables, and at the same time compute
        // which ops are safe to be executed
        self.update_table(key, |table| {
            table.add_detached_votes(votes);
            table.stable_ops()
        })
    }

    // Generic function to be used when updating some votes table.
    #[must_use]
    fn update_table<F, I>(&mut self, key: &Key, update: F) -> I
    where
        F: FnOnce(&mut VotesTable) -> I,
        I: Iterator<Item = Pending>,
    {
        let table = match self.tables.get_mut(key) {
            Some(table) => table,
            None => {
                // table does not exist, let's create a new one and insert it
                let table = VotesTable::new(
                    key.clone(),
                    self.process_id,
                    self.shard_id,
                    self.n,
                    self.stability_threshold,
                );
                self.tables.entry(key.clone()).or_insert(table)
            }
        };
        // update table
        update(table)
    }
}

#[derive(Clone)]
struct VotesTable {
    key: Key,
    process_id: ProcessId,
    n: usize,
    stability_threshold: usize,
    // `votes_clock` collects all votes seen until now so that we can compute
    // which timestamp is stable
    votes_clock: ARClock<ProcessId>,
    // this buffer saves us always allocating a vector when computing the
    // stable clock (see `stable_clock`)
    frontiers_buffer: Vec<u64>,
    ops: BTreeMap<SortId, Pending>,
}

impl VotesTable {
    fn new(
        key: Key,
        process_id: ProcessId,
        shard_id: ShardId,
        n: usize,
        stability_threshold: usize,
    ) -> Self {
        let ids = util::process_ids(shard_id, n);
        let votes_clock = ARClock::with(ids);
        let frontiers_buffer = Vec::with_capacity(n);
        Self {
            key,
            process_id,
            n,
            stability_threshold,
            votes_clock,
            frontiers_buffer,
            ops: BTreeMap::new(),
        }
    }

    fn add_attached_votes(
        &mut self,
        dot: Dot,
        clock: u64,
        pending: Pending,
        votes: Vec<VoteRange>,
    ) {
        // create sort identifier:
        // - if two ops got assigned the same clock, they will be ordered by
        //   their dot
        let sort_id = (clock, dot);

        trace!(
            "p{}: key={} Table::add {:?} {:?} | sort id {:?}",
            self.process_id,
            self.key,
            dot,
            clock,
            sort_id
        );

        // add op to the sorted list of ops to be executed
        let res = self.ops.insert(sort_id, pending);
        // and check there was nothing there for this exact same position
        assert!(res.is_none());

        // update votes with the votes used on this command
        self.add_detached_votes(votes);
    }

    fn add_detached_votes(&mut self, votes: Vec<VoteRange>) {
        trace!(
            "p{}: key={} Table::add_votes votes: {:?}",
            self.process_id,
            self.key,
            votes
        );
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
        trace!(
            "p{}: key={} Table::add_votes votes_clock: {:?}",
            self.process_id,
            self.key,
            self.votes_clock
        );
    }

    fn stable_ops(&mut self) -> impl Iterator<Item = Pending> {
        // compute *next* stable sort id:
        // - if clock 10 is stable, then we can execute all ops with an id
        //   smaller than `(11,0)`
        // - if id with `(11,0)` is also part of this local structure, we can
        //   also execute it without 11 being stable, because, once 11 is
        //   stable, it will be the first to be executed either way
        let stable_clock = self.stable_clock();
        trace!(
            "p{}: key={} Table::stable_ops stable_clock: {:?}",
            self.process_id,
            self.key,
            stable_clock
        );

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
            // now remaining contains what's the stable
            remaining
        };

        trace!(
            "p{}: key={} Table::stable_ops stable dots: {:?}",
            self.process_id,
            self.key,
            stable.iter().map(|((_, dot), _)| *dot).collect::<Vec<_>>()
        );

        // return stable ops
        stable.into_iter().map(|(_, pending)| pending)
    }

    // Computes the (potentially) new stable clock in this table.
    fn stable_clock(&mut self) -> u64 {
        // NOTE: we don't use `self.votes_clocks.frontier_threshold` function in
        // order to save us an allocation
        let clock_size = self.votes_clock.len();
        if self.stability_threshold <= clock_size {
            // clear current frontiers
            self.frontiers_buffer.clear();

            // get frontiers and sort them
            for (_, eset) in self.votes_clock.iter() {
                self.frontiers_buffer.push(eset.frontier());
            }
            self.frontiers_buffer.sort_unstable();

            // get the frontier at the correct threshold
            *self
                .frontiers_buffer
                .iter()
                .nth(clock_size - self.stability_threshold)
                .expect("there should be a stable clock")
        } else {
            panic!("stability threshold must always be smaller than the number of processes")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fantoch::command::DEFAULT_SHARD_ID;
    use fantoch::id::{ClientId, Rifl};
    use fantoch::kvs::KVOp;
    use permutator::Permutation;
    use std::sync::Arc;

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
        let process_id = 1;
        let shard_id = 0;
        let n = 5;
        let stability_threshold = 3;
        let mut table = VotesTable::new(
            String::from("KEY"),
            process_id,
            shard_id,
            n,
            stability_threshold,
        );

        // in this example we'll use the dot as rifl;
        // also, all commands access a single key
        let pending = |value: &'static str, rifl: Rifl| -> Pending {
            let shard_to_keys = Arc::new(
                vec![(DEFAULT_SHARD_ID, vec!["KEY".to_string()])]
                    .into_iter()
                    .collect(),
            );
            let ops = Arc::new(vec![KVOp::Put(String::from(value))]);
            Pending::new(DEFAULT_SHARD_ID, rifl, shard_to_keys, ops)
        };

        // a1
        let a1 = "A1";
        // assumes a single client per process that has the same id as the
        // process
        // p1, final clock = 1
        let a1_dot = Dot::new(process_id_1, 1);
        let a1_clock = 1;
        let a1_rifl = Rifl::new(process_id_1 as ClientId, 1);
        // p1, p2 and p3 voted with 1
        let a1_votes = vec![
            VoteRange::new(process_id_1, 1, 1),
            VoteRange::new(process_id_2, 1, 1),
            VoteRange::new(process_id_3, 1, 1),
        ];

        // c1
        let c1 = "C1";
        // p3, final clock = 3
        let c1_dot = Dot::new(process_id_3, 1);
        let c1_clock = 3;
        let c1_rifl = Rifl::new(process_id_3 as ClientId, 1);
        // p1 voted with 2, p2 voted with 3 and p3 voted with 2
        let c1_votes = vec![
            VoteRange::new(process_id_1, 2, 2),
            VoteRange::new(process_id_2, 3, 3),
            VoteRange::new(process_id_3, 2, 2),
        ];

        // d1
        let d1 = "D1";
        // p4, final clock = 3
        let d1_dot = Dot::new(process_id_4, 1);
        let d1_clock = 3;
        let d1_rifl = Rifl::new(process_id_4 as ClientId, 1);
        // p2 voted with 2, p3 voted with 3 and p4 voted with 1-3
        let d1_votes = vec![
            VoteRange::new(process_id_2, 2, 2),
            VoteRange::new(process_id_3, 3, 3),
            VoteRange::new(process_id_4, 1, 3),
        ];

        // e1
        let e1 = "E1";
        // p5, final clock = 4
        let e1_dot = Dot::new(process_id_5, 1);
        let e1_clock = 4;
        let e1_rifl = Rifl::new(process_id_5 as ClientId, 1);
        // p1 voted with 3, p4 voted with 4 and p5 voted with 1-4
        let e1_votes = vec![
            VoteRange::new(process_id_1, 3, 3),
            VoteRange::new(process_id_4, 4, 4),
            VoteRange::new(process_id_5, 1, 4),
        ];

        // e2
        let e2 = "E2";
        // p5, final clock = 5
        let e2_dot = Dot::new(process_id_5, 2);
        let e2_clock = 5;
        let e2_rifl = Rifl::new(process_id_5 as ClientId, 2);
        // p1 voted with 4-5, p4 voted with 5 and p5 voted with 5
        let e2_votes = vec![
            VoteRange::new(process_id_1, 4, 5),
            VoteRange::new(process_id_4, 5, 5),
            VoteRange::new(process_id_5, 5, 5),
        ];

        // add a1 to table
        table.add_attached_votes(
            a1_dot,
            a1_clock,
            pending(a1, a1_rifl),
            a1_votes.clone(),
        );
        // get stable: a1
        let stable = table.stable_ops().collect::<Vec<_>>();
        assert_eq!(stable, vec![pending(a1, a1_rifl)]);

        // add d1 to table
        table.add_attached_votes(
            d1_dot,
            d1_clock,
            pending(d1, d1_rifl),
            d1_votes.clone(),
        );
        // get stable: none
        let stable = table.stable_ops().collect::<Vec<_>>();
        assert_eq!(stable, vec![]);

        // add c1 to table
        table.add_attached_votes(
            c1_dot,
            c1_clock,
            pending(c1, c1_rifl),
            c1_votes.clone(),
        );
        // get stable: c1 then d1
        let stable = table.stable_ops().collect::<Vec<_>>();
        assert_eq!(stable, vec![pending(c1, c1_rifl), pending(d1, d1_rifl)]);

        // add e2 to table
        table.add_attached_votes(
            e2_dot,
            e2_clock,
            pending(e2, e2_rifl),
            e2_votes.clone(),
        );
        // get stable: none
        let stable = table.stable_ops().collect::<Vec<_>>();
        assert_eq!(stable, vec![]);

        // add e1 to table
        table.add_attached_votes(
            e1_dot,
            e1_clock,
            pending(e1, e1_rifl),
            e1_votes.clone(),
        );
        // get stable: none
        let stable = table.stable_ops().collect::<Vec<_>>();
        assert_eq!(stable, vec![pending(e1, e1_rifl), pending(e2, e2_rifl)]);

        // run all the permutations of the above and check that the final total
        // order is the same
        let total_order = vec![
            pending(a1, a1_rifl),
            pending(c1, c1_rifl),
            pending(d1, d1_rifl),
            pending(e1, e1_rifl),
            pending(e2, e2_rifl),
        ];
        let mut all_ops = vec![
            (a1_dot, a1_clock, pending(a1, a1_rifl), a1_votes),
            (c1_dot, c1_clock, pending(c1, c1_rifl), c1_votes),
            (d1_dot, d1_clock, pending(d1, d1_rifl), d1_votes),
            (e1_dot, e1_clock, pending(e1, e1_rifl), e1_votes),
            (e2_dot, e2_clock, pending(e2, e2_rifl), e2_votes),
        ];

        all_ops.permutation().for_each(|p| {
            let mut table = VotesTable::new(
                String::from("KEY"),
                process_id_1,
                shard_id,
                n,
                stability_threshold,
            );
            let permutation_total_order: Vec<_> = p
                .clone()
                .into_iter()
                .flat_map(|(dot, clock, pending, votes)| {
                    table.add_attached_votes(dot, clock, pending, votes);
                    table.stable_ops()
                })
                .collect();
            assert_eq!(total_order, permutation_total_order);
        });
    }

    #[test]
    fn votes_table_tiny_quorums() {
        let shard_id = 0;

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
        let mut table = VotesTable::new(
            String::from("KEY"),
            process_id_1,
            shard_id,
            n,
            stability_threshold,
        );

        // in this example we'll use the dot as rifl;
        // also, all commands access a single key
        let pending = |value: &'static str, rifl: Rifl| -> Pending {
            let shard_to_keys = Arc::new(
                vec![(DEFAULT_SHARD_ID, vec!["KEY".to_string()])]
                    .into_iter()
                    .collect(),
            );
            let ops = Arc::new(vec![KVOp::Put(String::from(value))]);
            Pending::new(DEFAULT_SHARD_ID, rifl, shard_to_keys, ops)
        };

        // a1
        let a1 = "A1";
        // p1, final clock = 1
        let a1_dot = Dot::new(process_id_1, 1);
        let a1_clock = 1;
        let a1_rifl = Rifl::new(process_id_1 as ClientId, 1);
        // p1, p2 voted with  1
        let a1_votes = vec![
            VoteRange::new(process_id_1, 1, 1),
            VoteRange::new(process_id_2, 1, 1),
        ];

        // add a1 to table
        table.add_attached_votes(
            a1_dot,
            a1_clock,
            pending(a1, a1_rifl),
            a1_votes.clone(),
        );
        // get stable: none
        let stable = table.stable_ops().collect::<Vec<_>>();
        assert_eq!(stable, vec![]);

        // c1
        let c1 = "C1";
        // p3, final clock = 2
        let c1_dot = Dot::new(process_id_3, 1);
        let c1_clock = 2;
        let c1_rifl = Rifl::new(process_id_3 as ClientId, 1);
        // p2 voted with 2, p3 voted with 1-2
        let c1_votes = vec![
            VoteRange::new(process_id_3, 1, 1),
            VoteRange::new(process_id_2, 2, 2),
            VoteRange::new(process_id_3, 2, 2),
        ];

        // add c1 to table
        table.add_attached_votes(
            c1_dot,
            c1_clock,
            pending(c1, c1_rifl),
            c1_votes.clone(),
        );
        // get stable: none
        let stable = table.stable_ops().collect::<Vec<_>>();
        assert_eq!(stable, vec![]);

        // e1
        let e1 = "E1";
        // p5, final clock = 1
        let e1_dot = Dot::new(process_id_5, 1);
        let e1_clock = 1;
        let e1_rifl = Rifl::new(process_id_5 as ClientId, 1);
        // p5 and p4 voted with 1
        let e1_votes = vec![
            VoteRange::new(process_id_5, 1, 1),
            VoteRange::new(process_id_4, 1, 1),
        ];

        // add e1 to table
        table.add_attached_votes(
            e1_dot,
            e1_clock,
            pending(e1, e1_rifl),
            e1_votes.clone(),
        );
        // get stable: a1 and e1
        let stable = table.stable_ops().collect::<Vec<_>>();
        assert_eq!(stable, vec![pending(a1, a1_rifl), pending(e1, e1_rifl)]);

        // a2
        let a2 = "A2";
        // p1, final clock = 3
        let a2_dot = Dot::new(process_id_1, 2);
        let a2_clock = 3;
        let a2_rifl = Rifl::new(process_id_1 as ClientId, 2);
        // p1 voted with 2-3 and p2 voted with 3
        let a2_votes = vec![
            VoteRange::new(process_id_1, 2, 2),
            VoteRange::new(process_id_2, 3, 3),
            VoteRange::new(process_id_1, 3, 3),
        ];

        // add a2 to table
        table.add_attached_votes(
            a2_dot,
            a2_clock,
            pending(a2, a2_rifl),
            a2_votes.clone(),
        );
        // get stable: none
        let stable = table.stable_ops().collect::<Vec<_>>();
        assert_eq!(stable, vec![]);

        // d1
        let d1 = "D1";
        // p4, final clock = 3
        let d1_dot = Dot::new(process_id_4, 1);
        let d1_clock = 3;
        let d1_rifl = Rifl::new(process_id_4 as ClientId, 1);
        // p4 voted with 2-3 and p3 voted with 3
        let d1_votes = vec![
            VoteRange::new(process_id_4, 2, 2),
            VoteRange::new(process_id_3, 3, 3),
            VoteRange::new(process_id_4, 3, 3),
        ];

        // add d1 to table
        table.add_attached_votes(
            d1_dot,
            d1_clock,
            pending(d1, d1_rifl),
            d1_votes.clone(),
        );
        // get stable
        let stable = table.stable_ops().collect::<Vec<_>>();
        assert_eq!(
            stable,
            vec![
                pending(c1, c1_rifl),
                pending(a2, a2_rifl),
                pending(d1, d1_rifl),
            ]
        );
    }

    #[test]
    fn detached_votes() {
        let shard_id = 0;

        // create table
        let process_id = 1;
        let n = 5;
        let stability_threshold = 3;
        let mut table =
            MultiVotesTable::new(process_id, shard_id, n, stability_threshold);

        // create keys
        let key_a = String::from("A");
        let key_b = String::from("B");

        // closure to compute the stable clock for some key
        let stable_clock = |table: &mut MultiVotesTable, key: &Key| {
            table
                .tables
                .get_mut(key)
                .expect("table for this key should exist")
                .stable_clock()
        };

        // p1 votes on key A
        let process_id = 1;
        let stable = table
            .add_detached_votes(&key_a, vec![VoteRange::new(process_id, 1, 1)])
            .collect::<Vec<_>>();
        assert!(stable.is_empty());
        // check stable clocks
        assert_eq!(stable_clock(&mut table, &key_a), 0);

        // p1 votes on key b
        let stable = table
            .add_detached_votes(&key_b, vec![VoteRange::new(process_id, 1, 1)])
            .collect::<Vec<_>>();
        assert!(stable.is_empty());
        // check stable clocks
        assert_eq!(stable_clock(&mut table, &key_a), 0);
        assert_eq!(stable_clock(&mut table, &key_b), 0);

        // p2 votes on key A
        let process_id = 2;
        let stable = table
            .add_detached_votes(&key_a, vec![VoteRange::new(process_id, 1, 1)])
            .collect::<Vec<_>>();
        assert!(stable.is_empty());
        // check stable clocks
        assert_eq!(stable_clock(&mut table, &key_a), 0);
        assert_eq!(stable_clock(&mut table, &key_b), 0);

        // p3 votes on key A
        let process_id = 3;
        let stable = table
            .add_detached_votes(&key_a, vec![VoteRange::new(process_id, 1, 1)])
            .collect::<Vec<_>>();
        assert!(stable.is_empty());
        // check stable clocks
        assert_eq!(stable_clock(&mut table, &key_a), 1);
        assert_eq!(stable_clock(&mut table, &key_b), 0);

        // p3 votes on key B
        let stable = table
            .add_detached_votes(&key_b, vec![VoteRange::new(process_id, 1, 1)])
            .collect::<Vec<_>>();
        assert!(stable.is_empty());
        // check stable clocks
        assert_eq!(stable_clock(&mut table, &key_a), 1);
        assert_eq!(stable_clock(&mut table, &key_b), 0);

        // p4 votes on key B
        let process_id = 4;
        let stable = table
            .add_detached_votes(&key_b, vec![VoteRange::new(process_id, 1, 1)])
            .collect::<Vec<_>>();
        assert!(stable.is_empty());
        // check stable clocks
        assert_eq!(stable_clock(&mut table, &key_a), 1);
        assert_eq!(stable_clock(&mut table, &key_b), 1);
    }
}
