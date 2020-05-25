// This module contains the definition of `TarjanSCCFinder` and `FinderResult`.
mod tarjan;

/// This module contains the definition of `VertexIndex` and `PendingIndex`.
mod index;

/// This modules contains the definition of `GraphExecutor` and
/// `GraphExecutionInfo`.
mod executor;

// Re-exports.
pub use executor::{GraphExecutionInfo, GraphExecutor};

use self::index::{PendingIndex, VertexIndex};
use self::tarjan::{FinderResult, TarjanSCCFinder, Vertex, SCC};
use fantoch::command::Command;
use fantoch::config::Config;
use fantoch::id::{Dot, ProcessId};
use fantoch::log;
use fantoch::util;
use std::collections::HashSet;
use std::fmt;
use std::mem;
use threshold::{AEClock, VClock};

pub struct DependencyGraph {
    process_id: ProcessId,
    transitive_conflicts: bool,
    executed_clock: AEClock<ProcessId>,
    vertex_index: VertexIndex,
    pending_index: PendingIndex,
    to_execute: Vec<Command>,
}

enum FinderInfo {
    // set of dots in found SCCs
    Found(Vec<Dot>),
    // the missing dependency and set of dots visited while searching for SCCs
    MissingDependency(Dot, HashSet<Dot>),
    // in case we try to find SCCs on dots that are no longer pending
    NotPending,
}

impl DependencyGraph {
    /// Create a new `Graph`.
    pub fn new(process_id: ProcessId, config: &Config) -> Self {
        // create bottom executed clock
        let ids = util::process_ids(config.n());
        let executed_clock = AEClock::with(ids);
        // create indexes
        let vertex_index = VertexIndex::new();
        let pending_index = PendingIndex::new();
        // create to execute
        let to_execute = Vec::new();
        DependencyGraph {
            process_id,
            transitive_conflicts: config.transitive_conflicts(),
            executed_clock,
            vertex_index,
            pending_index,
            to_execute,
        }
    }

    /// Returns new commands ready to be executed.
    #[must_use]
    pub fn commands_to_execute(&mut self) -> Vec<Command> {
        mem::take(&mut self.to_execute)
    }

    /// Add a new command with its clock to the queue.
    pub fn add(&mut self, dot: Dot, cmd: Command, clock: VClock<ProcessId>) {
        log!("p{}: Graph::add {:?} {:?}", self.process_id, dot, clock);
        // create new vertex for this command
        let vertex = Vertex::new(dot, cmd, clock);

        // index in vertex index and check if it hasn't been indexed before
        assert!(self.vertex_index.index(vertex));

        // try to find new SCCs
        match self.find_scc(dot) {
            FinderInfo::Found(dots) => {
                // try to execute other commands if new SCCs were found
                self.try_pending(dots);
            }
            FinderInfo::MissingDependency(dep_dot, _visited) => {
                // update the pending
                self.pending_index.index(dep_dot, dot);
            }
            FinderInfo::NotPending => panic!("just added dot must be pending"),
        }
    }

    #[must_use]
    fn find_scc(&mut self, dot: Dot) -> FinderInfo {
        log!("p{}: Graph:find_scc {:?}", self.process_id, dot);
        // execute tarjan's algorithm
        let mut finder =
            TarjanSCCFinder::new(self.process_id, self.transitive_conflicts);
        let finder_result = self.strong_connect(&mut finder, dot);

        // get sccs
        let (sccs, visited) = finder.finalize(&mut self.vertex_index);

        // save new SCCs if any were found
        match finder_result {
            FinderResult::Found => {
                // create set of dots in ready SCCs
                let mut dots = Vec::new();

                // save new SCCs
                sccs.into_iter().for_each(|scc| {
                    self.save_scc(scc, &mut dots);
                });

                FinderInfo::Found(dots)
            }
            FinderResult::MissingDependency(dep_dot) => {
                FinderInfo::MissingDependency(dep_dot, visited)
            }
            FinderResult::NotPending => FinderInfo::NotPending,
            FinderResult::NotFound => panic!(
                "either there's a missing dependency, or we should find an SCC"
            ),
        }
    }

    fn save_scc(&mut self, scc: SCC, dots: &mut Vec<Dot>) {
        scc.into_iter().for_each(|dot| {
            log!(
                "p{}: Graph:save_scc removing {:?} from indexes",
                self.process_id,
                dot
            );

            // update executed clock
            assert!(self.executed_clock.add(&dot.source(), dot.sequence()));

            log!(
                "p{}: Graph:save_scc executed clock {:?}",
                self.process_id,
                self.executed_clock,
            );

            // remove from vertex index
            let vertex = self
                .vertex_index
                .remove(&dot)
                .expect("dots from an SCC should exist");

            // update the set of ready dots
            dots.push(dot);

            // add vertex to commands to be executed
            self.to_execute.push(vertex.into_command())
        })
    }

    fn try_pending(&mut self, mut dots: Vec<Dot>) {
        while let Some(dot) = dots.pop() {
            // get pending commands that depend on this dot
            if let Some(pending) = self.pending_index.remove(&dot) {
                log!(
                    "p{}: Graph::try_pending {:?} depended on {:?}",
                    self.process_id,
                    pending,
                    dot
                );
                // try to find new SCCs for each of those commands
                let mut visited = HashSet::new();

                for dot in pending {
                    // only try to find new SCCs from non-visited commands
                    if !visited.contains(&dot) {
                        match self.find_scc(dot) {
                            FinderInfo::Found(new_dots) => {
                                // if new SCCs were found, now there are more
                                // dots to check
                                dots.extend(new_dots);
                                // reset visited
                                visited.clear();
                            }
                            FinderInfo::NotPending => {
                                // this happens if the pending dot is no longer
                                // pending
                            }
                            FinderInfo::MissingDependency(
                                dep_dot,
                                new_visited,
                            ) => {
                                // update pending
                                self.pending_index.index(dep_dot, dot);
                                // if no SCCs were found, try other pending
                                // commands, but don't try those that were
                                // visited in this search
                                visited.extend(new_visited);
                            }
                        }
                    }
                }
            }
        }
        // once there are no more dots to try, no command in pending should be
        // possible to be executed, so we give up!
    }

    fn strong_connect(
        &mut self,
        finder: &mut TarjanSCCFinder,
        dot: Dot,
    ) -> FinderResult {
        // get the vertex
        match self.vertex_index.find(&dot) {
            Some(vertex) => finder.strong_connect(
                dot,
                vertex,
                &self.executed_clock,
                &self.vertex_index,
            ),
            None => {
                // in this case this `dot` is no longer pending
                FinderResult::NotPending
            }
        }
    }
}

impl fmt::Debug for DependencyGraph {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "vertex index:")?;
        write!(f, "{:#?}", self.vertex_index)?;
        write!(f, "pending index:")?;
        write!(f, "{:#?}", self.pending_index)?;
        write!(f, "executed:")?;
        write!(f, "{:?}", self.executed_clock)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util;
    use fantoch::id::Rifl;
    use permutator::{Combination, Permutation};
    use std::cell::RefCell;
    use std::collections::{HashMap, HashSet};

    #[test]
    fn simple() {
        // create queue
        let process_id = 1;
        let n = 2;
        let f = 1;
        let config = Config::new(n, f);
        let mut queue = DependencyGraph::new(process_id, &config);

        // cmd 0
        let dot_0 = Dot::new(1, 1);
        let cmd_0 =
            Command::put(Rifl::new(1, 1), String::from("A"), String::new());
        let clock_0 = util::vclock(vec![0, 1]);

        // cmd 1
        let dot_1 = Dot::new(2, 1);
        let cmd_1 =
            Command::put(Rifl::new(2, 1), String::from("A"), String::new());
        let clock_1 = util::vclock(vec![1, 0]);

        // add cmd 0
        queue.add(dot_0, cmd_0.clone(), clock_0);
        // check commands ready to be executed
        assert!(queue.commands_to_execute().is_empty());

        // add cmd 1
        queue.add(dot_1, cmd_1.clone(), clock_1);
        // check commands ready to be executed
        assert_eq!(queue.commands_to_execute(), vec![cmd_0, cmd_1]);
    }

    #[ignore]
    #[test]
    /// We have 5 commands by the same process (process A) that access the same
    /// key. We have `n = 5` and `f = 1` and thus the fast quorum size of 3.
    /// The fast quorum used by process A is `{A, B, C}`. We have the
    /// following order of commands in the 3 processes:
    /// - A: (1,1) (1,2) (1,3) (1,4) (1,5)
    /// - B: (1,3) (1,4) (1,1) (1,2) (1,5)
    /// - C: (1,1) (1,2) (1,4) (1,5) (1,3)
    ///
    /// The above results in the following final dependencies:
    /// - dep[(1,1)] = {(1,4)}
    /// - dep[(1,2)] = {(1,4)}
    /// - dep[(1,3)] = {(1,5)}
    /// - dep[(1,4)] = {(1,3)}
    /// - dep[(1,5)] = {(1,4)}
    ///
    /// The executor then receives the commit notifications of (1,3) (1,4) and
    /// (1,5) and, if transitive conflicts are assumed, these 3 commands
    /// form an SCC. This is because with this assumption we only check the
    /// highest conflicting command per replica, and thus (1,3) (1,4) and
    /// (1,5) have "all the dependencies".
    ///
    /// Then, the executor executes whichever missing command comes first, since
    /// (1,1) and (1,2) "only depend on an SCC already formed". This means that
    /// if two executors receive (1,3) (1,4) (1,5), and then one receives (1,1)
    /// and (1,2) while the other receives (1,2) and (1,1), they will execute
    /// (1,1) and (1,2) in differents order, leading to an inconsistency.
    ///
    /// This example is impossible if commands from the same process are
    /// processed (on the replicas computing dependencies) in their submission
    /// order. With this, a command never depends on later commands from the
    /// same process, which seems to be enough to prevent this issue. This means
    /// that parallelizing the processing of messages needs to be on a
    /// per-process basis, i.e. commands by the same process are always
    /// processed by the same worker.
    fn transitive_conflicts_assumption_regression_test() {
        // config
        let n = 5;
        let transitive_conflicts = true;

        // cmd 1
        let dot_1 = Dot::new(1, 1);
        let clock_1 = util::vclock(vec![4, 0, 0, 0, 0]);

        // cmd 2
        let dot_2 = Dot::new(1, 2);
        let clock_2 = util::vclock(vec![4, 0, 0, 0, 0]);

        // cmd 3
        let dot_3 = Dot::new(1, 3);
        let clock_3 = util::vclock(vec![5, 0, 0, 0, 0]);

        // cmd 4
        let dot_4 = Dot::new(1, 4);
        let clock_4 = util::vclock(vec![3, 0, 0, 0, 0]);

        // cmd 5
        let dot_5 = Dot::new(1, 5);
        let clock_5 = util::vclock(vec![4, 0, 0, 0, 0]);

        let order_a = vec![
            (dot_3, clock_2.clone()),
            (dot_4, clock_3.clone()),
            (dot_5, clock_4.clone()),
            (dot_1, clock_1.clone()),
            (dot_2, clock_1.clone()),
        ];
        let order_b = vec![
            (dot_3, clock_3),
            (dot_4, clock_4),
            (dot_5, clock_5),
            (dot_2, clock_2),
            (dot_1, clock_1),
        ];
        let order_a = check_termination(n, order_a, transitive_conflicts);
        let order_b = check_termination(n, order_b, transitive_conflicts);
        assert_eq!(order_a, order_b);
    }

    #[test]
    /// We have 3 commands by the same process:
    /// - the first (1,1) accesses key A and thus has no dependencies
    /// - the second (1,2) accesses key B and thus has no dependencies
    /// - the third (1,3) accesses key B and thus it depends on the second
    ///   command (1,2)
    ///
    /// The commands are then received in the following order:
    /// - (1,2) executed as it has no dependencies
    /// - (1,3) can't be executed: this command depends on (1,2) and if we don't
    ///   assume the transitivity of conflicts, it also depends on (1,1) that
    ///   hasn't been executed
    /// - (1,1) executed as it has no dependencies
    ///
    /// Now when (1,1) is executed, we would hope that (1,3) would also be.
    /// However, since (1,1) accesses key A, when it is executed, the previous
    /// pending mechanism doesn't try the pending operation (1,3) because it
    /// accesses a different key (B). This means that such mechanism to
    /// track pending commands per key does not work when we don't assume
    /// the transitivity of conflicts.
    ///
    /// This was fixed by simply tracking one missing dependency per pending
    /// command. When that dependency is executed, if the command still can't be
    /// executed it's because there's another missing dependency, and now it
    /// will wait for that one.
    fn pending_on_different_key_regression_test() {
        // create config
        let n = 1;
        let f = 1;
        let mut config = Config::new(n, f);

        // cmd 1
        let dot_1 = Dot::new(1, 1);
        let cmd_1 =
            Command::put(Rifl::new(1, 1), String::from("A"), String::new());
        let clock_1 = util::vclock(vec![0]);

        // cmd 2
        let dot_2 = Dot::new(1, 2);
        let cmd_2 =
            Command::put(Rifl::new(2, 1), String::from("B"), String::new());
        let clock_2 = util::vclock(vec![0]);

        // cmd 3
        let dot_3 = Dot::new(1, 3);
        let cmd_3 =
            Command::put(Rifl::new(3, 1), String::from("B"), String::new());
        let clock_3 = util::vclock(vec![2]);

        for transitive_conflicts in vec![false, true] {
            config.set_transitive_conflicts(transitive_conflicts);

            // create queue
            let process_id = 1;
            let mut queue = DependencyGraph::new(process_id, &config);

            // add cmd 2
            queue.add(dot_2, cmd_2.clone(), clock_2.clone());
            assert_eq!(queue.commands_to_execute(), vec![cmd_2.clone()]);

            // add cmd 3
            queue.add(dot_3, cmd_3.clone(), clock_3.clone());
            if transitive_conflicts {
                // if we assume transitive conflicts, then cmd 3 can be executed
                assert_eq!(queue.commands_to_execute(), vec![cmd_3.clone()]);
            } else {
                // otherwise, it can't as it also depends cmd 1
                assert!(queue.commands_to_execute().is_empty());
            }

            // add cmd 1
            queue.add(dot_1, cmd_1.clone(), clock_1.clone());
            // cmd 1 can always be executed
            if transitive_conflicts {
                assert_eq!(queue.commands_to_execute(), vec![cmd_1.clone()]);
            } else {
                // the following used to fail because our previous mechanism to
                // track pending commands didn't work without the assumption of
                // transitive conflicts
                let ready = queue.commands_to_execute();
                assert_eq!(ready.len(), 2);
                assert!(ready.contains(&cmd_3));
                assert!(ready.contains(&cmd_1));
            }
        }
    }

    #[test]
    fn simple_test_add_1() {
        // the actual test_add_1 is:
        // {1, 2}, [2, 2]
        // {1, 1}, [3, 2]
        // {1, 5}, [6, 2]
        // {1, 6}, [6, 3]
        // {1, 3}, [3, 3]
        // {2, 2}, [0, 2]
        // {2, 1}, [4, 3]
        // {1, 4}, [6, 2]
        // {2, 3}, [6, 3]
        // in the simple version, {1, 5} and {1, 6} are removed

        // {1, 2}, [2, 2]
        let dot_a = Dot::new(1, 2);
        let clock_a = util::vclock(vec![2, 2]);

        // {1, 1}, [3, 2]
        let dot_b = Dot::new(1, 1);
        let clock_b = util::vclock(vec![3, 2]);

        // {1, 3}, [3, 3]
        let dot_c = Dot::new(1, 3);
        let clock_c = util::vclock(vec![3, 3]);

        // {2, 2}, [0, 2]
        let dot_d = Dot::new(2, 2);
        let clock_d = util::vclock(vec![0, 2]);

        // {2, 1}, [4, 3]
        let dot_e = Dot::new(2, 1);
        let clock_e = util::vclock(vec![4, 3]);

        // {1, 4}, [4, 2]
        let dot_f = Dot::new(1, 4);
        let clock_f = util::vclock(vec![4, 2]);

        // {2, 3}, [4, 3]
        let dot_g = Dot::new(2, 3);
        let clock_g = util::vclock(vec![4, 3]);

        // create args
        let args = vec![
            (dot_a, clock_a),
            (dot_b, clock_b),
            (dot_c, clock_c),
            (dot_d, clock_d),
            (dot_e, clock_e),
            (dot_f, clock_f),
            (dot_g, clock_g),
        ];

        let n = 2;
        shuffle_it(n, args);
    }

    #[test]
    fn test_add_2() {
        // {2, 4}, [3, 4]
        let dot_a = Dot::new(2, 4);
        let clock_a = util::vclock(vec![3, 4]);

        // {2, 3}, [0, 3]
        let dot_b = Dot::new(2, 3);
        let clock_b = util::vclock(vec![0, 3]);

        // {1, 3}, [3, 3]
        let dot_c = Dot::new(1, 3);
        let clock_c = util::vclock(vec![3, 3]);

        // {1, 1}, [3, 4]
        let dot_d = Dot::new(1, 1);
        let clock_d = util::vclock(vec![3, 4]);

        // {2, 2}, [0, 2]
        let dot_e = Dot::new(2, 2);
        let clock_e = util::vclock(vec![0, 2]);

        // {1, 2}, [3, 3]
        let dot_f = Dot::new(1, 2);
        let clock_f = util::vclock(vec![3, 3]);

        // {2, 1}, [3, 3]
        let dot_g = Dot::new(2, 1);
        let clock_g = util::vclock(vec![3, 3]);

        // create args
        let args = vec![
            (dot_a, clock_a),
            (dot_b, clock_b),
            (dot_c, clock_c),
            (dot_d, clock_d),
            (dot_e, clock_e),
            (dot_f, clock_f),
            (dot_g, clock_g),
        ];

        let n = 2;
        shuffle_it(n, args);
    }

    #[test]
    fn test_add_3() {
        // {3, 2}, [1, 0, 2]
        let dot_a = Dot::new(3, 2);
        let clock_a = util::vclock(vec![1, 0, 2]);

        // {3, 3}, [1, 1, 3]
        let dot_b = Dot::new(3, 3);
        let clock_b = util::vclock(vec![1, 1, 3]);

        // {3, 1}, [1, 1, 3]
        let dot_c = Dot::new(3, 1);
        let clock_c = util::vclock(vec![1, 1, 3]);

        // {1, 1}, [1, 0, 0]
        let dot_d = Dot::new(1, 1);
        let clock_d = util::vclock(vec![1, 0, 0]);

        // {2, 1}, [1, 1, 2]
        let dot_e = Dot::new(2, 1);
        let clock_e = util::vclock(vec![1, 1, 2]);

        // create args
        let args = vec![
            (dot_a, clock_a),
            (dot_b, clock_b),
            (dot_c, clock_c),
            (dot_d, clock_d),
            (dot_e, clock_e),
        ];

        let n = 3;
        shuffle_it(n, args);
    }

    #[test]
    fn test_add_4() {
        // {1, 5}, [5]
        let dot_a = Dot::new(1, 5);
        let clock_a = util::vclock(vec![5]);

        // {1, 4}, [6]
        let dot_b = Dot::new(1, 4);
        let clock_b = util::vclock(vec![6]);

        // {1, 1}, [5]
        let dot_c = Dot::new(1, 1);
        let clock_c = util::vclock(vec![5]);

        // {1, 2}, [6]
        let dot_d = Dot::new(1, 2);
        let clock_d = util::vclock(vec![6]);

        // {1, 3}, [5]
        let dot_e = Dot::new(1, 3);
        let clock_e = util::vclock(vec![5]);

        // {1, 6}, [6]
        let dot_f = Dot::new(1, 6);
        let clock_f = util::vclock(vec![6]);

        // create args
        let args = vec![
            (dot_a, clock_a),
            (dot_b, clock_b),
            (dot_c, clock_c),
            (dot_d, clock_d),
            (dot_e, clock_e),
            (dot_f, clock_f),
        ];

        let n = 1;
        shuffle_it(n, args);
    }

    #[test]
    fn test_add_5() {
        // {1, 1}, [1, 1]
        let dot_a = Dot::new(1, 1);
        let clock_a = util::vclock(vec![1, 1]);

        // {1, 2}, [2, 0]
        let dot_b = Dot::new(1, 2);
        let clock_b = util::vclock(vec![2, 0]);

        // {2, 1}, [1, 1]
        let dot_c = Dot::new(2, 1);
        let clock_c = util::vclock(vec![1, 1]);

        // create args
        let args = vec![(dot_a, clock_a), (dot_b, clock_b), (dot_c, clock_c)];

        let n = 2;
        shuffle_it(n, args);
    }

    #[test]
    fn test_add_6() {
        // {1, 1}, [1, 0]
        let dot_a = Dot::new(1, 1);
        let clock_a = util::vclock(vec![1, 0]);

        // {1, 2}, [4, 1]
        let dot_b = Dot::new(1, 2);
        let clock_b = util::vclock(vec![4, 1]);

        // {1, 3}, [3, 0]
        let dot_c = Dot::new(1, 3);
        let clock_c = util::vclock(vec![3, 0]);

        // {1, 4}, [4, 0]
        let dot_d = Dot::new(1, 4);
        let clock_d = util::vclock(vec![4, 0]);

        // {2, 1}, [1, 1]
        let dot_e = Dot::new(2, 1);
        let clock_e = util::vclock(vec![1, 1]);

        // {2, 2}, [3, 2]
        let dot_f = Dot::new(2, 2);
        let clock_f = util::vclock(vec![3, 2]);

        // create args
        let args = vec![
            (dot_a, clock_a),
            (dot_b, clock_b),
            (dot_c, clock_c),
            (dot_d, clock_d),
            (dot_e, clock_e),
            (dot_f, clock_f),
        ];

        let n = 2;
        shuffle_it(n, args);
    }

    #[test]
    fn test_add_random() {
        let n = 2;
        let iterations = 10;
        let events_per_process = 3;

        (0..iterations).for_each(|_| {
            let args = random_adds(n, events_per_process);
            shuffle_it(n, args);
        });
    }

    fn random_adds(
        n: usize,
        events_per_process: usize,
    ) -> Vec<(Dot, VClock<ProcessId>)> {
        // create dots
        let dots: Vec<_> = util::process_ids(n)
            .flat_map(|process_id| {
                (1..=events_per_process)
                    .map(move |event| Dot::new(process_id, event as u64))
            })
            .collect();

        // create bottom clocks
        let clocks: HashMap<_, _> = dots
            .clone()
            .into_iter()
            .map(|dot| {
                let clock = VClock::with(util::process_ids(n));
                (dot, RefCell::new(clock))
            })
            .collect();

        // for each pair of dots
        dots.combination(2).for_each(|dots| {
            let left = dots[0];
            let right = dots[1];

            // find their clocks
            let mut left_clock = clocks
                .get(left)
                .expect("left clock must exist")
                .borrow_mut();
            let mut right_clock = clocks
                .get(right)
                .expect("right clock must exist")
                .borrow_mut();

            // and make sure at least one is a dependency of the other
            match rand::random::<usize>() % 3 {
                0 => {
                    // left depends on right
                    left_clock.add(&right.source(), right.sequence());
                }
                1 => {
                    // right depends on left
                    right_clock.add(&left.source(), left.sequence());
                }
                2 => {
                    // both
                    left_clock.add(&right.source(), right.sequence());
                    right_clock.add(&left.source(), left.sequence());
                }
                _ => panic!("usize % 3 must < 3"),
            }
        });

        // return mapping from dot to its clock
        clocks
            .into_iter()
            .map(|(dot, clock_cell)| {
                let clock = clock_cell.into_inner();
                (dot, clock)
            })
            .collect()
    }

    fn shuffle_it(n: usize, mut args: Vec<(Dot, VClock<ProcessId>)>) {
        let transitive_conflicts = false;
        let total_order =
            check_termination(n, args.clone(), transitive_conflicts);
        args.permutation().for_each(|permutation| {
            println!("permutation = {:?}", permutation);
            let sorted =
                check_termination(n, permutation, transitive_conflicts);
            assert_eq!(total_order, sorted);
        });
    }

    fn check_termination(
        n: usize,
        args: Vec<(Dot, VClock<ProcessId>)>,
        transitive_conflicts: bool,
    ) -> Vec<Rifl> {
        // create queue
        let process_id = 1;
        let f = 1;
        let mut config = Config::new(n, f);
        config.set_transitive_conflicts(transitive_conflicts);
        let mut queue = DependencyGraph::new(process_id, &config);
        let mut all_rifls = HashSet::new();
        let mut sorted = Vec::new();

        args.into_iter().for_each(|(dot, clock)| {
            // create command rifl from its dot
            let rifl = Rifl::new(dot.source(), dot.sequence());

            // create command
            let key = String::from("black");
            let value = String::from("");
            let cmd = Command::put(rifl, key, value);

            // add to the set of all rifls
            assert!(all_rifls.insert(rifl));

            // add it to the queue
            queue.add(dot, cmd, clock);

            // get ready to execute
            let to_execute = queue.commands_to_execute();

            // for each command ready to be executed
            to_execute.iter().for_each(|cmd| {
                // get its rifl
                let rifl = cmd.rifl();

                // remove it from the set of rifls
                assert!(all_rifls.remove(&cmd.rifl()));

                // and add it to the sorted results
                sorted.push(rifl);
            });
        });

        // the set of all rifls should be empty
        if !all_rifls.is_empty() {
            panic!("the set of all rifls should be empty");
        }

        // return sorted commands
        sorted
    }
}
