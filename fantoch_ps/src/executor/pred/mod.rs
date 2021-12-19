/// This module contains the definition of `Vertex`, `VertexIndex` and
/// `PendingIndex`.
mod index;

/// This modules contains the definition of `PredecessorsExecutor` and
/// `PredecessorsExecutionInfo`.
mod executor;

// Re-exports.
pub use executor::{PredecessorsExecutionInfo, PredecessorsExecutor};

use self::index::{PendingIndex, Vertex, VertexIndex};
use crate::protocol::common::pred::{CaesarDeps, Clock};
use fantoch::command::Command;
use fantoch::config::Config;
use fantoch::executor::{ExecutorMetrics, ExecutorMetricsKind};
use fantoch::id::{Dot, ProcessId};
use fantoch::protocol::CommittedAndExecuted;
use fantoch::time::SysTime;
use fantoch::util;
use fantoch::{debug, trace};
use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;
use threshold::AEClock;

#[derive(Clone)]
pub struct PredecessorsGraph {
    process_id: ProcessId,
    committed_clock: AEClock<ProcessId>,
    executed_clock: AEClock<ProcessId>,
    vertex_index: VertexIndex,
    // mapping from non committed dep to pending dot
    phase_one_pending_index: PendingIndex,
    // mapping from committed (but not executed) dep to pending dot
    phase_two_pending_index: PendingIndex,
    metrics: ExecutorMetrics,
    // count of committed commands
    new_committed_dots: u64,
    // dots of new commands executed
    new_executed_dots: Vec<Dot>,
    to_execute: VecDeque<Command>,
    execute_at_commit: bool,
}

impl PredecessorsGraph {
    /// Create a new `Graph`.
    pub fn new(process_id: ProcessId, config: &Config) -> Self {
        // create executed clock and its snapshot
        let ids: Vec<_> =
            util::all_process_ids(config.shard_count(), config.n())
                .map(|(process_id, _)| process_id)
                .collect();
        let committed_clock = AEClock::with(ids.clone());
        let executed_clock = AEClock::with(ids.clone());
        // create indexes
        let vertex_index = VertexIndex::new();
        let phase_one_pending_index = PendingIndex::new();
        let phase_two_pending_index = PendingIndex::new();
        let metrics = ExecutorMetrics::new();
        let new_committed_dots = 0;
        let new_executed_dots = Vec::new();
        // create to execute
        let to_execute = VecDeque::new();
        let execute_at_commit = config.execute_at_commit();
        PredecessorsGraph {
            process_id,
            executed_clock,
            committed_clock,
            vertex_index,
            phase_one_pending_index,
            phase_two_pending_index,
            metrics,
            new_committed_dots,
            new_executed_dots,
            to_execute,
            execute_at_commit,
        }
    }

    /// Returns a new command ready to be executed.
    #[must_use]
    pub fn command_to_execute(&mut self) -> Option<Command> {
        self.to_execute.pop_front()
    }

    #[cfg(test)]
    fn commands_to_execute(&mut self) -> VecDeque<Command> {
        std::mem::take(&mut self.to_execute)
    }

    fn committed_and_executed(&mut self) -> CommittedAndExecuted {
        (
            std::mem::take(&mut self.new_committed_dots),
            std::mem::take(&mut self.new_executed_dots),
        )
    }

    fn metrics(&self) -> &ExecutorMetrics {
        &self.metrics
    }

    /// Add a new command.
    pub fn add(
        &mut self,
        dot: Dot,
        cmd: Command,
        clock: Clock,
        deps: Arc<CaesarDeps>,
        time: &dyn SysTime,
    ) {
        debug!(
            "p{}: Predecessors::add {:?} {:?} {:?} | time = {}",
            self.process_id,
            dot,
            clock,
            deps,
            time.millis()
        );

        // mark dot as committed
        self.new_committed_dots += 1;
        assert!(self.committed_clock.add(&dot.source(), dot.sequence()));

        // we assume that commands to not depend on themselves
        assert!(!deps.contains(&dot));

        if self.execute_at_commit {
            self.execute(dot, cmd, time);
        } else {
            // index the command
            self.index_committed_command(dot, cmd, clock, deps, time);

            // try all commands that are pending on phase one due to this
            // command
            self.try_phase_one_pending(dot, time);

            // move command to phase 1
            self.move_to_phase_one(dot, time);

            trace!(
                "p{}: Predecessors::log committed {:?} | executed {:?} | index {:?} | time = {}",
                self.process_id,
                self.committed_clock,
                self.executed_clock,
                self.vertex_index
                    .dots()
                    .collect::<std::collections::BTreeSet<_>>(),
                time.millis()
            );
        }
    }

    fn move_to_phase_one(&mut self, dot: Dot, time: &dyn SysTime) {
        debug!(
            "p{}: Predecessors::move_1 {:?} | time = {}",
            self.process_id,
            dot,
            time.millis()
        );

        // get vertex
        let vertex_ref = self
            .vertex_index
            .find(&dot)
            .expect("command just indexed must exist");
        let mut vertex = vertex_ref.borrow_mut();

        // compute number of non yet committed dependencies
        let mut non_committed_deps_count = 0;
        for dep_dot in vertex.deps.iter() {
            let committed = self
                .committed_clock
                .contains(&dep_dot.source(), dep_dot.sequence());

            if !committed {
                trace!(
                    "p{}: Predecessors::move_1 non committed dep {:?} | time = {}",
                    self.process_id,
                    dep_dot,
                    time.millis()
                );
                non_committed_deps_count += 1;
                self.phase_one_pending_index.index(dot, *dep_dot);
            }
        }

        trace!(
            "p{}: Predecessors::move_1 {:?} missing deps for {:?} | time = {}",
            self.process_id,
            non_committed_deps_count,
            dot,
            time.millis()
        );

        if non_committed_deps_count > 0 {
            // if it has non committed deps, simply save that value
            vertex.set_missing_deps(non_committed_deps_count);
        } else {
            // move command to phase two
            drop(vertex);
            self.move_to_phase_two(dot, time);
        }
    }

    /// Moves a command to phase two, i.e., where it waits for all its
    /// dependencies to become executed.
    fn move_to_phase_two(&mut self, dot: Dot, time: &dyn SysTime) {
        debug!(
            "p{}: Predecessors::move_2 {:?} | time = {}",
            self.process_id,
            dot,
            time.millis()
        );

        // get vertex
        let vertex_ref = self
            .vertex_index
            .find(&dot)
            .expect("command moved to phase two must exist");
        let mut vertex = vertex_ref.borrow_mut();

        // compute number of yet executed dependencies
        let mut non_executed_deps_count = 0;
        for dep_dot in vertex.deps.iter() {
            // consider only non-executed dependencies with a lower clock
            let executed = self
                .executed_clock
                .contains(&dep_dot.source(), dep_dot.sequence());
            if !executed {
                trace!(
                    "p{}: Predecessors::move_2 non executed dep {:?} | time = {}",
                    self.process_id,
                    dep_dot,
                    time.millis()
                );
                // get the dependency and check its clock to see if it should be
                // consider
                let dep_ref = self
                    .vertex_index
                    .find(&dep_dot)
                    .expect("non-executed dependency must exist");
                let dep = dep_ref.borrow();

                // only consider this dep if it has a lower clock
                if dep.clock < vertex.clock {
                    trace!(
                        "p{}: Predecessors::move_2 non executed dep with lower clock {:?} | time = {}",
                        self.process_id,
                        dep_dot,
                        time.millis()
                    );
                    non_executed_deps_count += 1;
                    self.phase_two_pending_index.index(dot, *dep_dot);
                }
            }
        }

        trace!(
            "p{}: Predecessors::move_2 {:?} missing deps for {:?} | time = {}",
            self.process_id,
            non_executed_deps_count,
            dot,
            time.millis()
        );

        if non_executed_deps_count > 0 {
            // if it has committed but non executed deps, simply save that value
            vertex.set_missing_deps(non_executed_deps_count);
        } else {
            // save the command to be executed
            drop(vertex);
            self.save_to_execute(dot, time);
        }
    }

    fn index_committed_command(
        &mut self,
        dot: Dot,
        cmd: Command,
        clock: Clock,
        deps: Arc<CaesarDeps>,
        time: &dyn SysTime,
    ) {
        // create new vertex for this command and index it
        let vertex = Vertex::new(dot, cmd, clock, deps, time);
        if self.vertex_index.index(vertex).is_some() {
            panic!(
                "p{}: Predecessors::index tried to index already indexed {:?}",
                self.process_id, dot
            );
        }
    }

    fn try_phase_one_pending(&mut self, dot: Dot, time: &dyn SysTime) {
        for pending_dot in self.phase_one_pending_index.remove(&dot) {
            // get vertex
            let vertex_ref = self
                .vertex_index
                .find(&pending_dot)
                .expect("command pending at phase one must exist");
            let mut vertex = vertex_ref.borrow_mut();

            // a non-committed dep became committed, so update the number of
            // missing deps at phase one
            vertex.decrease_missing_deps();

            // check if there are no more missing deps, and if so, move the
            // command to phase two
            if vertex.get_missing_deps() == 0 {
                // move command to phase two
                drop(vertex);
                self.move_to_phase_two(pending_dot, time);
            }
        }
    }

    fn try_phase_two_pending(&mut self, dot: Dot, time: &dyn SysTime) {
        for pending_dot in self.phase_two_pending_index.remove(&dot) {
            // get vertex
            let vertex_ref = self
                .vertex_index
                .find(&pending_dot)
                .expect("command pending at phase two must exist");
            let mut vertex = vertex_ref.borrow_mut();

            // a non-executed dep became executed, so update the number of
            // missing deps at phase two
            vertex.decrease_missing_deps();

            // check if there are no more missing deps, and if so, save the
            // command to be executed
            if vertex.get_missing_deps() == 0 {
                // save the command to be executed
                drop(vertex);
                self.save_to_execute(pending_dot, time);
            }
        }
    }

    fn save_to_execute(&mut self, dot: Dot, time: &dyn SysTime) {
        trace!(
            "p{}: Predecessors::save removing {:?} from indexes | time = {}",
            self.process_id,
            dot,
            time.millis()
        );

        // remove from vertex index
        let vertex = self
            .vertex_index
            .remove(&dot)
            .expect("ready-to-execute command should exist");

        // get command
        let (duration_ms, cmd) = vertex.into_command(time);

        // save execution delay metric
        self.metrics
            .collect(ExecutorMetricsKind::ExecutionDelay, duration_ms);

        // mark dot as executed and add command to commands to be executed
        self.execute(dot, cmd, time);

        // try commands pending at phase two due to this command
        self.try_phase_two_pending(dot, time);
    }

    fn execute(&mut self, dot: Dot, cmd: Command, _time: &dyn SysTime) {
        trace!(
            "p{}: Predecessors::update_executed {:?} | time = {}",
            self.process_id,
            dot,
            _time.millis()
        );

        // mark dot as executed
        self.new_executed_dots.push(dot);
        assert!(self.executed_clock.add(&dot.source(), dot.sequence()));

        // add command to commands to be executed
        self.to_execute.push_back(cmd);
    }
}

impl fmt::Debug for PredecessorsGraph {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "vertex index:")?;
        write!(f, "{:#?}", self.vertex_index)?;
        write!(f, "phase one pending index:")?;
        write!(f, "{:#?}", self.phase_one_pending_index)?;
        write!(f, "phase two pending index:")?;
        write!(f, "{:#?}", self.phase_two_pending_index)?;
        write!(f, "executed:")?;
        write!(f, "{:?}", self.executed_clock)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fantoch::id::{ClientId, Rifl};
    use fantoch::kvs::{KVOp, Key};
    use fantoch::time::RunTime;
    use fantoch::{HashMap, HashSet};
    use permutator::{Combination, Permutation};
    use rand::seq::SliceRandom;
    use rand::Rng;
    use std::cell::RefCell;
    use std::cmp::Ordering;
    use std::collections::{BTreeMap, BTreeSet};
    use std::iter::FromIterator;

    fn caesar_deps(deps: Vec<Dot>) -> Arc<CaesarDeps> {
        Arc::new(CaesarDeps::from_iter(deps))
    }

    #[test]
    fn simple() {
        // create queue
        let p1 = 1;
        let p2 = 2;
        let n = 2;
        let f = 1;
        let config = Config::new(n, f);
        let mut queue = PredecessorsGraph::new(p1, &config);
        let time = RunTime;

        // create dots
        let dot_0 = Dot::new(p1, 1);
        let dot_1 = Dot::new(p2, 1);

        // cmd 0
        let cmd_0 = Command::from(
            Rifl::new(1, 1),
            vec![(String::from("A"), KVOp::Put(String::new()))],
        );
        let clock_0 = Clock::from(2, p1);
        let deps_0 = caesar_deps(vec![dot_1]);

        // cmd 1
        let cmd_1 = Command::from(
            Rifl::new(2, 1),
            vec![(String::from("A"), KVOp::Put(String::new()))],
        );
        let clock_1 = Clock::from(1, p2);
        let deps_1 = caesar_deps(vec![dot_0]);

        // add cmd 0
        queue.add(dot_0, cmd_0.clone(), clock_0, deps_0, &time);
        // check commands ready to be executed
        assert!(queue.commands_to_execute().is_empty());

        // add cmd 1
        queue.add(dot_1, cmd_1.clone(), clock_1, deps_1, &time);
        // check commands ready to be executed
        assert_eq!(queue.commands_to_execute(), vec![cmd_1, cmd_0]);
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
    ) -> Vec<(Dot, Option<BTreeSet<Key>>, Clock, Arc<CaesarDeps>)> {
        let mut rng = rand::thread_rng();
        let mut possible_keys: Vec<_> =
            ('A'..='D').map(|key| key.to_string()).collect();

        // create dots
        let dots: Vec<_> =
            fantoch::util::process_ids(fantoch::command::DEFAULT_SHARD_ID, n)
                .flat_map(|process_id| {
                    (1..=events_per_process)
                        .map(move |event| Dot::new(process_id, event as u64))
                })
                .collect();

        // compute all possible clocks
        let mut all_clocks: Vec<_> = (1..=dots.len())
            .map(|clock| {
                let process_id = 1;
                Clock::from(clock as u64, process_id)
            })
            .collect();
        // shuffle the clocks
        all_clocks.shuffle(&mut rng);

        // compute keys, clock, and empty deps
        let dot_to_data: HashMap<_, _> = dots
            .clone()
            .into_iter()
            .map(|dot| {
                // select two random keys from the set of possible keys:
                // - this makes sure that the conflict relation is not
                //   transitive
                possible_keys.shuffle(&mut rng);
                let mut keys = BTreeSet::new();
                assert!(keys.insert(possible_keys[0].clone()));
                assert!(keys.insert(possible_keys[1].clone()));

                // assign a random clock to this command
                let clock = all_clocks
                    .pop()
                    .expect("there must be a clock for each command");

                // create empty deps
                let deps = CaesarDeps::new();

                (dot, (Some(keys), clock, RefCell::new(deps)))
            })
            .collect();

        // for each pair of dots
        dots.combination(2).for_each(|dots| {
            let left = dots[0];
            let right = dots[1];

            // find their data
            let (left_keys, left_clock, left_deps) =
                dot_to_data.get(left).expect("left dot data must exist");
            let (right_keys, right_clock, right_deps) =
                dot_to_data.get(right).expect("right dot data must exist");

            // unwrap keys
            let left_keys = left_keys.as_ref().expect("left keys should exist");
            let right_keys =
                right_keys.as_ref().expect("right keys should exist");

            // check if the commands conflict (i.e. if the keys being accessed
            // intersect)
            let conflict = left_keys.intersection(&right_keys).next().is_some();

            // if the commands conflict:
            // - the one with the lower clock should be a dependency of the
            //   other
            // - the one with the higher clock doesn't have to be a dependency
            //   of the other, but that can happen
            if conflict {
                // borrow their clocks mutably
                let mut left_deps = left_deps.borrow_mut();
                let mut right_deps = right_deps.borrow_mut();

                // check to which sets of deps we should add the other command
                let (add_left_to_right, add_right_to_left) =
                    match left_clock.cmp(&right_clock) {
                        Ordering::Less => (true, rng.gen_bool(0.5)),
                        Ordering::Greater => (rng.gen_bool(0.5), true),
                        _ => unreachable!("clocks must be different"),
                    };

                if add_left_to_right {
                    right_deps.insert(*left);
                }

                if add_right_to_left {
                    left_deps.insert(*right);
                }
            }
        });

        dot_to_data
            .into_iter()
            .map(|(dot, (keys, clock, deps_cell))| {
                let deps = deps_cell.into_inner();
                (dot, keys, clock, Arc::new(deps))
            })
            .collect()
    }

    fn shuffle_it(
        n: usize,
        mut args: Vec<(Dot, Option<BTreeSet<Key>>, Clock, Arc<CaesarDeps>)>,
    ) {
        let total_order = check_termination(n, args.clone());
        args.permutation().for_each(|permutation| {
            println!("permutation = {:?}", permutation);
            let sorted = check_termination(n, permutation);
            assert_eq!(total_order, sorted);
        });
    }

    fn check_termination(
        n: usize,
        args: Vec<(Dot, Option<BTreeSet<Key>>, Clock, Arc<CaesarDeps>)>,
    ) -> BTreeMap<Key, Vec<Rifl>> {
        // create queue
        let process_id = 1;
        let f = 1;
        let config = Config::new(n, f);
        let mut queue = PredecessorsGraph::new(process_id, &config);
        let time = RunTime;
        let mut all_rifls = HashSet::new();
        let mut sorted = BTreeMap::new();

        args.into_iter().for_each(|(dot, keys, clock, deps)| {
            // create command rifl from its dot
            let rifl = Rifl::new(dot.source() as ClientId, dot.sequence());

            // create command:
            // - set single CONF key if no keys were provided
            let keys = keys.unwrap_or_else(|| {
                BTreeSet::from_iter(vec![String::from("CONF")])
            });
            let ops = keys.into_iter().map(|key| {
                let value = String::from("");
                (key, KVOp::Put(value))
            });
            let cmd = Command::from(rifl, ops);

            // add to the set of all rifls
            assert!(all_rifls.insert(rifl));

            // add it to the queue
            queue.add(dot, cmd, clock, deps, &time);

            // get ready to execute
            let to_execute = queue.commands_to_execute();

            // for each command ready to be executed
            to_execute.iter().for_each(|cmd| {
                // get its rifl
                let rifl = cmd.rifl();

                // remove it from the set of rifls
                assert!(all_rifls.remove(&cmd.rifl()));

                // and add it to the sorted results
                cmd.keys(fantoch::command::DEFAULT_SHARD_ID)
                    .for_each(|key| {
                        sorted
                            .entry(key.clone())
                            .or_insert_with(Vec::new)
                            .push(rifl);
                    })
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
