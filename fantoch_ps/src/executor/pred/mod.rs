/// This modul contains the definition of `Vertex`, `VertexIndex` and
/// `PendingIndex`.
mod index;

/// This modules contains the definition of `PredecessorsExecutor` and
/// `PredecessorsExecutionInfo`.
mod executor;

// Re-exports.
pub use executor::{PredecessorsExecutionInfo, PredecessorsExecutor};

use self::index::{PendingIndex, Vertex, VertexIndex};
use crate::protocol::common::pred::{Clock, CompressedDots};
use fantoch::command::Command;
use fantoch::config::Config;
use fantoch::executor::{
    ExecutionOrderMonitor, ExecutorMetrics, ExecutorMetricsKind, ExecutorResult,
};
use fantoch::id::{Dot, ProcessId, ShardId};
use fantoch::kvs::KVStore;
use fantoch::protocol::Executed;
use fantoch::time::SysTime;
use fantoch::util;
use fantoch::{debug, trace};
use parking_lot::{Mutex, RwLock};
use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;
use threshold::AEClock;

#[derive(Clone)]
pub struct PredecessorsGraph {
    process_id: ProcessId,
    shard_id: ShardId,
    committed_clock: Arc<RwLock<AEClock<ProcessId>>>,
    executed_clock: Arc<RwLock<AEClock<ProcessId>>>,
    vertex_index: VertexIndex,
    // mapping from non committed dep to pending dot
    phase_one_pending_index: PendingIndex,
    // mapping from committed (but not executed) dep to pending dot
    phase_two_pending_index: PendingIndex,
    metrics: ExecutorMetrics,
    execute_at_commit: bool,

    // these two usually live at the upper level, but since in caesar any
    // executor may execute commands on any key, we need the kvs to be shared
    // by all executors.
    // TODO: since kvs operations are super fast, this should not be a
    // bottleneck
    store: Arc<Mutex<KVStore>>,
    to_clients: VecDeque<ExecutorResult>,
}

impl PredecessorsGraph {
    /// Create a new `PredecessorsGraph`.
    pub fn new(
        process_id: ProcessId,
        shard_id: ShardId,
        config: &Config,
    ) -> Self {
        // create executed clock and its snapshot
        let ids: Vec<_> =
            util::all_process_ids(config.shard_count(), config.n())
                .map(|(process_id, _)| process_id)
                .collect();
        let committed_clock = Arc::new(RwLock::new(AEClock::with(ids.clone())));
        let executed_clock = Arc::new(RwLock::new(AEClock::with(ids.clone())));
        // create indexes
        let vertex_index = VertexIndex::new();
        let phase_one_pending_index = PendingIndex::new();
        let phase_two_pending_index = PendingIndex::new();
        let metrics = ExecutorMetrics::new();
        let execute_at_commit = config.execute_at_commit();

        // create kvs
        let store = Arc::new(Mutex::new(KVStore::new(
            config.executor_monitor_execution_order(),
        )));
        let to_clients = Default::default();
        PredecessorsGraph {
            process_id,
            shard_id,
            executed_clock,
            committed_clock,
            vertex_index,
            phase_one_pending_index,
            phase_two_pending_index,
            metrics,
            execute_at_commit,
            store,
            to_clients,
        }
    }

    /// Returns a new command ready to be executed.
    #[must_use]
    fn to_clients(&mut self) -> Option<ExecutorResult> {
        self.to_clients.pop_front()
    }

    fn executed_frontier(&self) -> Executed {
        self.executed_clock.read().frontier().clone()
    }

    fn metrics(&self) -> &ExecutorMetrics {
        &self.metrics
    }

    fn monitor(&self) -> Option<ExecutionOrderMonitor> {
        self.store.lock().monitor().cloned()
    }

    /// Add a new command.
    pub fn add(
        &mut self,
        dot: Dot,
        cmd: Command,
        clock: Clock,
        deps: Arc<CompressedDots>,
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
                self.vertex_index.dots(),
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
        let mut vertex = vertex_ref.write();

        // compute number of non yet committed dependencies
        let mut non_committed_deps_count = 0;
        for dep_dot in vertex.deps.iter() {
            let committed = self
                .committed_clock
                .read()
                .contains(&dep_dot.source(), dep_dot.sequence());

            if !committed {
                trace!(
                    "p{}: Predecessors::move_1 non committed dep {:?} | time = {}",
                    self.process_id,
                    dep_dot,
                    time.millis()
                );
                non_committed_deps_count += 1;
                self.phase_one_pending_index.index(&dep_dot, dot);
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
            drop(vertex_ref);
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
        let mut vertex = vertex_ref.write();

        // compute number of yet executed dependencies
        let mut non_executed_deps_count = 0;
        for dep_dot in vertex.deps.iter() {
            trace!(
                "p{}: Predecessors::move_2 non executed dep {:?} | time = {}",
                self.process_id,
                dep_dot,
                time.millis()
            );
            // get the dependency and check its clock to see if it should be
            // consider
            if let Some(dep_ref) = self.vertex_index.find(&dep_dot) {
                let dep = dep_ref.read();

                // only consider this dep if it has a lower clock
                if dep.clock < vertex.clock {
                    trace!(
                    "p{}: Predecessors::move_2 non executed dep with lower clock {:?} | time = {}",
                    self.process_id,
                    dep_dot,
                    time.millis()
                );
                    non_executed_deps_count += 1;
                    self.phase_two_pending_index.index(&dep_dot, dot);
                }
            } else {
                // if it's not indexed, then it must be already executed
                trace!(
                    "p{}: Predecessors::move_2 dependency {:?} of {:?} already executed",
                    self.process_id, dep_dot, dot
                );
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
            drop(vertex_ref);
            self.save_to_execute(dot, time);
        }
    }

    fn index_committed_command(
        &mut self,
        dot: Dot,
        cmd: Command,
        clock: Clock,
        deps: Arc<CompressedDots>,
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

        // mark dot as committed
        assert!(self
            .committed_clock
            .write()
            .add(&dot.source(), dot.sequence()));
    }

    fn try_phase_one_pending(&mut self, dot: Dot, time: &dyn SysTime) {
        for pending_dot in self.phase_one_pending_index.remove(&dot) {
            // get vertex
            let vertex_ref = self
                .vertex_index
                .find(&pending_dot)
                .expect("command pending at phase one must exist");
            let mut vertex = vertex_ref.write();

            // a non-committed dep became committed, so update the number of
            // missing deps at phase one
            vertex.decrease_missing_deps();

            // check if there are no more missing deps, and if so, move the
            // command to phase two
            if vertex.get_missing_deps() == 0 {
                // move command to phase two
                drop(vertex);
                drop(vertex_ref);
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
            let mut vertex = vertex_ref.write();

            // a non-executed dep became executed, so update the number of
            // missing deps at phase two
            vertex.decrease_missing_deps();

            // check if there are no more missing deps, and if so, save the
            // command to be executed
            if vertex.get_missing_deps() == 0 {
                // save the command to be executed
                drop(vertex);
                drop(vertex_ref);
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

        // execute the command
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

        // mark dot as executed.
        assert!(self
            .executed_clock
            .write()
            .add(&dot.source(), dot.sequence()));

        // execute the command
        let mut store = self.store.lock();
        let results = cmd.execute(self.shard_id, &mut store);
        self.to_clients.extend(results);
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
    use fantoch::HashMap;
    use permutator::{Combination, Permutation};
    use rand::seq::SliceRandom;
    use rand::Rng;
    use std::cell::RefCell;
    use std::cmp::Ordering;
    use std::collections::{BTreeMap, BTreeSet};
    use std::iter::FromIterator;

    fn compressed_deps(deps: Vec<Dot>) -> Arc<CompressedDots> {
        Arc::new(CompressedDots::from_iter(deps))
    }

    #[test]
    fn simple() {
        // create queue
        let p1 = 1;
        let p2 = 2;
        let n = 2;
        let f = 1;
        let shard_id = 0;
        let config = Config::new(n, f);
        let mut queue = PredecessorsGraph::new(p1, shard_id, &config);
        let time = RunTime;

        // create dots
        let dot_0 = Dot::new(p1, 1);
        let dot_1 = Dot::new(p2, 1);

        // cmd 0
        let rifl0 = Rifl::new(1, 1);
        let cmd_0 = Command::from(
            rifl0,
            vec![(String::from("A"), KVOp::Put(String::new()))],
        );
        let clock_0 = Clock::from(2, p1);
        let deps_0 = compressed_deps(vec![dot_1]);

        // cmd 1
        let rifl1 = Rifl::new(2, 1);
        let cmd_1 = Command::from(
            rifl1,
            vec![(String::from("A"), KVOp::Put(String::new()))],
        );
        let clock_1 = Clock::from(1, p2);
        let deps_1 = compressed_deps(vec![dot_0]);

        // add cmd 0
        queue.add(dot_0, cmd_0.clone(), clock_0, deps_0, &time);
        // check commands ready to be executed
        assert!(queue.to_clients().is_none());

        // add cmd 1
        queue.add(dot_1, cmd_1.clone(), clock_1, deps_1, &time);
        // check commands ready to be executed
        assert_eq!(queue.to_clients().map(|result| result.rifl), Some(rifl1));
        assert_eq!(queue.to_clients().map(|result| result.rifl), Some(rifl0));
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
    ) -> Vec<(Dot, Option<BTreeSet<Key>>, Clock, Arc<CompressedDots>)> {
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
                let deps = CompressedDots::new();

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
        mut args: Vec<(Dot, Option<BTreeSet<Key>>, Clock, Arc<CompressedDots>)>,
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
        args: Vec<(Dot, Option<BTreeSet<Key>>, Clock, Arc<CompressedDots>)>,
    ) -> BTreeMap<Key, Vec<Rifl>> {
        // create queue
        let process_id = 1;
        let f = 1;
        let shard_id = 0;
        let config = Config::new(n, f);
        let mut queue = PredecessorsGraph::new(process_id, shard_id, &config);
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
            let to_clients = std::mem::take(&mut queue.to_clients);

            // for each command ready to be executed
            to_clients.into_iter().for_each(|result| {
                // get its rifl
                let rifl = result.rifl;

                // remove it from the set of rifls
                all_rifls.remove(&rifl);

                // and add it to the sorted results
                sorted.entry(result.key).or_insert_with(Vec::new).push(rifl);
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
