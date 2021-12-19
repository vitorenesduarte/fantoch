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
use crate::protocol::common::graph::Dependency;
use fantoch::command::Command;
use fantoch::config::Config;
use fantoch::executor::{ExecutorMetrics, ExecutorMetricsKind};
use fantoch::id::{Dot, ProcessId, ShardId};
use fantoch::time::SysTime;
use fantoch::util;
use fantoch::{debug, trace};
use fantoch::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fmt;
use std::time::Duration;
use threshold::AEClock;

const MONITOR_PENDING_THRESHOLD: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RequestReply {
    Info {
        dot: Dot,
        cmd: Command,
        deps: Vec<Dependency>,
    },
    Executed {
        dot: Dot,
    },
}

#[derive(Clone)]
pub struct DependencyGraph {
    executor_index: usize,
    process_id: ProcessId,
    executed_clock: AEClock<ProcessId>,
    vertex_index: VertexIndex,
    pending_index: PendingIndex,
    finder: TarjanSCCFinder,
    metrics: ExecutorMetrics,
    // worker 0 (handles commands):
    // - adds new commands `to_execute`
    // - `out_requests` dependencies to be able to order commands
    // - notifies remaining workers about what's been executed through
    //   `added_to_executed_clock`
    to_execute: VecDeque<Command>,
    out_requests: HashMap<ShardId, HashSet<Dot>>,
    added_to_executed_clock: HashSet<Dot>,
    // auxiliary workers (handles requests):
    // - may have `buffered_in_requests` when doesn't have the command yet
    // - produces `out_request_replies` when it has the command
    buffered_in_requests: HashMap<ShardId, HashSet<Dot>>,
    out_request_replies: HashMap<ShardId, Vec<RequestReply>>,
}

enum FinderInfo {
    // set of dots in found SCCs
    Found(Vec<Dot>),
    // set of dots in found SCCs (it's possible to find SCCs even though the
    // search for another dot failed), missing dependencies and set of dots
    // visited while searching for SCCs
    MissingDependencies(Vec<Dot>, HashSet<Dot>, HashSet<Dependency>),
    // in case we try to find SCCs on dots that are no longer pending
    NotPending,
}

impl DependencyGraph {
    /// Create a new `Graph`.
    pub fn new(
        process_id: ProcessId,
        shard_id: ShardId,
        config: &Config,
    ) -> Self {
        // this value will be overwritten
        let executor_index = 0;
        // create executed clock and its snapshot
        let ids: Vec<_> =
            util::all_process_ids(config.shard_count(), config.n())
                .map(|(process_id, _)| process_id)
                .collect();
        let executed_clock = AEClock::with(ids.clone());
        // create indexes
        let vertex_index = VertexIndex::new(process_id);
        let pending_index = PendingIndex::new(shard_id, *config);
        // create finder
        let finder = TarjanSCCFinder::new(process_id, *config);
        let metrics = ExecutorMetrics::new();
        // create to execute
        let to_execute = Default::default();
        // create requests and request replies
        let out_requests = Default::default();
        // only track what's added to the executed clock if partial replication
        let added_to_executed_clock = HashSet::new();
        let buffered_in_requests = Default::default();
        let out_request_replies = Default::default();
        DependencyGraph {
            executor_index,
            process_id,
            executed_clock,
            vertex_index,
            pending_index,
            finder,
            metrics,
            to_execute,
            out_requests,
            added_to_executed_clock,
            buffered_in_requests,
            out_request_replies,
        }
    }

    fn set_executor_index(&mut self, index: usize) {
        self.executor_index = index;
    }

    /// Returns a new command ready to be executed.
    #[must_use]
    pub fn command_to_execute(&mut self) -> Option<Command> {
        self.to_execute.pop_front()
    }

    /// Returns which dots have been added to the executed clock.
    #[must_use]
    pub fn to_executors(&mut self) -> Option<HashSet<Dot>> {
        if self.added_to_executed_clock.is_empty() {
            None
        } else {
            Some(std::mem::take(&mut self.added_to_executed_clock))
        }
    }

    /// Returns a request.
    #[must_use]
    pub fn requests(&mut self) -> HashMap<ShardId, HashSet<Dot>> {
        std::mem::take(&mut self.out_requests)
    }

    /// Returns a set of request replies.
    #[must_use]
    pub fn request_replies(&mut self) -> HashMap<ShardId, Vec<RequestReply>> {
        std::mem::take(&mut self.out_request_replies)
    }

    #[cfg(test)]
    fn commands_to_execute(&mut self) -> VecDeque<Command> {
        std::mem::take(&mut self.to_execute)
    }

    fn metrics(&self) -> &ExecutorMetrics {
        &self.metrics
    }

    fn cleanup(&mut self, time: &dyn SysTime) {
        trace!(
            "p{}: @{} Graph::cleanup | time = {}",
            self.process_id,
            self.executor_index,
            time.millis()
        );
        if self.executor_index > 0 {
            // if not main executor, check pending remote requests
            self.check_pending_requests(time);
        }
    }

    fn monitor_pending(&self, time: &dyn SysTime) {
        debug!(
            "p{}: @{} Graph::monitor_pending | time = {}",
            self.process_id,
            self.executor_index,
            time.millis()
        );

        if self.executor_index == 0 {
            // check requests that have been committed at least 1 second ago
            self.vertex_index.monitor_pending(
                &self.executed_clock,
                MONITOR_PENDING_THRESHOLD,
                time,
            )
        }
    }

    fn handle_executed(&mut self, dots: HashSet<Dot>, _time: &dyn SysTime) {
        debug!(
            "p{}: @{} Graph::handle_executed {:?} | time = {}",
            self.process_id,
            self.executor_index,
            dots,
            _time.millis()
        );
        if self.executor_index > 0 {
            for dot in dots {
                self.executed_clock.add(&dot.source(), dot.sequence());
            }
        }
    }

    /// Add a new command with its clock to the queue.
    pub fn handle_add(
        &mut self,
        dot: Dot,
        cmd: Command,
        deps: Vec<Dependency>,
        time: &dyn SysTime,
    ) {
        assert_eq!(self.executor_index, 0);
        debug!(
            "p{}: @{} Graph::handle_add {:?} {:?} | time = {}",
            self.process_id,
            self.executor_index,
            dot,
            deps,
            time.millis()
        );

        // create new vertex for this command
        let vertex = Vertex::new(dot, cmd, deps, time);

        if self.vertex_index.index(vertex).is_some() {
            panic!(
                "p{}: @{} Graph::handle_add tried to index already indexed {:?}",
                self.process_id, self.executor_index, dot
            );
        }

        // get current command ready count and count newly ready commands
        let initial_ready = self.to_execute.len();
        let mut total_scc_count = 0;

        // try to find new SCCs
        let first_find = true;
        match self.find_scc(first_find, dot, &mut total_scc_count, time) {
            FinderInfo::Found(dots) => {
                // try to execute other commands if new SCCs were found
                self.check_pending(dots, &mut total_scc_count, time);
            }
            FinderInfo::MissingDependencies(dots, _visited, missing_deps) => {
                // update the pending
                self.index_pending(dot, missing_deps, time);
                // try to execute other commands if new SCCs were found
                self.check_pending(dots, &mut total_scc_count, time);
            }
            FinderInfo::NotPending => {
                panic!("just added dot must be pending");
            }
        }

        // check that all newly ready commands have been incorporated
        assert_eq!(self.to_execute.len(), initial_ready + total_scc_count);

        trace!(
            "p{}: @{} Graph::log executed {:?} | pending {:?} | time = {}",
            self.process_id,
            self.executor_index,
            self.executed_clock,
            self.vertex_index
                .dots()
                .collect::<std::collections::BTreeSet<_>>(),
            time.millis()
        );
    }

    fn handle_request(
        &mut self,
        from: ShardId,
        dots: HashSet<Dot>,
        time: &dyn SysTime,
    ) {
        assert!(self.executor_index > 0);
        trace!(
            "p{}: @{} Graph::handle_request {:?} from {:?} | time = {}",
            self.process_id,
            self.executor_index,
            dots,
            from,
            time.millis()
        );
        // save in requests metric
        self.metrics.aggregate(ExecutorMetricsKind::InRequests, 1);
        // try to process requests
        self.process_requests(from, dots.into_iter(), time)
    }

    fn process_requests(
        &mut self,
        from: ShardId,
        dots: impl Iterator<Item = Dot>,
        time: &dyn SysTime,
    ) {
        assert!(self.executor_index > 0);
        for dot in dots {
            if let Some(vertex) = self.vertex_index.find(&dot) {
                let vertex = vertex.read();

                // panic if the shard that requested this vertex replicates it
                if vertex.cmd.replicated_by(&from) {
                    panic!(
                        "p{}: @{} Graph::process_requests {:?} is replicated by {:?} (WARN) | time = {}",
                        self.process_id,
                        self.executor_index,
                        dot,
                        from,
                        time.millis()
                    )
                } else {
                    debug!(
                        "p{}: @{} Graph::process_requests {:?} sending info to {:?} | time = {}",
                        self.process_id,
                        self.executor_index,
                        dot,
                        from,
                        time.millis()
                    );
                    self.out_request_replies.entry(from).or_default().push(
                        RequestReply::Info {
                            dot,
                            cmd: vertex.cmd.clone(),
                            deps: vertex.deps.clone(),
                        },
                    )
                }
            } else {
                // if we don't have it, then check if it's executed (in our
                // snapshot)
                if self.executed_clock.contains(&dot.source(), dot.sequence()) {
                    debug!(
                        "p{}: @{} Graph::process_requests {:?} sending executed to {:?} | time = {}",
                        self.process_id,
                        self.executor_index,
                        dot,
                        from,
                        time.millis()
                    );
                    // if it's executed, notify the shard that it has already
                    // been executed
                    // - TODO: the Janus paper says that in this case, we should
                    //   send the full SCC; this will require a GC mechanism
                    self.out_request_replies
                        .entry(from)
                        .or_default()
                        .push(RequestReply::Executed { dot });
                } else {
                    debug!(
                        "p{}: @{} Graph::process_requests {:?} buffered from {:?} | time = {}",
                        self.process_id,
                        self.executor_index,
                        dot,
                        from,
                        time.millis()
                    );
                    // buffer request again
                    self.buffered_in_requests
                        .entry(from)
                        .or_default()
                        .insert(dot);
                }
            }
        }
    }

    pub fn handle_request_reply(
        &mut self,
        infos: Vec<RequestReply>,
        time: &dyn SysTime,
    ) {
        assert_eq!(self.executor_index, 0);
        for info in infos {
            debug!(
                "p{}: @{} Graph::handle_request_reply {:?} | time = {}",
                self.process_id,
                self.executor_index,
                info,
                time.millis()
            );

            match info {
                RequestReply::Info { dot, cmd, deps } => {
                    // add requested command to our graph
                    self.handle_add(dot, cmd, deps, time)
                }
                RequestReply::Executed { dot } => {
                    // update executed clock
                    self.executed_clock.add(&dot.source(), dot.sequence());
                    self.added_to_executed_clock.insert(dot);
                    // check pending
                    let dots = vec![dot];
                    let mut total_scc_count = 0;
                    self.check_pending(dots, &mut total_scc_count, time);
                }
            }
        }
    }

    #[must_use]
    fn find_scc(
        &mut self,
        first_find: bool,
        dot: Dot,
        total_scc_count: &mut usize,
        time: &dyn SysTime,
    ) -> FinderInfo {
        assert_eq!(self.executor_index, 0);
        trace!(
            "p{}: @{} Graph::find_scc {:?} | time = {}",
            self.process_id,
            self.executor_index,
            dot,
            time.millis()
        );
        // execute tarjan's algorithm
        let mut scc_count = 0;
        let mut missing_deps_count = 0;
        let finder_result = self.strong_connect(
            first_find,
            dot,
            &mut scc_count,
            &mut missing_deps_count,
        );

        // update total scc's found
        *total_scc_count += scc_count;

        // get sccs
        let sccs = self.finder.sccs();

        // save new SCCs
        let mut dots = Vec::with_capacity(scc_count);
        sccs.into_iter().for_each(|scc| {
            self.save_scc(scc, &mut dots, time);
        });

        // reset finder state and get visited dots
        let (visited, missing_deps) = self.finder.finalize(&self.vertex_index);
        assert!(
            // we can have a count higher the the number of dependencies if
            // there are cycles
            missing_deps.len() <= missing_deps_count,
            "more missing deps than the ones counted"
        );

        // NOTE: what follows must be done even if
        // `FinderResult::MissingDependency` was returned - it's possible that
        // while running the finder for some dot `X` we actually found SCCs with
        // another dots, even though the find for this dot `X` failed!

        // save new SCCs if any were found
        match finder_result {
            FinderResult::Found => FinderInfo::Found(dots),
            FinderResult::MissingDependencies(result_missing_deps) => {
                // if `MissingDependencies` was returned, then `missing_deps`
                // must be empty since we give up on the first missing dep
                assert!(
                    missing_deps.is_empty(),
                    "if MissingDependencies is returned, missing_deps must be empty"
                );
                FinderInfo::MissingDependencies(
                    dots,
                    visited,
                    result_missing_deps,
                )
            }
            FinderResult::NotPending => FinderInfo::NotPending,
            FinderResult::NotFound => {
                // in this case, `missing_deps` must be non-empty
                assert!(
                    !missing_deps.is_empty(),
                    "either there's a missing dependency, or we should find an SCC"
                );
                FinderInfo::MissingDependencies(dots, visited, missing_deps)
            }
        }
    }

    fn save_scc(&mut self, scc: SCC, dots: &mut Vec<Dot>, time: &dyn SysTime) {
        assert_eq!(self.executor_index, 0);

        // save chain size metric
        self.metrics
            .collect(ExecutorMetricsKind::ChainSize, scc.len() as u64);

        scc.into_iter().for_each(|dot| {
            trace!(
                "p{}: @{} Graph::save_scc removing {:?} from indexes | time = {}",
                self.process_id,
                self.executor_index,
                dot,
                time.millis()
            );

            // remove from vertex index
            let vertex = self
                .vertex_index
                .remove(&dot)
                .expect("dots from an SCC should exist");

            // update the set of ready dots
            dots.push(dot);

            // get command
            let (duration_ms, cmd) = vertex.into_command(time);

            // save execution delay metric
            self.metrics
                .collect(ExecutorMetricsKind::ExecutionDelay, duration_ms);

            // add command to commands to be executed
            self.to_execute.push_back(cmd);
        })
    }

    fn index_pending(
        &mut self,
        dot: Dot,
        missing_deps: HashSet<Dependency>,
        _time: &dyn SysTime,
    ) {
        let mut requests = 0;
        for dep in missing_deps {
            if let Some((dep_dot, target_shard)) =
                self.pending_index.index(&dep, dot)
            {
                debug!(
                    "p{}: @{} Graph::index_pending will ask {:?} to {:?} | time = {}",
                    self.process_id,
                    self.executor_index,
                    dep_dot,
                    target_shard,
                    _time.millis()
                );
                requests += 1;
                self.out_requests
                    .entry(target_shard)
                    .or_default()
                    .insert(dep_dot);
            }
        }
        // save out requests metric
        self.metrics
            .aggregate(ExecutorMetricsKind::OutRequests, requests);
    }

    fn check_pending(
        &mut self,
        mut dots: Vec<Dot>,
        total_scc_count: &mut usize,
        time: &dyn SysTime,
    ) {
        assert_eq!(self.executor_index, 0);
        while let Some(dot) = dots.pop() {
            // get pending commands that depend on this dot
            if let Some(pending) = self.pending_index.remove(&dot) {
                debug!(
                    "p{}: @{} Graph::try_pending {:?} depended on {:?} | time = {}",
                    self.process_id,
                    self.executor_index,
                    pending,
                    dot,
                    time.millis()
                );
                self.try_pending(pending, &mut dots, total_scc_count, time);
            } else {
                debug!(
                    "p{}: @{} Graph::try_pending nothing depended on {:?} | time = {}",
                    self.process_id,
                    self.executor_index,
                    dot,
                    time.millis()
                );
            }
        }
        // once there are no more dots to try, no command in pending should be
        // possible to be executed, so we give up!
    }

    fn try_pending(
        &mut self,
        pending: HashSet<Dot>,
        dots: &mut Vec<Dot>,
        total_scc_count: &mut usize,
        time: &dyn SysTime,
    ) {
        assert_eq!(self.executor_index, 0);
        // try to find new SCCs for each of those commands
        let mut visited = HashSet::new();
        let first_find = false;

        for dot in pending {
            // only try to find new SCCs from non-visited commands
            if !visited.contains(&dot) {
                match self.find_scc(first_find, dot, total_scc_count, time) {
                    FinderInfo::Found(new_dots) => {
                        // reset visited
                        visited.clear();

                        // if new SCCs were found, now there are more
                        // child dots to check
                        dots.extend(new_dots);
                    }
                    FinderInfo::MissingDependencies(
                        new_dots,
                        new_visited,
                        missing_deps,
                    ) => {
                        // update pending
                        self.index_pending(dot, missing_deps, time);

                        if !new_dots.is_empty() {
                            // if we found a new SCC, reset visited;
                            visited.clear();
                        } else {
                            // otherwise, try other pending commands,
                            // but don't try those that were visited in
                            // this search
                            visited.extend(new_visited);
                        }

                        // if new SCCs were found, now there are more
                        // child dots to check
                        dots.extend(new_dots);
                    }
                    FinderInfo::NotPending => {
                        // this happens if the pending dot is no longer
                        // pending
                    }
                }
            }
        }
    }

    fn strong_connect(
        &mut self,
        first_find: bool,
        dot: Dot,
        scc_count: &mut usize,
        missing_deps_count: &mut usize,
    ) -> FinderResult {
        assert_eq!(self.executor_index, 0);
        // get the vertex
        match self.vertex_index.find(&dot) {
            Some(vertex) => self.finder.strong_connect(
                first_find,
                dot,
                &vertex,
                &mut self.executed_clock,
                &mut self.added_to_executed_clock,
                &self.vertex_index,
                scc_count,
                missing_deps_count,
            ),
            None => {
                // in this case this `dot` is no longer pending
                FinderResult::NotPending
            }
        }
    }

    fn check_pending_requests(&mut self, time: &dyn SysTime) {
        let buffered = std::mem::take(&mut self.buffered_in_requests);
        for (from, dots) in buffered {
            self.process_requests(from, dots.into_iter(), time);
        }
    }
}

impl fmt::Debug for DependencyGraph {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
    use fantoch::id::{ClientId, Rifl, ShardId};
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
    use threshold::{AEClock, AboveExSet, EventSet};

    fn dep(dot: Dot, shard_id: ShardId) -> Dependency {
        Dependency {
            dot,
            shards: Some(BTreeSet::from_iter(vec![shard_id])),
        }
    }

    #[test]
    fn simple() {
        // create queue
        let process_id = 1;
        let shard_id = 0;
        let n = 2;
        let f = 1;
        let config = Config::new(n, f);
        let mut queue = DependencyGraph::new(process_id, shard_id, &config);
        let time = RunTime;

        // create dots
        let dot_0 = Dot::new(1, 1);
        let dot_1 = Dot::new(2, 1);

        // cmd 0
        let cmd_0 = Command::from(
            Rifl::new(1, 1),
            vec![(String::from("A"), KVOp::Put(String::new()))],
        );
        let deps_0 = vec![dep(dot_1, shard_id)];

        // cmd 1
        let cmd_1 = Command::from(
            Rifl::new(2, 1),
            vec![(String::from("A"), KVOp::Put(String::new()))],
        );
        let deps_1 = vec![dep(dot_0, shard_id)];

        // add cmd 0
        queue.handle_add(dot_0, cmd_0.clone(), deps_0, &time);
        // check commands ready to be executed
        assert!(queue.commands_to_execute().is_empty());

        // add cmd 1
        queue.handle_add(dot_1, cmd_1.clone(), deps_1, &time);
        // check commands ready to be executed
        assert_eq!(queue.commands_to_execute(), vec![cmd_0, cmd_1]);
    }

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
    #[test]
    fn transitive_conflicts_assumption_regression_test_1() {
        // config
        let n = 5;

        // dots
        let dot_1 = Dot::new(1, 1);
        let dot_2 = Dot::new(1, 2);
        let dot_3 = Dot::new(1, 3);
        let dot_4 = Dot::new(1, 4);
        let dot_5 = Dot::new(1, 5);

        // deps
        let deps_1 = HashSet::from_iter(vec![dot_4]);
        let deps_2 = HashSet::from_iter(vec![dot_4]);
        let deps_3 = HashSet::from_iter(vec![dot_5]);
        let deps_4 = HashSet::from_iter(vec![dot_3]);
        let deps_5 = HashSet::from_iter(vec![dot_4]);

        let order_a = vec![
            (dot_3, None, deps_3.clone()),
            (dot_4, None, deps_4.clone()),
            (dot_5, None, deps_5.clone()),
            (dot_1, None, deps_1.clone()),
            (dot_2, None, deps_2.clone()),
        ];
        let order_b = vec![
            (dot_3, None, deps_3),
            (dot_4, None, deps_4),
            (dot_5, None, deps_5),
            (dot_2, None, deps_2),
            (dot_1, None, deps_1),
        ];
        let order_a = check_termination(n, order_a);
        let order_b = check_termination(n, order_b);
        assert!(order_a != order_b);
    }

    /// Simple example showing why encoding of dependencies matters for the
    /// `transitive_conflicts` optimization to be correct (which, makes the name
    /// of the optimization not great):
    /// - 3 replicas (A, B, C), and 3 commands
    ///   * command (A, 1), keys = {x}
    ///   * command (A, 2), keys = {y}
    ///   * command (B, 1), keys = {x, y}
    ///
    /// First, (A, 1) is submitted and gets no dependencies:
    /// - {A -> 0, B -> 0, C -> 0}
    /// Then, (A, 2) is submitted and also gets no dependencies:
    /// - {A -> 0, B -> 0, C -> 0}
    /// Finally, (B, 1) is submitted and gets (A, 2) as a dependency:
    /// - {A -> 2, B -> 0, C -> 0}
    /// It only gets (A, 2) because we only return the highest conflicting
    /// command from each replica.
    ///
    /// With the optimization, the order in which commands are received by the
    /// ordering component affects results:
    /// - (A, 1), (A, 2), (B, 1): commands are executed in the order they're
    ///   received, producing correct results
    /// - (A, 2), (B, 1), (A, 1): (B, 1) is executed before (A, 1) and shouldn't
    ///
    /// Without the optimization, (B, 1) would be forced to wait for (A, 1) in
    /// the last case, producing a correct result.
    ///
    /// It looks like the optimization would be correct if, instead of returning
    /// the highest conflicting command per replica, we would return the highest
    /// conflict command per replica *per key*.
    #[test]
    fn transitive_conflicts_assumption_regression_test_2() {
        // config
        let n = 3;

        let keys = |keys: Vec<&str>| {
            keys.into_iter()
                .map(|key| key.to_string())
                .collect::<BTreeSet<_>>()
        };

        // cmd 1,1
        let dot_1_1 = Dot::new(1, 1);
        let keys_1_1 = keys(vec!["A"]);
        let deps_1_1 = HashSet::new();

        // cmd 1,2
        let dot_1_2 = Dot::new(1, 2);
        let keys_1_2 = keys(vec!["B"]);
        let deps_1_2 = HashSet::new();

        // cmd 2,1
        let dot_2_1 = Dot::new(2, 1);
        let keys_2_1 = keys(vec!["A", "B"]);
        let deps_2_1 = HashSet::from_iter(vec![dot_1_2]);

        let order_a = vec![
            (dot_1_1, Some(keys_1_1.clone()), deps_1_1.clone()),
            (dot_1_2, Some(keys_1_2.clone()), deps_1_2.clone()),
            (dot_2_1, Some(keys_2_1.clone()), deps_2_1.clone()),
        ];
        let order_b = vec![
            (dot_1_2, Some(keys_1_2), deps_1_2),
            (dot_2_1, Some(keys_2_1), deps_2_1),
            (dot_1_1, Some(keys_1_1), deps_1_1),
        ];
        let order_a = check_termination(n, order_a);
        let order_b = check_termination(n, order_b);
        assert!(order_a != order_b);
    }

    #[test]
    fn cycle() {
        // config
        let n = 3;

        // dots
        let dot_1 = Dot::new(1, 1);
        let dot_2 = Dot::new(2, 1);
        let dot_3 = Dot::new(3, 1);

        // deps
        let deps_1 = HashSet::from_iter(vec![dot_3]);
        let deps_2 = HashSet::from_iter(vec![dot_1]);
        let deps_3 = HashSet::from_iter(vec![dot_2]);

        let args = vec![
            (dot_1, None, deps_1),
            (dot_2, None, deps_2),
            (dot_3, None, deps_3),
        ];
        shuffle_it(n, args);
    }

    #[test]
    fn test_add_random() {
        let shard_id = 0;
        let n = 2;
        let iterations = 10;
        let events_per_process = 3;

        (0..iterations).for_each(|_| {
            let args = random_adds(shard_id, n, events_per_process);
            shuffle_it(n, args);
        });
    }

    fn random_adds(
        shard_id: ShardId,
        n: usize,
        events_per_process: usize,
    ) -> Vec<(Dot, Option<BTreeSet<Key>>, HashSet<Dot>)> {
        let mut possible_keys: Vec<_> =
            ('A'..='D').map(|key| key.to_string()).collect();

        // create dots
        let dots: Vec<_> = fantoch::util::process_ids(shard_id, n)
            .flat_map(|process_id| {
                (1..=events_per_process)
                    .map(move |event| Dot::new(process_id, event as u64))
            })
            .collect();

        // compute keys and empty deps
        let deps: HashMap<_, _> = dots
            .clone()
            .into_iter()
            .map(|dot| {
                // select two random keys from the set of possible keys:
                // - this makes sure that the conflict relation is not
                //   transitive
                possible_keys.shuffle(&mut rand::thread_rng());
                let mut keys = BTreeSet::new();
                assert!(keys.insert(possible_keys[0].clone()));
                assert!(keys.insert(possible_keys[1].clone()));
                // create empty deps
                let deps = HashSet::new();
                (dot, (Some(keys), RefCell::new(deps)))
            })
            .collect();

        // for each pair of dots
        dots.combination(2).for_each(|dots| {
            let left = dots[0];
            let right = dots[1];

            // find their data
            let (left_keys, left_clock) =
                deps.get(left).expect("left dot data must exist");
            let (right_keys, right_clock) =
                deps.get(right).expect("right dot data must exist");

            // unwrap keys
            let left_keys = left_keys.as_ref().expect("left keys should exist");
            let right_keys =
                right_keys.as_ref().expect("right keys should exist");

            // check if the commands conflict (i.e. if the keys being accessed
            // intersect)
            let conflict = left_keys.intersection(&right_keys).next().is_some();

            // if the commands conflict, then make sure at least one is a
            // dependency of the other
            if conflict {
                // borrow their clocks mutably
                let mut left_deps = left_clock.borrow_mut();
                let mut right_deps = right_clock.borrow_mut();

                if left.source() == right.source() {
                    // if dots belong to the same process, make the latest
                    // depend on the oldest
                    match left.sequence().cmp(&right.sequence()) {
                        Ordering::Less => right_deps.insert(*left),
                        Ordering::Greater => left_deps.insert(*right),
                        _ => unreachable!("dots must be different"),
                    };
                } else {
                    // otherwise, make them depend on each other (maybe both
                    // ways)
                    let mut rng = rand::thread_rng();
                    match rng.gen_range(0..3) {
                        0 => {
                            // left depends on right
                            left_deps.insert(*right);
                        }
                        1 => {
                            // right depends on left
                            right_deps.insert(*left);
                        }
                        2 => {
                            // both
                            left_deps.insert(*right);
                            right_deps.insert(*left);
                        }
                        _ => panic!("out-of-bounds random number"),
                    }
                }
            }
        });

        deps.into_iter()
            .map(|(dot, (keys, deps_cell))| {
                let deps = deps_cell.into_inner();
                (dot, keys, deps)
            })
            .collect()
    }

    fn shuffle_it(
        n: usize,
        mut args: Vec<(Dot, Option<BTreeSet<Key>>, HashSet<Dot>)>,
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
        args: Vec<(Dot, Option<BTreeSet<Key>>, HashSet<Dot>)>,
    ) -> BTreeMap<Key, Vec<Rifl>> {
        // create queue
        let process_id = 1;
        let shard_id = 0;
        let f = 1;
        let config = Config::new(n, f);
        let mut queue = DependencyGraph::new(process_id, shard_id, &config);
        let time = RunTime;
        let mut all_rifls = HashSet::new();
        let mut sorted = BTreeMap::new();

        args.into_iter().for_each(|(dot, keys, dep_dots)| {
            // transform dep dots into deps
            let deps = dep_dots
                .into_iter()
                .map(|dep_dot| dep(dep_dot, shard_id))
                .collect();
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
            queue.handle_add(dot, cmd, deps, &time);

            // get ready to execute
            let to_execute = queue.commands_to_execute();

            // for each command ready to be executed
            to_execute.iter().for_each(|cmd| {
                // get its rifl
                let rifl = cmd.rifl();

                // remove it from the set of rifls
                assert!(all_rifls.remove(&cmd.rifl()));

                // and add it to the sorted results
                cmd.keys(shard_id).for_each(|key| {
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

    #[test]
    fn sccs_found_and_missing_dep() {
        /*
        MCommit((4, 31), Clock { clock: {60, 50, 50, 30, 60} })
        MCommit((4, 32), Clock { clock: {60, 50, 50, 31, 60} })
        MCommit((4, 33), Clock { clock: {60, 50, 50, 32, 60} })
        MCommit((4, 34), Clock { clock: {60, 50, 50, 33, 60} })
        MCommit((4, 35), Clock { clock: {60, 50, 50, 34, 60} })
        MCommit((4, 36), Clock { clock: {60, 50, 50, 35, 60} })
        MCommit((4, 37), Clock { clock: {60, 50, 50, 36, 60} })
        MCommit((4, 38), Clock { clock: {60, 50, 50, 37, 60} })
        MCommit((4, 39), Clock { clock: {60, 50, 50, 38, 60} })
        MCommit((4, 40), Clock { clock: {60, 50, 50, 39, 60} })
        MCommit((5, 70), Clock { clock: {60, 50, 50, 40, 61} })
        ...
        stuff happens and at some point (5, 70) is tried because one of its dependencies, (2, 50), is delivered
        ...
        Graph:save_scc removing (2, 50) from indexes
        Graph:save_scc executed clock Clock { clock: {60, 50, 50, 30, 60} }
        Graph::try_pending {(5, 70), ...} depended on (2, 50)
        Graph:find_scc (5, 70)
        */
        // in order for this test to pass, sccs must be found by the finder; the
        // following loop stops once that happens
        std::iter::repeat(()).any(|_| check_sccs_found_with_missing_dep());
    }

    fn check_sccs_found_with_missing_dep() -> bool {
        let conflicting_command = || {
            let rifl = Rifl::new(1, 1);
            Command::from(
                rifl,
                vec![(String::from("CONF"), KVOp::Put(String::new()))],
            )
        };

        // create queue
        let process_id = 4;
        let shard_id = 0;
        let n = 5;
        let f = 1;
        let config = Config::new(n, f);
        let mut queue = DependencyGraph::new(process_id, shard_id, &config);
        let time = RunTime;

        // (5, 70): only (5, 61) is missing
        let missing_dot = Dot::new(5, 61);

        let root_dot = Dot::new(5, 70);
        queue.vertex_index.index(Vertex::new(
            root_dot,
            conflicting_command(),
            vec![
                dep(Dot::new(1, 60), shard_id),
                dep(Dot::new(2, 50), shard_id),
                dep(Dot::new(3, 50), shard_id),
                dep(Dot::new(4, 40), shard_id),
                dep(Dot::new(5, 61), shard_id),
            ],
            &time,
        ));

        // (4, 31)
        queue.vertex_index.index(Vertex::new(
            Dot::new(4, 31),
            conflicting_command(),
            vec![
                dep(Dot::new(1, 60), shard_id),
                dep(Dot::new(2, 50), shard_id),
                dep(Dot::new(3, 50), shard_id),
                dep(Dot::new(4, 30), shard_id),
                dep(Dot::new(5, 60), shard_id),
            ],
            &time,
        ));
        // (4, 32)
        queue.vertex_index.index(Vertex::new(
            Dot::new(4, 32),
            conflicting_command(),
            vec![
                dep(Dot::new(1, 60), shard_id),
                dep(Dot::new(2, 50), shard_id),
                dep(Dot::new(3, 50), shard_id),
                dep(Dot::new(4, 31), shard_id),
                dep(Dot::new(5, 60), shard_id),
            ],
            &time,
        ));
        // (4, 33)
        queue.vertex_index.index(Vertex::new(
            Dot::new(4, 33),
            conflicting_command(),
            vec![
                dep(Dot::new(1, 60), shard_id),
                dep(Dot::new(2, 50), shard_id),
                dep(Dot::new(3, 50), shard_id),
                dep(Dot::new(4, 32), shard_id),
                dep(Dot::new(5, 60), shard_id),
            ],
            &time,
        ));
        // (4, 34)
        queue.vertex_index.index(Vertex::new(
            Dot::new(4, 34),
            conflicting_command(),
            vec![
                dep(Dot::new(1, 60), shard_id),
                dep(Dot::new(2, 50), shard_id),
                dep(Dot::new(3, 50), shard_id),
                dep(Dot::new(4, 33), shard_id),
                dep(Dot::new(5, 60), shard_id),
            ],
            &time,
        ));
        // (4, 35)
        queue.vertex_index.index(Vertex::new(
            Dot::new(4, 35),
            conflicting_command(),
            vec![
                dep(Dot::new(1, 60), shard_id),
                dep(Dot::new(2, 50), shard_id),
                dep(Dot::new(3, 50), shard_id),
                dep(Dot::new(4, 34), shard_id),
                dep(Dot::new(5, 60), shard_id),
            ],
            &time,
        ));
        // (4, 36)
        queue.vertex_index.index(Vertex::new(
            Dot::new(4, 36),
            conflicting_command(),
            vec![
                dep(Dot::new(1, 60), shard_id),
                dep(Dot::new(2, 50), shard_id),
                dep(Dot::new(3, 50), shard_id),
                dep(Dot::new(4, 35), shard_id),
                dep(Dot::new(5, 60), shard_id),
            ],
            &time,
        ));
        // (4, 37)
        queue.vertex_index.index(Vertex::new(
            Dot::new(4, 37),
            conflicting_command(),
            vec![
                dep(Dot::new(1, 60), shard_id),
                dep(Dot::new(2, 50), shard_id),
                dep(Dot::new(3, 50), shard_id),
                dep(Dot::new(4, 36), shard_id),
                dep(Dot::new(5, 60), shard_id),
            ],
            &time,
        ));
        // (4, 38)
        queue.vertex_index.index(Vertex::new(
            Dot::new(4, 38),
            conflicting_command(),
            vec![
                dep(Dot::new(1, 60), shard_id),
                dep(Dot::new(2, 50), shard_id),
                dep(Dot::new(3, 50), shard_id),
                dep(Dot::new(4, 37), shard_id),
                dep(Dot::new(5, 60), shard_id),
            ],
            &time,
        ));
        // (4, 39)
        queue.vertex_index.index(Vertex::new(
            Dot::new(4, 39),
            conflicting_command(),
            vec![
                dep(Dot::new(1, 60), shard_id),
                dep(Dot::new(2, 50), shard_id),
                dep(Dot::new(3, 50), shard_id),
                dep(Dot::new(4, 38), shard_id),
                dep(Dot::new(5, 60), shard_id),
            ],
            &time,
        ));
        // (4, 40)
        queue.vertex_index.index(Vertex::new(
            Dot::new(4, 40),
            conflicting_command(),
            vec![
                dep(Dot::new(1, 60), shard_id),
                dep(Dot::new(2, 50), shard_id),
                dep(Dot::new(3, 50), shard_id),
                dep(Dot::new(4, 39), shard_id),
                dep(Dot::new(5, 60), shard_id),
            ],
            &time,
        ));

        // create executed clock
        queue.executed_clock = AEClock::from(
            util::vclock(vec![60, 50, 50, 30, 60]).into_iter().map(
                |(process_id, max_set)| {
                    (process_id, AboveExSet::from_events(max_set.event_iter()))
                },
            ),
        );

        // create ready commands counter and try to find an SCC
        let first_find = true;
        let mut ready_commands = 0;
        let finder_info =
            queue.find_scc(first_find, root_dot, &mut ready_commands, &time);

        if let FinderInfo::MissingDependencies(
            to_be_executed,
            _visited,
            missing_deps,
        ) = finder_info
        {
            // check the missing dot
            assert_eq!(
                missing_deps.len(),
                1,
                "there's a single missing dependency"
            );
            assert_eq!(
                missing_deps.into_iter().next().unwrap().dot,
                missing_dot
            );

            // check that ready commands are actually delivered
            assert_eq!(ready_commands, to_be_executed.len());

            // return whether sccs have been found
            !to_be_executed.is_empty()
        } else {
            panic!("FinderInfo::MissingDependency not found");
        }
    }
}
