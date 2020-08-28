use super::index::{VertexIndex, VertexRef};
use crate::protocol::common::graph::Dependency;
use fantoch::command::Command;
use fantoch::config::Config;
use fantoch::id::{Dot, ProcessId, ShardId};
use fantoch::time::SysTime;
use fantoch::HashSet;
use std::cmp;
use std::collections::BTreeSet;
use threshold::AEClock;

/// commands are sorted inside an SCC given their dot
pub type SCC = BTreeSet<Dot>;

#[derive(PartialEq)]
pub enum FinderResult {
    Found,
    MissingDependencies(HashSet<Dependency>),
    NotPending,
    NotFound,
}

#[derive(Clone)]
pub struct TarjanSCCFinder {
    process_id: ProcessId,
    shard_id: ShardId,
    config: Config,
    id: usize,
    stack: Vec<Dot>,
    sccs: Vec<SCC>,
}

impl TarjanSCCFinder {
    /// Creates a new SCC finder that employs Tarjan's algorithm.
    pub fn new(
        process_id: ProcessId,
        shard_id: ShardId,
        config: Config,
    ) -> Self {
        Self {
            process_id,
            shard_id,
            config,
            id: 0,
            stack: Vec::new(),
            sccs: Vec::new(),
        }
    }

    /// Returns a list with the SCCs found.
    #[must_use]
    pub fn sccs(&mut self) -> Vec<SCC> {
        std::mem::take(&mut self.sccs)
    }

    /// Returns a set with all dots visited.
    /// It also resets the ids of all vertices still on the stack.
    #[must_use]
    pub fn finalize(&mut self, vertex_index: &VertexIndex) -> HashSet<Dot> {
        let _process_id = self.process_id;
        // reset id
        self.id = 0;
        // reset the id of each dot in the stack, while computing the set of
        // visited dots
        let mut visited = HashSet::new();
        while let Some(dot) = self.stack.pop() {
            tracing::trace!(
                "p{}: Finder::finalize removing {:?} from stack",
                _process_id,
                dot
            );

            // find vertex and reset its id
            let vertex = if let Some(vertex) = vertex_index.find(&dot) {
                vertex
            } else {
                panic!(
                    "p{}: Finder::finalize stack member {:?} should exist",
                    self.process_id, dot
                );
            };
            vertex.write().id = 0;

            // add dot to set of visited
            visited.insert(dot);
        }
        // return visited dots
        visited
    }

    /// Tries to find an SCC starting from root `dot`.
    pub fn strong_connect(
        &mut self,
        dot: Dot,
        vertex_ref: &VertexRef<'_>,
        executed_clock: &mut AEClock<ProcessId>,
        added_to_executed_clock: &mut HashSet<Dot>,
        vertex_index: &VertexIndex,
        found: &mut usize,
    ) -> FinderResult {
        // update id
        self.id += 1;

        // get vertex
        let mut vertex = vertex_ref.write();

        // set id and low for vertex
        vertex.id = self.id;
        vertex.low = self.id;

        // add to the stack
        vertex.on_stack = true;
        self.stack.push(dot);

        tracing::debug!(
            "p{}: Finder::strong_connect {:?} with id {}",
            self.process_id,
            dot,
            self.id
        );

        for i in 0..vertex.deps.len() {
            // TODO we should panic if we find a dependency highest than self
            let ignore = |dep_dot: Dot| {
                // ignore self or if already executed
                dep_dot == dot
                    || executed_clock
                        .contains(&dep_dot.source(), dep_dot.sequence())
            };

            // get dep dot
            let dep_dot = vertex.deps[i].dot;

            if ignore(dep_dot) {
                tracing::trace!(
                    "p{}: Finder::strong_connect ignoring dependency {:?}",
                    self.process_id,
                    dep_dot
                );
                continue;
            }

            match vertex_index.find(&dep_dot) {
                None => {
                    let dep = vertex.deps[i].clone();
                    let missing_deps = if self.config.shards() == 1 {
                        std::iter::once(dep).collect()
                    } else {
                        // if partial replication, compute all missing dependencies transitively; this makes sure that we request all needed dependencies in a single request
                        let mut visited = HashSet::new();
                        vertex_index.missing_dependencies(
                            &vertex_ref.read(),
                            executed_clock,
                            &mut visited,
                        )
                    };
                    tracing::debug!(
                        "p{}: Finder::strong_connect missing {:?} | {:?}",
                        self.process_id,
                        dep_dot,
                        missing_deps
                    );
                    return FinderResult::MissingDependencies(missing_deps);
                }
                Some(dep_vertex_ref) => {
                    // get vertex
                    let mut dep_vertex = dep_vertex_ref.read();

                    // if not visited, visit
                    if dep_vertex.id == 0 {
                        tracing::trace!(
                            "p{}: Finder::strong_connect non-visited {:?}",
                            self.process_id,
                            dep_dot
                        );

                        // drop guards
                        drop(vertex);
                        drop(dep_vertex);

                        // OPTIMIZATION: passing the dep vertex ref as an
                        // argument to `strong_connect` avoids double look-up
                        let result = self.strong_connect(
                            dep_dot,
                            &dep_vertex_ref,
                            executed_clock,
                            added_to_executed_clock,
                            vertex_index,
                            found,
                        );

                        // if missing dependency, give up
                        if let FinderResult::MissingDependencies(_) = result {
                            return result;
                        }

                        // get guards again
                        vertex = vertex_ref.write();
                        dep_vertex = dep_vertex_ref.read();

                        // min low with dep low
                        vertex.low = cmp::min(vertex.low, dep_vertex.low);

                        // drop dep guard
                        drop(dep_vertex);
                    } else {
                        // if visited and on the stack
                        if dep_vertex.on_stack {
                            tracing::trace!("p{}: Finder::strong_connect dependency on stack {:?}", self.process_id, dep_dot);
                            // min low with dep id
                            vertex.low = cmp::min(vertex.low, dep_vertex.id);
                        }

                        // drop dep guard
                        drop(dep_vertex);
                    }
                }
            }
        }

        // if after visiting all neighbors, an SCC was found if vertex.id ==
        // vertex.low
        // - good news: the SCC members are on the stack
        if vertex.id == vertex.low {
            let mut scc = SCC::new();

            // drop guards
            drop(vertex);
            drop(vertex_ref);

            loop {
                // pop an element from the stack
                let member_dot = self
                    .stack
                    .pop()
                    .expect("there should be an SCC member on the stack");

                tracing::debug!(
                    "p{}: Finder::strong_connect new SCC member {:?}",
                    self.process_id,
                    member_dot
                );

                // get its vertex and change its `on_stack` value
                let member_vertex_ref = vertex_index
                    .find(&member_dot)
                    .expect("stack member should exist");

                // increment number of commands found
                *found += 1;

                // get its vertex and change its `on_stack` value
                let mut member_vertex = member_vertex_ref.write();
                member_vertex.on_stack = false;

                // add it to the SCC and check it wasn't there before
                assert!(scc.insert(member_dot));

                // drop guards
                drop(member_vertex);
                drop(member_vertex_ref);

                // update executed clock:
                // - this is a nice optimization (that I think we missed in
                //   Atlas); instead of waiting for the root-level recursion to
                //   finish in order to update `executed_clock` (which is
                //   consulted to decide what are the dependencies of a
                //   command), we can update it right here, possibly reducing a
                //   few iterations

                // TODO add this check back:
                // check if the command is replicated by my shard
                // let is_mine =
                // member_vertex.cmd.replicated_by(&self.shard_id);
                // if executed_clock.write("Finder::strong_connect", |clock| {
                //     clock.add(&member_dot.source(), member_dot.sequence())
                // })
                // && is_mine
                // {
                //     panic!(
                //         "p{}: Finder::strong_connect dot {:?} already
                // executed",         self.process_id,
                // member_dot     );
                // }
                executed_clock.add(&member_dot.source(), member_dot.sequence());
                if self.config.shards() > 1 {
                    added_to_executed_clock.insert(member_dot);
                }

                tracing::trace!(
                    "p{}: Finder::strong_connect executed clock {:?}",
                    self.process_id,
                    executed_clock
                );

                // quit if root is found
                if member_dot == dot {
                    break;
                }
            }

            // add scc to to the set of sccs
            self.sccs.push(scc);
            FinderResult::Found
        } else {
            FinderResult::NotFound
        }
    }
}

#[derive(Debug, Clone)]
pub struct Vertex {
    pub dot: Dot,
    pub cmd: Command,
    pub deps: Vec<Dependency>,
    pub start_time_ms: u64,
    // specific to tarjan's algorithm
    id: usize,
    low: usize,
    on_stack: bool,
}

impl Vertex {
    pub fn new(
        dot: Dot,
        cmd: Command,
        deps: Vec<Dependency>,
        time: &dyn SysTime,
    ) -> Self {
        let start_time_ms = time.millis();
        Self {
            dot,
            cmd,
            deps,
            start_time_ms,
            id: 0,
            low: 0,
            on_stack: false,
        }
    }

    /// Consumes the vertex, returning its command.
    pub fn into_command(self, time: &dyn SysTime) -> (u64, Command) {
        let end_time_ms = time.millis();
        let duration_ms = end_time_ms - self.start_time_ms;
        (duration_ms, self.cmd)
    }
}
