use super::index::{VertexIndex, VertexRef};
use fantoch::command::Command;
use fantoch::id::{Dot, ProcessId, ShardId};
use fantoch::log;
use fantoch::time::SysTime;
use fantoch::HashSet;
use std::cmp;
use std::collections::BTreeSet;
use threshold::{AEClock, EventSet, VClock};

/// commands are sorted inside an SCC given their dot
pub type SCC = BTreeSet<Dot>;

#[derive(PartialEq)]
pub enum FinderResult {
    Found,
    MissingDependencies(HashSet<Dot>),
    NotPending,
    NotFound,
}

#[derive(Clone)]
pub struct TarjanSCCFinder {
    process_id: ProcessId,
    shard_id: ShardId,
    transitive_conflicts: bool,
    id: usize,
    stack: Vec<Dot>,
    sccs: Vec<SCC>,
}

impl TarjanSCCFinder {
    /// Creates a new SCC finder that employs Tarjan's algorithm.
    pub fn new(
        process_id: ProcessId,
        shard_id: ShardId,
        transitive_conflicts: bool,
    ) -> Self {
        Self {
            process_id,
            shard_id,
            transitive_conflicts,
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
            log!(
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
            vertex.write("Finder::finallize").id = 0;

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
        vertex_index: &VertexIndex,
        found: &mut usize,
    ) -> FinderResult {
        // update id
        self.id += 1;

        // get vertex
        let mut vertex = vertex_ref.write("Finder::strong_connect command 1/2");

        // set id and low for vertex
        vertex.id = self.id;
        vertex.low = self.id;

        // add to the stack
        vertex.on_stack = true;
        self.stack.push(dot);

        log!(
            "p{}: Finder::strong_connect {:?} with id {}",
            self.process_id,
            dot,
            self.id
        );

        let ignore_dep =
            |process_id: ProcessId,
             dep: u64,
             executed_clock: &AEClock<ProcessId>| {
                let dep_dot = Dot::new(process_id, dep);
                // ignore if self or if already executed:

                // - we need this check because the clock may not be contiguous,
                //   i.e. `executed_clock_frontier` is simply a safe
                //   approximation of what's been executed
                let ignore =
                    dot == dep_dot || executed_clock.contains(&process_id, dep);
                (ignore, dep_dot)
            };

        // TODO can we avoid vertex.clock.clone()
        // compute non-executed deps for each process
        let clock = vertex.clock.clone();
        let mut deps_iter = clock.into_iter();
        while let Some((process_id, to)) = deps_iter.next() {
            // get min event from which we need to start checking for
            // dependencies
            let to = to.frontier();
            let from = if self.transitive_conflicts {
                // if we can assume that conflicts are transitive, it is enough
                // to check for the highest dependency
                to
            } else {
                executed_clock
                    .get(&process_id)
                    .expect("process should exist in the executed clock")
                    .frontier()
                    + 1
            };

            // OPTIMIZATION: start from the highest dep to the lowest:
            // - assuming we will give up, we give up faster this way
            // THE BENEFITS ARE HUGE!!!
            // - obviously, this is only relevant when we can't assume that
            //   conflicts are transitive
            // - when we can, the following loop has a single iteration
            for dep in (from..=to).rev() {
                let (ignore, dep_dot) =
                    ignore_dep(process_id, dep, executed_clock);
                if ignore {
                    log!(
                        "p{}: Finder::strong_connect {:?} dependency ignored",
                        self.process_id,
                        dep_dot
                    );
                    continue;
                }

                match vertex_index.find(&dep_dot) {
                    None => {
                        // not necesserarily a missing dependency, since it may
                        // not conflict with `dot` but
                        // we can't be sure until we have it locally
                        log!(
                            "p{}: Finder::strong_connect missing {:?}",
                            self.process_id,
                            dep_dot
                        );
                        let deps = std::iter::once(dep_dot)
                            // add remaining frontier deps as missing dependencies
                            .chain(deps_iter.filter_map(|(process_id, to)| {
                                let (ignore, dep_dot) = ignore_dep(
                                    process_id,
                                    to.frontier(),
                                    executed_clock,
                                );
                                if ignore {
                                    None
                                } else {
                                    Some(dep_dot)
                                }
                            }))
                            .collect();
                        return FinderResult::MissingDependencies(deps);
                    }
                    Some(dep_vertex_ref) => {
                        // get vertex
                        let mut dep_vertex = dep_vertex_ref
                            .read("Finder::strong_connect dependency 1/2");

                        // ignore non-conflicting commands:
                        // - this check is only necesssary if we can't assume
                        //   that conflicts are transitive
                        if !self.transitive_conflicts
                            && !vertex.conflicts(&dep_vertex)
                        {
                            log!(
                                "p{}: Finder::strong_connect non-conflicting {:?}",
                                self.process_id,
                                dep_dot
                            );
                            continue;
                        }

                        // if not visited, visit
                        if dep_vertex.id == 0 {
                            log!(
                                "p{}: Finder::strong_connect non-visited {:?}",
                                self.process_id,
                                dep_dot
                            );

                            // drop guards
                            drop(vertex);
                            drop(dep_vertex);

                            // OPTIMIZATION: passing the vertex as an argument
                            // to `strong_connect`
                            // is also essential to avoid double look-up
                            let result = self.strong_connect(
                                dep_dot,
                                &dep_vertex_ref,
                                executed_clock,
                                vertex_index,
                                found,
                            );

                            // if missing dependency, give up
                            if let FinderResult::MissingDependencies(_) = result
                            {
                                return result;
                            }

                            // get guards again
                            vertex = vertex_ref
                                .write("Finder::strong_connect command 2/2");
                            dep_vertex = dep_vertex_ref
                                .read("Finder::strong_connect dependency 2/2");

                            // min low with dep low
                            vertex.low = cmp::min(vertex.low, dep_vertex.low);

                            // drop dep guard
                            drop(dep_vertex);
                        } else {
                            // if visited and on the stack
                            if dep_vertex.on_stack {
                                log!("p{}: Finder::strong_connect dependency on stack {:?}", self.process_id, dep_dot);
                                // min low with dep id
                                vertex.low =
                                    cmp::min(vertex.low, dep_vertex.id);
                            }

                            // drop dep guard
                            drop(dep_vertex);
                        }
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

                log!(
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
                let mut member_vertex = member_vertex_ref
                    .write("Finder::strong_connect SCC member");
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
                // let is_mine = member_vertex.cmd.replicated_by(&self.shard_id);
                // if executed_clock.write("Finder::strong_connect", |clock| {
                //     clock.add(&member_dot.source(), member_dot.sequence())
                // })
                // && is_mine
                // {
                //     panic!(
                //         "p{}: Finder::strong_connect dot {:?} already executed",
                //         self.process_id, member_dot
                //     );
                // }
                executed_clock.add(&member_dot.source(), member_dot.sequence());

                log!(
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
    dot: Dot,
    pub cmd: Command,
    pub clock: VClock<ProcessId>,
    start_time: u64,
    // specific to tarjan's algorithm
    id: usize,
    low: usize,
    on_stack: bool,
}

impl Vertex {
    pub fn new(
        dot: Dot,
        cmd: Command,
        clock: VClock<ProcessId>,
        time: &dyn SysTime,
    ) -> Self {
        let start_time = time.millis();
        Self {
            dot,
            cmd,
            clock,
            start_time,
            id: 0,
            low: 0,
            on_stack: false,
        }
    }

    /// Consumes the vertex, returning its command.
    pub fn into_command(self, time: &dyn SysTime) -> (u64, Command) {
        let end_time = time.millis();
        let duration = end_time - self.start_time;
        (duration, self.cmd)
    }

    /// Retrieves vertex's dot.
    pub fn dot(&self) -> Dot {
        self.dot
    }

    /// This vertex conflicts with another vertex by checking if their commands
    /// conflict.
    fn conflicts(&self, other: &Vertex) -> bool {
        self.cmd.conflicts(&other.cmd)
    }
}
