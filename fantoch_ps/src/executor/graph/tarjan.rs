use super::index::VertexIndex;
use fantoch::command::Command;
use fantoch::id::{Dot, ProcessId};
use fantoch::log;
use fantoch::HashSet;
use std::cell::RefCell;
use std::cmp;
use std::collections::BTreeSet;
use threshold::{AEClock, EventSet, VClock};

/// commands are sorted inside an SCC given their dot
pub type SCC = BTreeSet<Dot>;

#[derive(PartialEq)]
pub enum FinderResult {
    Found,
    MissingDependency(Dot),
    NotPending,
    NotFound,
}

pub struct TarjanSCCFinder {
    process_id: ProcessId,
    transitive_conflicts: bool,
    id: usize,
    stack: Vec<Dot>,
    sccs: Vec<SCC>,
}

impl TarjanSCCFinder {
    /// Creates a new SCC finder that employs Tarjan's algorithm.
    pub fn new(process_id: ProcessId, transitive_conflicts: bool) -> Self {
        Self {
            process_id,
            transitive_conflicts,
            id: 0,
            stack: Vec::new(),
            sccs: Vec::new(),
        }
    }

    /// Returns a list with the SCCs found and a set with all dots visited.
    /// It also resets the ids of all vertices still on the stack.
    #[must_use]
    pub fn finalize(
        self,
        vertex_index: &VertexIndex,
    ) -> (Vec<SCC>, HashSet<Dot>) {
        let _process_id = self.process_id;
        // reset the id of each dot in the stack, while computing the set of
        // visited dots
        let visited = self
            .stack
            .into_iter()
            .map(|dot| {
                log!(
                    "p{}: Finder::finalize removing {:?} from stack",
                    _process_id,
                    dot
                );

                // find vertex and reset its id
                let vertex_cell =
                    vertex_index.find(&dot).expect("stack member should exist");
                vertex_cell.borrow_mut().set_id(0);

                // add dot to set of visited
                dot
            })
            .collect();
        // return SCCs found and visited dots
        (self.sccs, visited)
    }

    /// Tries to find an SCC starting from root `dot`.
    pub fn strong_connect(
        &mut self,
        dot: Dot,
        vertex_cell: &RefCell<Vertex>,
        executed_clock: &mut AEClock<ProcessId>,
        vertex_index: &VertexIndex,
        ready_commands: &mut usize,
    ) -> FinderResult {
        // borrow the vertex mutably
        let mut vertex = vertex_cell.borrow_mut();

        // update id
        self.id += 1;

        log!(
            "p{}: Finder::strong_connect {:?} with id {}",
            self.process_id,
            dot,
            self.id
        );

        // set id and low for vertex
        vertex.set_id(self.id);
        vertex.update_low(|_| self.id);

        // add to the stack
        vertex.set_on_stack(true);
        self.stack.push(dot);

        // TODO can we avoid vertex.clock().clone()
        // - if rust understood mutability of struct fields, the clone wouldn't
        //   be necessary
        // compute non-executed deps for each process
        for (process_id, to) in vertex.clock().clone().iter() {
            // get min event from which we need to start checking for
            // dependencies
            let to = to.frontier();
            let from = if self.transitive_conflicts {
                // if we can assume that conflicts are transitive, it is enough
                // to check for the highest dependency
                to
            } else {
                let executed = executed_clock
                    .get(process_id)
                    .expect("process should exist in the executed clock");
                executed.frontier() + 1
            };

            // OPTIMIZATION: start from the highest dep to the lowest:
            // - assuming we will give up, we give up faster this way
            // THE BENEFITS ARE HUGE!!!
            // - obviously, this is only relevant when we can't assume that
            //   conflicts are transitive
            // - when we can, the following loop has a single iteration
            for dep in (from..=to).rev() {
                // ignore dependency if already executed:
                // - we need this check because the clock may not be contiguous,
                //   i.e. `executed_clock_frontier` is simply a safe
                //   approximation of what's been executed
                if executed_clock.contains(process_id, dep) {
                    continue;
                }

                // create dot and find vertex
                let dep_dot = Dot::new(*process_id, dep);
                log!(
                    "p{}: Finder::strong_connect non-executed {:?}",
                    self.process_id,
                    dep_dot
                );

                // ignore dependency if self
                if dep_dot == dot {
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
                        return FinderResult::MissingDependency(dep_dot);
                    }
                    Some(dep_vertex_cell) => {
                        // ignore non-conflicting commands:
                        // - this check is only necesssary if we can't assume
                        //   that conflicts are trnasitive
                        let mut dep_vertex = dep_vertex_cell.borrow();
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
                        if dep_vertex.id() == 0 {
                            log!(
                                "p{}: Finder::strong_connect non-visited {:?}",
                                self.process_id,
                                dep_dot
                            );

                            // drop borrow guards
                            drop(vertex);
                            drop(dep_vertex);

                            // OPTIMIZATION: passing the vertex as an argument
                            // to `strong_connect`
                            // is also essential to avoid double look-up
                            let result = self.strong_connect(
                                dep_dot,
                                &dep_vertex_cell,
                                executed_clock,
                                vertex_index,
                                ready_commands,
                            );

                            // borrow again
                            vertex = vertex_cell.borrow_mut();
                            dep_vertex = dep_vertex_cell.borrow();

                            // if missing dependency, give up
                            if let FinderResult::MissingDependency(_) = result {
                                return result;
                            }

                            // min low with dep low
                            vertex.update_low(|low| {
                                cmp::min(low, dep_vertex.low())
                            });

                            // drop dep borrow
                            drop(dep_vertex);
                        } else {
                            // if visited and on the stack
                            if dep_vertex.on_stack() {
                                log!("p{}: Finder::strong_connect dependency on stack {:?}", self.process_id, dep_dot);
                                // min low with dep id
                                vertex.update_low(|low| {
                                    cmp::min(low, dep_vertex.id())
                                });
                            }

                            // drop dep borrow
                            drop(dep_vertex);
                        }
                    }
                }
            }
        }

        // if after visiting all neighbors, an SCC was found if vertex.id ==
        // vertex.low
        // - good news: the SCC members are on the stack
        if vertex.id() == vertex.low() {
            let mut scc = SCC::new();

            // drop borrow
            drop(vertex);

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

                // increment ready count
                *ready_commands += 1;

                // get its vertex and change its `on_stack` value
                let mut member_vertex = vertex_index
                    .find(&member_dot)
                    .expect("stack member should exist")
                    .borrow_mut();
                member_vertex.set_on_stack(false);

                // add it to the SCC and check it wasn't there before
                assert!(scc.insert(member_dot));

                // update executed clock:
                // - this is a nice optimization (that I think we missed in
                //   Atlas); instead of waiting for the root-level recursion to
                //   finish in order to update `executed_clock` (which is
                //   consulted to decide what are the dependencies of a
                //   command), we can update it right here, possibly reducing a
                //   few iterations
                assert!(executed_clock
                    .add(&member_dot.source(), member_dot.sequence()));
                log!(
                    "p{}: Finder:strong_connect executed clock {:?}",
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
    cmd: Command,
    clock: VClock<ProcessId>,
    // specific to tarjan's algorithm
    id: usize,
    low: usize,
    on_stack: bool,
}

impl Vertex {
    pub fn new(dot: Dot, cmd: Command, clock: VClock<ProcessId>) -> Self {
        Self {
            dot,
            cmd,
            clock,
            id: 0,
            low: 0,
            on_stack: false,
        }
    }

    /// Consumes the vertex, returning its command.
    pub fn into_command(self) -> Command {
        self.cmd
    }

    /// Retrieves vertex's dot.
    pub fn dot(&self) -> Dot {
        self.dot
    }

    /// Retrieves vertex's clock.
    fn clock(&self) -> &VClock<ProcessId> {
        &self.clock
    }

    /// Retrieves vertex's id.
    fn id(&self) -> usize {
        self.id
    }

    /// Retrieves vertex's low.
    fn low(&self) -> usize {
        self.low
    }

    /// Check if vertex is on the stack.
    fn on_stack(&self) -> bool {
        self.on_stack
    }

    /// Sets vertex's id.
    fn set_id(&mut self, id: usize) {
        self.id = id;
    }

    /// Updates vertex's low.
    fn update_low<F>(&mut self, update: F)
    where
        F: FnOnce(usize) -> usize,
    {
        self.low = update(self.low);
    }

    /// Sets if vertex is on the stack or not.
    fn set_on_stack(&mut self, on_stack: bool) {
        self.on_stack = on_stack;
    }

    /// This vertex conflicts with another vertex by checking if their commands
    /// conflict.
    fn conflicts(&self, other: &Vertex) -> bool {
        self.cmd.conflicts(&other.cmd)
    }
}
