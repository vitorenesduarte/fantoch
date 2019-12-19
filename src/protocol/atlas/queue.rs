use crate::command::Command;
use crate::id::{Dot, ProcessId};
use crate::kvs::Key;
use crate::util;
use std::cell::{Ref, RefCell, RefMut};
use std::cmp;
use std::collections::{HashMap, HashSet, VecDeque};
use threshold::{AEClock, VClock};

#[allow(dead_code)]
pub struct Queue {
    executed_clock: AEClock<ProcessId>,
    vertex_index: VertexIndex,
    pending_index: PendingIndex,
}

impl Queue {
    /// Create a new `Queue`.
    #[allow(dead_code)]
    pub fn new(n: usize) -> Self {
        // create bottom executed clock
        let ids = util::process_ids(n);
        let executed_clock = AEClock::with(ids);
        // create indexes
        let vertex_index = VertexIndex::new();
        let pending_index = PendingIndex::new();
        Self {
            executed_clock,
            vertex_index,
            pending_index,
        }
    }

    /// Add a new command with its clock to the queue.
    #[must_use]
    #[allow(dead_code)]
    pub fn add(&mut self, dot: Dot, cmd: Command, clock: VClock<ProcessId>) -> Vec<Command> {
        // create new vertex for this command
        let vertex = Vertex::new(dot, cmd, clock);

        // index vertex
        self.index(vertex);

        // find a new scc
        let colors = self.find_scc(dot);

        //
        vec![]
    }

    fn index(&mut self, vertex: Vertex) {
        // index in pending index
        self.pending_index.index(&vertex);

        // index in vertex index and check if it hasn't been indexed before
        assert!(self.vertex_index.index(vertex));
    }

    fn find_scc(&mut self, dot: Dot) -> HashSet<Key> {
        // execute tarjan's algorithm
        let mut finder = TarjanSCCFinder::new();
        let finder_result = finder.strong_connect(dot, &self.executed_clock, &self.vertex_index);

        //
        HashSet::new()
    }
}

#[derive(Default)]
struct VertexIndex {
    index: HashMap<Dot, RefCell<Vertex>>,
}

impl VertexIndex {
    fn new() -> Self {
        Default::default()
    }

    /// Indexes a new vertex, returning whether a vertex with this dot was already indexed or not.
    fn index(&mut self, vertex: Vertex) -> bool {
        let res = self.index.insert(vertex.dot, RefCell::new(vertex));
        res.is_none()
    }

    /// Returns a mutable reference to an indexed vertex (if previously indexed).
    fn get_mut(&self, dot: &Dot) -> Option<RefMut<Vertex>> {
        self.index.get(dot).map(|cell| cell.borrow_mut())
    }

    /// Returns a reference to an indexed vertex (if previously indexed).
    fn get(&self, dot: &Dot) -> Option<Ref<Vertex>> {
        self.index.get(dot).map(|cell| cell.borrow())
    }
}

#[derive(Default)]
struct PendingIndex {
    index: HashMap<Key, Vec<Dot>>,
}

impl PendingIndex {
    fn new() -> Self {
        Default::default()
    }

    fn index(&mut self, vertex: &Vertex) {
        vertex.cmd.keys().for_each(|key| {
            // get current set of pending commands for this key
            let pending = match self.index.get_mut(key) {
                Some(pending) => pending,
                None => self.index.entry(key.clone()).or_insert(Vec::new()),
            };
            // add to pending
            pending.push(vertex.dot);
        });
    }
}

#[derive(PartialEq)]
enum FinderResult {
    Found,
    NotFound,
    MissingDependency,
}

struct TarjanSCCFinder {
    id: usize,
    stack: VecDeque<Dot>,
    sccs: Vec<Vec<Dot>>,
}

impl TarjanSCCFinder {
    fn new() -> Self {
        Self {
            id: 0,
            stack: VecDeque::new(),
            sccs: Vec::new(),
        }
    }

    fn strong_connect(
        &mut self,
        dot: Dot,
        executed_clock: &AEClock<ProcessId>,
        vertex_index: &VertexIndex,
    ) -> FinderResult {
        // get the vertex
        let mut vertex = vertex_index.get_mut(&dot).expect("root vertex must exist");

        // update id
        self.id += 1;

        // set id and low for vertex
        vertex.set_id(self.id);
        vertex.update_low(|_| self.id);

        // add to the stack
        vertex.set_on_stack(true);
        self.stack.push_front(dot);

        // for all deps
        for (process_id, frontier) in vertex.clock().frontier() {
            // compute non-executed deps for each process
            for dep in executed_clock.subtract_iter(process_id, frontier) {
                // create dot and find vertex
                let dep_dot = Dot::new(*process_id, dep);
                match vertex_index.get(&dep_dot) {
                    None => {
                        // not necesserarily a missing dependency, since it may not conflict
                        // with `dot` but we can't be sure until we have it locally
                        return FinderResult::MissingDependency;
                    }
                    Some(dep_vertex) => {
                        // ignore non-conflicting commands
                        if !vertex.conflicts(&dep_vertex) {
                            continue;
                        }

                        // if not visited, visit
                        if dep_vertex.id() == 0 {
                            let result = self.strong_connect(dep_dot, executed_clock, vertex_index);
                            if result == FinderResult::MissingDependency {
                                return result;
                            }

                            // min low with dep low
                            vertex
                                .update_low(|current_low| cmp::min(current_low, dep_vertex.low()));
                        } else {
                            // if visited and on the stack:w
                            if dep_vertex.on_stack() {
                                // min low with dep id
                                vertex.update_low(|low| cmp::min(low, dep_vertex.id()));
                            }
                        }
                    }
                }
            }
        }

        // if after visiting all neighbors, an SCC was found if vertex.id == vertex.low
        // - good news: the SCC members are on the stack

        if vertex.id() == vertex.low() {
            let mut scc = Vec::new();

            loop {
                // pop an element from the stack
                let member_dot = self
                    .stack
                    .pop_front()
                    .expect("there should be an SCC member on the stack");

                // get its vertex and change its `on_stack` value
                let mut member_vertex = vertex_index
                    .get_mut(&member_dot)
                    .expect("SCC member must exist");
                member_vertex.set_on_stack(false);

                // add it to the SCC
                scc.push(member_dot);

                // quit if root is found
                if member_dot == dot {
                    break;
                }
            }
            // add scc to to the set of sccs
            self.sccs.push(scc);
            return FinderResult::Found;
        }

        return FinderResult::NotFound;
    }
}

struct Vertex {
    dot: Dot,
    cmd: Command,
    clock: VClock<ProcessId>,
    // specific to tarjan's algorithm
    id: usize,
    low: RefCell<usize>,
    on_stack: bool,
}

impl Vertex {
    fn new(dot: Dot, cmd: Command, clock: VClock<ProcessId>) -> Self {
        Self {
            dot,
            cmd,
            clock,
            id: 0,
            low: RefCell::new(0),
            on_stack: false,
        }
    }

    /// This vertex conflicts with another vertex by checking if their commands conflict.
    fn conflicts(&self, other: &Vertex) -> bool {
        self.cmd.conflicts(&other.cmd)
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
        *self.low.borrow()
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
    fn update_low<F>(&self, update: F)
    where
        F: FnOnce(usize) -> usize,
    {
        let current_low = self.low();
        *self.low.borrow_mut() = update(current_low);
    }

    /// Sets if vertex is on the stack or not.
    fn set_on_stack(&mut self, on_stack: bool) {
        self.on_stack = on_stack;
    }
}
