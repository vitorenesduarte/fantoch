use crate::protocol::common::pred::{Clock, CaesarDots};
use fantoch::command::Command;
use fantoch::id::Dot;
use fantoch::shared::{SharedMap, SharedMapRef};
use fantoch::time::SysTime;
use fantoch::HashSet;
use parking_lot::{Mutex, RwLock};
use std::collections::BTreeSet;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Vertex {
    pub dot: Dot,
    pub cmd: Command,
    pub clock: Clock,
    pub deps: Arc<CaesarDots>,
    pub start_time_ms: u64,
    missing_deps: usize,
}

impl Vertex {
    pub fn new(
        dot: Dot,
        cmd: Command,
        clock: Clock,
        deps: Arc<CaesarDots>,
        time: &dyn SysTime,
    ) -> Self {
        let start_time_ms = time.millis();
        Self {
            dot,
            cmd,
            clock,
            deps,
            start_time_ms,
            missing_deps: 0,
        }
    }

    pub fn get_missing_deps(&self) -> usize {
        self.missing_deps
    }

    pub fn set_missing_deps(&mut self, missing_deps: usize) {
        // this value can only be written if it's at zero
        assert_eq!(self.missing_deps, 0);
        self.missing_deps = missing_deps;
    }

    // Decreases the number of missing deps by one.
    pub fn decrease_missing_deps(&mut self) {
        // this value can only be decreased if it's non zero
        assert!(self.missing_deps > 0);
        self.missing_deps -= 1;
    }

    /// Consumes the vertex, returning its command.
    pub fn into_command(self, time: &dyn SysTime) -> (u64, Command) {
        let end_time_ms = time.millis();
        let duration_ms = end_time_ms - self.start_time_ms;
        (duration_ms, self.cmd)
    }
}

#[derive(Debug, Clone)]
pub struct VertexIndex {
    index: Arc<SharedMap<Dot, RwLock<Vertex>>>,
}

impl VertexIndex {
    pub fn new() -> Self {
        Self {
            index: Arc::new(SharedMap::new()),
        }
    }

    /// Indexes a new vertex, returning any previous vertex indexed.
    pub fn index(&mut self, vertex: Vertex) -> Option<Vertex> {
        let dot = vertex.dot;
        self.index
            .insert(dot, RwLock::new(vertex))
            .map(|cell| cell.into_inner())
    }

    #[allow(dead_code)]
    pub fn dots(&self) -> BTreeSet<Dot> {
        self.index.iter().map(|entry| *entry.key()).collect()
    }

    pub fn find(
        &self,
        dot: &Dot,
    ) -> Option<SharedMapRef<'_, Dot, RwLock<Vertex>>> {
        self.index.get(dot)
    }

    /// Removes a vertex from the index.
    pub fn remove(&mut self, dot: &Dot) -> Option<Vertex> {
        self.index.remove(dot).map(|(_, cell)| cell.into_inner())
    }
}

#[derive(Debug, Clone)]
pub struct PendingIndex {
    index: Arc<SharedMap<Dot, Mutex<HashSet<Dot>>>>,
}

impl PendingIndex {
    pub fn new() -> Self {
        Self {
            index: Arc::new(SharedMap::new()),
        }
    }

    /// Indexes a new `dot` with `dep_dot` as a missing dependency.
    pub fn index(&mut self, dep_dot: &Dot, dot: Dot) {
        self.index
            .get_or(dep_dot, || Default::default())
            .lock()
            .insert(dot);
    }

    /// Finds all pending dots for a given dependency.
    pub fn remove(&mut self, dep_dot: &Dot) -> HashSet<Dot> {
        self.index
            .remove(dep_dot)
            .map(|(_, deps)| deps.into_inner())
            .unwrap_or_default()
    }
}
