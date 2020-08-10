use super::tarjan::Vertex;
use crate::shared::Shared;
use fantoch::id::{Dot, ProcessId};
use fantoch::{HashMap, HashSet};
use parking_lot::RwLock;
use std::cell::UnsafeCell;
use std::sync::Arc;
use threshold::AEClock;

#[derive(Debug)]
struct Cell(UnsafeCell<Vertex>);

impl Clone for Cell {
    fn clone(&self) -> Self {
        panic!("impossible to clone a Cell");
    }
}

#[derive(Debug)]
pub struct VertexIndex {
    local: Shared<Dot, Cell>,
    remote: Shared<Dot, Cell>,
}

impl Clone for VertexIndex {
    fn clone(&self) -> Self {
        assert!(
            self.local.is_empty(),
            "it's only possible to clone an empty VertexIndex"
        );
        assert!(
            self.remote.is_empty(),
            "it's only possible to clone an empty VertexIndex"
        );
        Self {
            local: self.local.clone(),
            remote: self.remote.clone(),
        }
    }
}

impl VertexIndex {
    pub fn new() -> Self {
        Self {
            local: Shared::new(),
            remote: Shared::new(),
        }
    }

    /// Indexes a new vertex, returning true if it was already indexed.
    pub fn index(
        &mut self,
        vertex: Vertex,
        is_mine: bool,
        executed_clock: &Arc<RwLock<AEClock<ProcessId>>>,
    ) -> bool {
        let dot = vertex.dot();
        let cell = Cell(UnsafeCell::new(vertex));
        if is_mine {
            self.local.insert(dot, cell).is_some()
        } else {
            // if it's a remote command, only index it if we haven't been told
            // already that it has been executed in a remote shard
            if !executed_clock.read().contains(&dot.source(), dot.sequence()) {
                self.remote.insert(dot, cell).is_some()
            } else {
                false
            }
        }
    }

    pub fn local_dots(&self) -> impl Iterator<Item = Dot> + '_ {
        self.local.iter().map(|entry| *entry.key())
    }

    pub fn remote_dots(&self) -> impl Iterator<Item = Dot> + '_ {
        self.remote.iter().map(|entry| *entry.key())
    }

    pub fn get_mut(&self, dot: &Dot) -> Option<&mut Vertex> {
        // search first in the local index
        self.local
            .get(dot)
            .or_else(|| self.remote.get(dot))
            .map(|cell| unsafe { &mut *cell.0.get() })
    }

    /// Removes a vertex from the index.
    pub fn remove(&mut self, dot: &Dot) -> Option<Vertex> {
        self.local
            .remove(dot)
            .or_else(|| self.remote.remove(dot))
            .map(|(_, cell)| cell.0.into_inner())
    }

    /// Removes a remote vertex from the index.
    pub fn remove_remote(&mut self, dot: &Dot) {
        self.remote.remove(dot);
    }
}

#[derive(Default, Debug, Clone)]
pub struct PendingIndex {
    index: HashMap<Dot, HashSet<Dot>>,
}

impl PendingIndex {
    pub fn new() -> Self {
        Default::default()
    }

    /// Indexes a new `dot` as a child of `dep_dot`:
    /// - when `dep_dot` is executed, we'll try to execute `dot` as `dep_dot`
    ///   was a dependency and maybe now `dot` can be executed
    pub fn index(&mut self, dep_dot: Dot, dot: Dot) {
        // get current list of pending dots
        let pending = self.index.entry(dep_dot).or_insert_with(HashSet::new);
        // add new `dot` to pending
        pending.insert(dot);
    }

    /// Finds all pending dots for a given dependency dot.
    pub fn remove(&mut self, dep_dot: &Dot) -> Option<HashSet<Dot>> {
        self.index.remove(dep_dot)
    }
}
