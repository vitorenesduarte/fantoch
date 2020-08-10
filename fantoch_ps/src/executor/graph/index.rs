use super::tarjan::Vertex;
use crate::shared::Shared;
use dashmap::mapref::one::Ref as DashMapRef;
use fantoch::id::{Dot, ProcessId};
use fantoch::{HashMap, HashSet};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::Arc;

pub struct VertexRef<'a> {
    r: DashMapRef<'a, Dot, RwLock<Vertex>>,
}

impl<'a> VertexRef<'a> {
    fn new(r: DashMapRef<'a, Dot, RwLock<Vertex>>) -> Self {
        Self { r }
    }

    pub fn read(&self) -> RwLockReadGuard<'_, Vertex> {
        self.r.read()
    }

    pub fn write(&self) -> RwLockWriteGuard<'_, Vertex> {
        self.r.write()
    }
}

#[derive(Debug, Clone)]
pub struct VertexIndex {
    process_id: ProcessId,
    local: Arc<Shared<Dot, RwLock<Vertex>>>,
    remote: Arc<Shared<Dot, RwLock<Vertex>>>,
}

impl VertexIndex {
    pub fn new(process_id: ProcessId) -> Self {
        Self {
            process_id,
            local: Arc::new(Shared::new()),
            remote: Arc::new(Shared::new()),
        }
    }

    /// Indexes a new vertex, returning true if it was already indexed.
    pub fn index(&mut self, vertex: Vertex, is_mine: bool) -> bool {
        let dot = vertex.dot();
        let cell = RwLock::new(vertex);
        if is_mine {
            self.local.insert(dot, cell).is_some()
        } else {
            self.remote.insert(dot, cell).is_some()
        }
    }

    pub fn local_dots(&self) -> impl Iterator<Item = Dot> + '_ {
        self.local.iter().map(|entry| *entry.key())
    }

    pub fn remote_dots(&self) -> impl Iterator<Item = Dot> + '_ {
        self.remote.iter().map(|entry| *entry.key())
    }

    pub fn find(&self, dot: &Dot) -> Option<VertexRef<'_>> {
        // search first in the local index
        self.local
            .get(dot)
            .or_else(|| self.remote.get(dot))
            .map(VertexRef::new)
    }

    /// Removes a vertex from the index.
    pub fn remove(&mut self, dot: &Dot) -> Option<Vertex> {
        self.local
            .remove(dot)
            .map(|(_, cell)| cell.into_inner())
            .or_else(|| self.remove_remote(dot))
    }

    /// Removes a remote vertex from the index.
    pub fn remove_remote(&mut self, dot: &Dot) -> Option<Vertex> {
        self.remote.remove(dot).map(|(_, cell)| cell.into_inner())
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
