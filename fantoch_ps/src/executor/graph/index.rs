use super::tarjan::Vertex;
use crate::shared::Shared;
use dashmap::mapref::one::Ref as DashMapRef;
use fantoch::id::{Dot, ProcessId, ShardId};
use fantoch::{HashMap, HashSet};
use parking_lot::{Mutex, MutexGuard};
use std::sync::Arc;

pub struct VertexRef<'a> {
    r: DashMapRef<'a, Dot, Mutex<Vertex>>,
}

impl<'a> VertexRef<'a> {
    fn new(r: DashMapRef<'a, Dot, Mutex<Vertex>>) -> Self {
        Self { r }
    }

    pub fn lock(&self) -> MutexGuard<'_, Vertex> {
        self.r.lock()
    }
}

#[derive(Debug, Clone)]
pub struct VertexIndex {
    process_id: ProcessId,
    local: Arc<Shared<Dot, Mutex<Vertex>>>,
}

impl VertexIndex {
    pub fn new(process_id: ProcessId) -> Self {
        Self {
            process_id,
            local: Arc::new(Shared::new()),
        }
    }

    /// Indexes a new vertex, returning any previous vertex indexed.
    pub fn index(&mut self, vertex: Vertex) -> Option<Vertex> {
        let dot = vertex.dot();
        let cell = Mutex::new(vertex);
        self.local.insert(dot, cell).map(|cell| cell.into_inner())
    }

    pub fn dots(&self) -> impl Iterator<Item = Dot> + '_ {
        self.local.iter().map(|entry| *entry.key())
    }

    pub fn find(&self, dot: &Dot) -> Option<VertexRef<'_>> {
        // search first in the local index
        self.local.get(dot).map(VertexRef::new)
    }

    /// Removes a vertex from the index.
    pub fn remove(&mut self, dot: &Dot) -> Option<Vertex> {
        self.local.remove(dot).map(|(_, cell)| cell.into_inner())
    }
}

#[derive(Debug, Clone)]
pub struct PendingIndex {
    shard_id: ShardId,
    n: usize,
    index: HashMap<Dot, HashSet<Dot>>,
}

impl PendingIndex {
    pub fn new(shard_id: ShardId, n: usize) -> Self {
        Self {
            shard_id,
            n,
            index: HashMap::new(),
        }
    }

    /// Indexes a new `dot` as a child of `dep_dot`:
    /// - when `dep_dot` is executed, we'll try to execute `dot` as `dep_dot`
    ///   was a dependency and maybe now `dot` can be executed
    #[must_use]
    pub fn index(&mut self, dep_dot: Dot, dot: Dot) -> Option<ShardId> {
        match self.index.get_mut(&dep_dot) {
            None => {
                // this is the first time we detect `dep_dot` as a missing
                // dependency; in this case, we may have to request another
                // shard for its info; from the identifier we can know if our
                // shard was the target shard, but maybe we were not the target
                // shard and yet replicate the command; however, as we don't
                // have the command, only its identifier, we will be pessimistic
                // and send a request for all commands that:
                // - are missing dependencies *and*
                // - we were not its target shard
                self.index.entry(dep_dot).or_default().insert(dot);
                let target = dep_dot.target_shard(self.n);
                if target != self.shard_id {
                    return Some(target);
                }
            }
            Some(dots) => {
                // in this case, `dep_dot` has already been a missing dependency
                // of another command, so simply save `dot` as a child
                dots.insert(dot);
            }
        }
        None
    }

    /// Finds all pending dots for a given dependency dot.
    pub fn remove(&mut self, dep_dot: &Dot) -> Option<HashSet<Dot>> {
        self.index.remove(dep_dot)
    }
}
