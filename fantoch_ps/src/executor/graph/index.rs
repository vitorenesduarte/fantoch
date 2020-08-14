use super::tarjan::Vertex;
use crate::shared::Shared;
use dashmap::mapref::one::Ref as DashMapRef;
use fantoch::config::Config;
use fantoch::hash_map::{Entry, HashMap};
use fantoch::id::{Dot, ProcessId, ShardId};
use fantoch::HashSet;
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
        let cell = RwLock::new(vertex);
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
    process_id: ProcessId,
    shard_id: ShardId,
    config: Config,
    index: HashMap<Dot, HashSet<Dot>>,
    // `mine` is only used in partial replication
    mine: HashSet<Dot>,
}

impl PendingIndex {
    pub fn new(
        process_id: ProcessId,
        shard_id: ShardId,
        config: Config,
    ) -> Self {
        Self {
            process_id,
            shard_id,
            config,
            index: HashMap::new(),
            mine: HashSet::new(),
        }
    }

    pub fn add_mine(&mut self, dot: Dot) {
        assert!(self.mine.insert(dot));
    }

    pub fn is_mine(&mut self, dot: &Dot) -> bool {
        self.mine.contains(dot)
    }

    /// Indexes a new `dot` as a child of `dep_dot`:
    /// - when `dep_dot` is executed, we'll try to execute `dot` as `dep_dot`
    ///   was a dependency and maybe now `dot` can be executed
    #[must_use]
    pub fn index(&mut self, dep_dot: Dot, dot: Dot) -> Option<ShardId> {
        match self.index.entry(dep_dot) {
            Entry::Vacant(vacant) => {
                // save `dot`
                let mut dots = HashSet::new();
                dots.insert(dot);
                vacant.insert(dots);

                // this is the first time we detect `dep_dot` as a missing
                // dependency; in this case, we may have to ask another
                // shard for its info; from the identifier we can't know if we
                // replicate the command, but can know who the target shard is.
                // since we don't have the command, we try our best by tracking
                // in `self.mine` commands that we replicate but we are not
                // their target shard.
                // NOTE: this is a best effort only; it's totally possible that
                // we replicate a command but don't have it *yet* in `self.mine`
                // when such command is first declared as a dependency
                // NOTE: we're always the target shard in full replication, and
                // for that reason, the following is a noop
                let target = dep_dot.target_shard(self.config.n());
                if target != self.shard_id && !self.mine.contains(&dep_dot) {
                    return Some(target);
                }
            }
            Entry::Occupied(mut dots) => {
                // in this case, `dep_dot` has already been a missing dependency
                // of another command, so simply save `dot` as a child
                dots.get_mut().insert(dot);
            }
        }
        None
    }

    /// Finds all pending dots for a given dependency dot.
    pub fn remove(&mut self, dep_dot: &Dot) -> Option<HashSet<Dot>> {
        self.mine.remove(dep_dot);
        self.index.remove(dep_dot)
    }
}
