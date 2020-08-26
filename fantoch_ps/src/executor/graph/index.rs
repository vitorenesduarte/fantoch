use super::tarjan::Vertex;
use crate::protocol::common::graph::Dependency;
use crate::shared::Shared;
use dashmap::mapref::one::Ref as DashMapRef;
use fantoch::config::Config;
use fantoch::hash_map::{Entry, HashMap};
use fantoch::id::{Dot, ProcessId, ShardId};
use fantoch::time::SysTime;
use fantoch::HashSet;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Duration;
use threshold::AEClock;

pub type VertexRef<'a> = DashMapRef<'a, Dot, RwLock<Vertex>>;

#[derive(Debug, Clone)]
pub struct VertexIndex {
    process_id: ProcessId,
    index: Arc<Shared<Dot, RwLock<Vertex>>>,
}

impl VertexIndex {
    pub fn new(process_id: ProcessId) -> Self {
        Self {
            process_id,
            index: Arc::new(Shared::new()),
        }
    }

    /// Indexes a new vertex, returning any previous vertex indexed.
    pub fn index(&mut self, vertex: Vertex) -> Option<Vertex> {
        let dot = vertex.dot;
        let cell = RwLock::new(vertex);
        self.index.insert(dot, cell).map(|cell| cell.into_inner())
    }

    #[cfg(debug_assertions)]
    pub fn dots(&self) -> impl Iterator<Item = Dot> + '_ {
        self.index.iter().map(|entry| *entry.key())
    }

    pub fn find(&self, dot: &Dot) -> Option<VertexRef<'_>> {
        self.index.get(dot)
    }

    /// Removes a vertex from the index.
    pub fn remove(&mut self, dot: &Dot) -> Option<Vertex> {
        self.index.remove(dot).map(|(_, cell)| cell.into_inner())
    }

    pub fn show_pending(
        &self,
        executed_clock: &AEClock<ProcessId>,
        pending_for: Duration,
        time: &dyn SysTime,
    ) {
        // first show executed clock
        println!(
            "p{}: executed before showing pending {:?}",
            self.process_id, executed_clock
        );

        // show pending commands
        let now_ms = time.millis();
        let pending_for_ms = pending_for.as_millis() as u64;
        self.index.iter().for_each(|vertex_ref| {
            let vertex = vertex_ref.read();
            if now_ms - vertex.start_time_ms >= pending_for_ms {
                println!(
                    "p{}: {:?} is pending with deps {:?}",
                    self.process_id, vertex.dot, vertex.deps
                );
            }
        })
    }
}

#[derive(Debug, Clone)]
pub struct PendingIndex {
    process_id: ProcessId,
    shard_id: ShardId,
    config: Config,
    index: HashMap<Dot, HashSet<Dot>>,
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
        }
    }

    /// Indexes a new `dot` as a child of `dep`:
    /// - when `dep.dot` is executed, we'll try to execute `dot` as `dep.dot`
    ///   was a dependency and maybe now `dot` can be executed
    #[must_use]
    pub fn index(
        &mut self,
        dep: Dependency,
        dot: Dot,
    ) -> Option<(Dot, ShardId)> {
        match self.index.entry(dep.dot) {
            Entry::Vacant(vacant) => {
                // save `dot`
                let mut dots = HashSet::new();
                dots.insert(dot);
                vacant.insert(dots);

                // this is the first time we detect `dep.dot` as a missing
                // dependency; in this case, we may have to ask another
                // shard for its info if we don't replicate it; we can know if
                // we replicate it by inspecting `dep.shards`
                let is_mine = dep
                    .shards
                    .as_ref()
                    .expect("dep shards should be set if it's not a noop")
                    .contains(&self.shard_id);
                if !is_mine {
                    let target = dep.dot.target_shard(self.config.n());
                    return Some((dep.dot, target));
                }
            }
            Entry::Occupied(mut dots) => {
                // in this case, `dep.dot` has already been a missing dependency
                // of another command, so simply save `dot` as a child
                dots.get_mut().insert(dot);
            }
        }
        None
    }

    /// Finds all pending dots for a given dependency dot.
    pub fn remove(&mut self, dep_dot: &Dot) -> Option<HashSet<Dot>> {
        self.index.remove(dep_dot)
    }
}
