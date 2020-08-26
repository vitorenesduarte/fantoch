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
use std::collections::BTreeMap;
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
        show_pending_threshold: Duration,
        time: &dyn SysTime,
    ) {
        // first show executed clock
        println!(
            "p{}: executed frontier before showing pending {:?}",
            self.process_id,
            executed_clock.frontier()
        );

        // collect pending commands
        let now_ms = time.millis();
        let show_pending_threshold_ms =
            show_pending_threshold.as_millis() as u64;
        let mut pending = BTreeMap::new();
        self.index.iter().for_each(|vertex_ref| {
            let vertex = vertex_ref.read();
            let pending_for_ms = now_ms - vertex.start_time_ms;
            if pending_for_ms >= show_pending_threshold_ms {
                pending.entry(pending_for_ms).or_insert_with(Vec::new).push(
                    format!(
                        "p{}: {:?} is pending for {:?}ms with deps {:?}",
                        self.process_id,
                        vertex.dot,
                        pending_for_ms,
                        vertex.deps
                    ),
                )
            }
        });

        // show pending commands: pending longest first
        for (_pending_for_ms, pending) in pending.into_iter().rev() {
            for fmt in pending {
                println!("{}", fmt);
            }
        }
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
