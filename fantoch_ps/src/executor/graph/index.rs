use super::tarjan::Vertex;
use crate::protocol::common::graph::Dependency;
use fantoch::config::Config;
use fantoch::hash_map::{Entry, HashMap};
use fantoch::id::{Dot, ProcessId, ShardId};
use fantoch::info;
use fantoch::shared::{SharedMap, SharedMapRef};
use fantoch::time::SysTime;
use fantoch::HashSet;
use parking_lot::{RwLock, RwLockReadGuard};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use threshold::AEClock;

pub type VertexRef<'a> = SharedMapRef<'a, Dot, RwLock<Vertex>>;

#[derive(Debug, Clone)]
pub struct VertexIndex {
    process_id: ProcessId,
    index: Arc<SharedMap<Dot, RwLock<Vertex>>>,
}

impl VertexIndex {
    pub fn new(process_id: ProcessId) -> Self {
        Self {
            process_id,
            index: Arc::new(SharedMap::new()),
        }
    }

    /// Indexes a new vertex, returning any previous vertex indexed.
    pub fn index(&mut self, vertex: Vertex) -> Option<Vertex> {
        let dot = vertex.dot;
        let cell = RwLock::new(vertex);
        self.index.insert(dot, cell).map(|cell| cell.into_inner())
    }

    #[allow(dead_code)]
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

    pub fn monitor_pending(
        &self,
        executed_clock: &AEClock<ProcessId>,
        monitor_pending_threshold: Duration,
        time: &dyn SysTime,
    ) {
        // collect pending commands
        let now_ms = time.millis();
        let threshold_ms = monitor_pending_threshold.as_millis() as u64;
        let mut pending = BTreeMap::new();
        let mut pending_without_missing_deps = HashSet::new();

        self.index.iter().for_each(|vertex_ref| {
            // check if we should show this pending command
            let vertex = vertex_ref.read();
            let pending_for_ms = now_ms - vertex.start_time_ms;

            if pending_for_ms >= threshold_ms {
                // compute missing dependencies
                let mut visited = HashSet::new();
                let missing_deps = self.missing_dependencies(&vertex, executed_clock, &mut visited);

                if missing_deps.is_empty() {
                    pending_without_missing_deps.insert(vertex.dot);
                }

                pending.entry(pending_for_ms).or_insert_with(Vec::new).push(
                    format!(
                        "p{}: {:?} is pending for {:?}ms with deps {:?} | missing {:?}",
                        self.process_id,
                        vertex.dot,
                        pending_for_ms,
                        vertex.deps,
                        missing_deps,
                    ),
                )
            }
        });

        // show pending commands: pending longest first
        for (_pending_for_ms, pending) in pending.into_iter().rev() {
            for fmt in pending {
                info!("{}", fmt);
            }
        }

        // panic if there's a pending command without missing dependencies
        if !pending_without_missing_deps.is_empty() {
            panic!("p{}: the following commands are pending without missing dependencies: {:?}", self.process_id, pending_without_missing_deps);
        }
    }

    fn missing_dependencies(
        &self,
        vertex: &RwLockReadGuard<'_, Vertex>,
        executed_clock: &AEClock<ProcessId>,
        visited: &mut HashSet<Dot>,
    ) -> HashSet<Dot> {
        let mut missing_dependencies = HashSet::new();

        // add self to the set of visited pending commands
        if !visited.insert(vertex.dot) {
            // if already visited, return
            return missing_dependencies;
        }

        for dep in &vertex.deps {
            let dep_dot = dep.dot;
            if executed_clock.contains(&dep_dot.source(), dep_dot.sequence()) {
                // ignore executed dep
                continue;
            }

            // if it's not executed, check if it's also pending
            if let Some(dep_vertex_ref) = self.index.get(&dep_dot) {
                let dep_vertex = dep_vertex_ref.read();
                // if it is, then we're pending for the same reason our
                // dependency is pending
                missing_dependencies.extend(self.missing_dependencies(
                    &dep_vertex,
                    executed_clock,
                    visited,
                ));
            } else {
                // if it's not pending, then there's a missing dependency
                missing_dependencies.insert(dep_dot);
            }
        }
        missing_dependencies
    }
}

#[derive(Debug, Clone)]
pub struct PendingIndex {
    shard_id: ShardId,
    config: Config,
    index: HashMap<Dot, HashSet<Dot>>,
}

impl PendingIndex {
    pub fn new(
        shard_id: ShardId,
        config: Config,
    ) -> Self {
        Self {
            shard_id,
            config,
            index: HashMap::new(),
        }
    }

    /// Indexes a new `dot` as a child of `parent`:
    /// - when `parent.dot` is executed, we'll try to execute `dot` as
    ///   `parent.dot` was a dependency and maybe now `dot` can be executed
    #[must_use]
    pub fn index(
        &mut self,
        parent: &Dependency,
        dot: Dot,
    ) -> Option<(Dot, ShardId)> {
        match self.index.entry(parent.dot) {
            Entry::Vacant(vacant) => {
                // save `dot` as a child
                let mut children = HashSet::new();
                children.insert(dot);
                // create `parent` entry
                vacant.insert(children);

                // this is the first time we detect `parent.dot` as a missing
                // dependency; in this case, we may have to ask another
                // shard for its info if we don't replicate it; we can know if
                // we replicate it by inspecting `parent.shards`
                let is_mine = parent
                    .shards
                    .as_ref()
                    .expect("shards should be set if it's not a noop")
                    .contains(&self.shard_id);
                if !is_mine {
                    let target = parent.dot.target_shard(self.config.n());
                    return Some((parent.dot, target));
                }
            }
            Entry::Occupied(mut children) => {
                // in this case, `parent` has already been a missing dependency
                // of another command, so simply save `dot` as a child
                children.get_mut().insert(dot);
            }
        }
        None
    }

    /// Finds all pending dots for a given dependency dot.
    pub fn remove(&mut self, dep_dot: &Dot) -> Option<HashSet<Dot>> {
        self.index.remove(dep_dot)
    }
}
