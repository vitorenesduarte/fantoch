use super::tarjan::Vertex;
use crate::shared::Shared;
use dashmap::mapref::one::Ref as DashMapRef;
use fantoch::config::Config;
use fantoch::hash_map::{Entry, HashMap};
use fantoch::id::{Dot, ProcessId, ShardId};
use fantoch::HashSet;
use parking_lot::{Mutex, MutexGuard};
use std::sync::Arc;
use threshold::AEClock;

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
    process_id: ProcessId,
    shard_id: ShardId,
    config: Config,
    index: HashMap<Dot, HashSet<Dot>>,
    // `committed_clock` and `mine` are only used in partial replication
    committed_clock: AEClock<ProcessId>,
    mine: HashSet<Dot>,
}

impl PendingIndex {
    pub fn new(
        process_id: ProcessId,
        shard_id: ShardId,
        config: Config,
        committed_clock: AEClock<ProcessId>,
    ) -> Self {
        Self {
            process_id,
            shard_id,
            config,
            index: HashMap::new(),
            committed_clock,
            mine: HashSet::new(),
        }
    }

    pub fn add_mine(&mut self, dot: Dot) {
        assert!(self.mine.insert(dot));
    }

    pub fn is_mine(&mut self, dot: &Dot) -> bool {
        self.mine.contains(dot)
    }

    pub fn committed(&mut self, dot: Dot) {
        // update set of committed commands if partial replication
        if self.config.shards() > 1 {
            if !self.committed_clock.add(&dot.source(), dot.sequence()) {
                panic!(
                    "p{}: PendingIndex::committed {:?} already committed",
                    self.process_id, dot
                );
            }
        }
    }

    /// Indexes a new `dot` as a child of `dep_dot`:
    /// - when `dep_dot` is executed, we'll try to execute `dot` as `dep_dot`
    ///   was a dependency and maybe now `dot` can be executed
    #[must_use]
    pub fn index(
        &mut self,
        dep_dot: Dot,
        dot: Dot,
    ) -> Option<(ShardId, HashSet<Dot>)> {
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
                    // if we don't replicate the command, ask its target shard
                    // for its info
                    if let Some(event_set) =
                        self.committed_clock.get(&dep_dot.source())
                    {
                        let dots_to_request: HashSet<_> = event_set
                            .missing_below(dep_dot.sequence())
                            .map(|event| Dot::new(dep_dot.source(), event))
                            // don't ask for dots we have already asked for
                            .filter(|dot| !self.index.contains_key(dot))
                            // missing below doesn't include
                            // `dep_dot.sequence()`; thus, include `dep_dot` as
                            // well
                            .chain(std::iter::once(dep_dot))
                            .collect();

                        // make `dot` a child of all these dots to be requested;
                        // this makes sure that we don't request the same dot
                        // twice
                        for dot_to_request in dots_to_request.iter() {
                            self.index
                                .entry(*dot_to_request)
                                .or_default()
                                .insert(dot);
                        }
                        return Some((target, dots_to_request));
                    } else {
                        panic!(
                            "p{}: PendingIndex::index {:?} not found in committed clock {:?}", 
                            self.process_id,
                            dep_dot.source(),
                            self.committed_clock,
                        );
                    }
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
