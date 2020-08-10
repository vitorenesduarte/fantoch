use super::tarjan::Vertex;
use crate::shared::Shared;
use dashmap::mapref::one::Ref as DashMapRef;
use fantoch::id::{Dot, ProcessId};
use fantoch::log;
use fantoch::time::SysTime;
use fantoch::HashSet;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::collections::VecDeque;
use std::sync::Arc;
use threshold::AEClock;

// epoch length is 1 second
const EPOCH_MILLIS: u64 = 1000;
const EPOCH_CLEANUP_AGE: u64 = 10;

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
    current_epoch: Option<u64>,
    // list of remote dots indexed in the current epoch
    remote_indexed_current_epoch: Vec<Dot>,
    // list of pairs (epoch, remote dots) to be cleaned up
    to_cleanup: Arc<RwLock<VecDeque<(u64, Vec<Dot>)>>>,
    // list of dots which cleanup has failed
    failed_cleanup: Vec<Dot>,
}

impl VertexIndex {
    pub fn new(process_id: ProcessId) -> Self {
        Self {
            process_id,
            local: Arc::new(Shared::new()),
            remote: Arc::new(Shared::new()),
            current_epoch: None,
            remote_indexed_current_epoch: Vec::new(),
            to_cleanup: Arc::new(RwLock::new(VecDeque::new())),
            failed_cleanup: Vec::new(),
        }
    }

    /// Indexes a new vertex, returning true if it was already indexed.
    pub fn index(
        &mut self,
        vertex: Vertex,
        is_mine: bool,
        time: &dyn SysTime,
    ) -> bool {
        let dot = vertex.dot();
        let cell = RwLock::new(vertex);
        if is_mine {
            self.local.insert(dot, cell).is_some()
        } else {
            self.maybe_update_epoch(time);
            self.remote_indexed_current_epoch.push(dot);
            self.remote.insert(dot, cell).is_some()
        }
    }

    fn maybe_update_epoch(&mut self, time: &dyn SysTime) {
        let now = time.millis() / EPOCH_MILLIS;
        match self.current_epoch {
            Some(current_epoch) => {
                // check if should update epoch
                if now > current_epoch {
                    log!(
                        "p{}: VertexIndex::maybe_update_epoch next epoch: {} | {:?}",
                        self.process_id,
                        now,
                        self.remote_indexed_current_epoch.iter().collect::<std::collections::BTreeSet<_>>()
                    );
                    // update epoch
                    self.current_epoch = Some(now);
                    // move `self.remote_index_current_epoch` to
                    // `self.to_cleanup`
                    self.to_cleanup.write().push_back((
                        current_epoch,
                        std::mem::take(&mut self.remote_indexed_current_epoch),
                    ));
                }
            }
            None => {
                log!(
                    "p{}: VertexIndex::maybe_update_epoch first epoch: {}",
                    self.process_id,
                    now
                );
                self.current_epoch = Some(now);
            }
        }
    }

    pub fn maybe_cleanup(
        &mut self,
        pending: &PendingIndex,
        executed_clock: &Arc<RwLock<AEClock<ProcessId>>>,
        time: &dyn SysTime,
    ) {
        // cleanup previous cleanups that failed
        let failed = std::mem::take(&mut self.failed_cleanup);
        self.do_cleanup(failed, pending, executed_clock);

        let now = time.millis() / EPOCH_MILLIS;
        // 1 - 10 > 3

        let to_cleanup = self.to_cleanup.read();
        if let Some((epoch, _)) = to_cleanup.get(0) {
            let epoch_age = now - epoch;
            log!(
                "p{}: VertexIndex::maybe_cleanup now {} | epoch {} | age {}",
                self.process_id,
                now,
                epoch,
                epoch_age
            );
            if epoch_age >= EPOCH_CLEANUP_AGE {
                // drop current read guard and get a write guard
                drop(to_cleanup);
                let mut to_cleanup = self.to_cleanup.write();

                // get dots to be cleaned up
                let (_, dots) = to_cleanup
                    .pop_front()
                    .expect("there should be a front to cleanup");

                // drop write guard
                drop(to_cleanup);

                // try to cleanup
                self.do_cleanup(dots, pending, executed_clock);
            }
        }
    }

    fn do_cleanup(
        &mut self,
        dots: Vec<Dot>,
        pending: &PendingIndex,
        executed_clock: &Arc<RwLock<AEClock<ProcessId>>>,
    ) {
        for dot in dots {
            if pending.contains(&dot) {
                // if there are commands waiting on this command, do not cleanup
                self.failed_cleanup.push(dot);
            } else {
                if self.remote.remove(&dot).is_some() {
                    assert!(executed_clock
                        .write()
                        .add(&dot.source(), dot.sequence()));
                }
            }
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

#[derive(Debug, Clone)]
pub struct PendingIndex {
    index: Arc<Shared<Dot, RwLock<HashSet<Dot>>>>,
}

impl PendingIndex {
    pub fn new() -> Self {
        Self {
            index: Arc::new(Shared::new()),
        }
    }

    /// Indexes a new `dot` as a child of `dep_dot`:
    /// - when `dep_dot` is executed, we'll try to execute `dot` as `dep_dot`
    ///   was a dependency and maybe now `dot` can be executed
    pub fn index(&mut self, dep_dot: Dot, dot: Dot) {
        // get current list of pending dots and add new `dot` to pending
        self.index
            .get_or(&dep_dot, || RwLock::new(HashSet::new()))
            .write()
            .insert(dot);
    }

    fn contains(&self, dot: &Dot) -> bool {
        self.index.contains_key(dot)
    }

    /// Finds all pending dots for a given dependency dot.
    pub fn remove(&mut self, dep_dot: &Dot) -> Option<HashSet<Dot>> {
        self.index
            .remove(dep_dot)
            .map(|(_, dots)| dots.into_inner())
    }
}
