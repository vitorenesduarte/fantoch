use super::tarjan::Vertex;
use crate::shared::Shared;
use dashmap::mapref::one::Ref as DashMapRef;
use fantoch::id::{Dot, ProcessId};
use fantoch::log;
use fantoch::time::SysTime;
use fantoch::HashSet;
use parking_lot::{Mutex, MutexGuard, RwLock};
use std::collections::VecDeque;
use std::sync::Arc;
use threshold::AEClock;

// epoch length is 500ms
const EPOCH_MILLIS: u64 = 500;
// commands may be GCed after 1 second
const EPOCH_CLEANUP_AGE: u64 = 2;

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
    remote: Arc<Shared<Dot, Mutex<Vertex>>>,
    // local to worker 1:
    // - current epoch number
    // - list of remote dots that were indexed in the current epoch
    // - list of pairs (epoch, remote dots) to be cleaned up when their age
    //   expires
    // - list of dots that tried to be cleaned up but couldn't as there were
    //   pending commands that depend on them
    current_epoch: Option<u64>,
    remote_indexed_current_epoch: Vec<Dot>,
    to_cleanup: VecDeque<(u64, Vec<Dot>)>,
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
            to_cleanup: VecDeque::new(),
            failed_cleanup: Vec::new(),
        }
    }

    /// Indexes a new vertex, returning true if it was already indexed.
    pub fn index(
        &mut self,
        executor_index: usize,
        vertex: Vertex,
        is_mine: bool,
        pending_index: &PendingIndex,
        time: &dyn SysTime,
    ) -> bool {
        let dot = vertex.dot();
        let cell = Mutex::new(vertex);
        if is_mine {
            self.local.insert(dot, cell).is_some()
        } else {
            assert_eq!(executor_index, 1);
            self.maybe_update_epoch(executor_index, time);
            self.remote_indexed_current_epoch.push(dot);
            let res = self.remote.insert(dot, cell).is_some();
            // at this point, the dot is in the remote index; this means that if
            // worker 0 tries to consult the remote index it will see this dot;
            // however, there may be existing pending commands that had this dot
            // as a dependency. for that reason, we remove them from the pending
            // index and add them to a `to_try` list that will be consulted by
            // worker 0 every time a new non-remote command is added.
            if let Some(dots) = pending_index.remove(&dot) {
                pending_index.add_to_try(dots);
            }
            res
        }
    }

    fn maybe_update_epoch(
        &mut self,
        executor_index: usize,
        time: &dyn SysTime,
    ) {
        assert_eq!(executor_index, 1);
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
                    self.to_cleanup.push_back((
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
        executor_index: usize,
        pending_index: &PendingIndex,
        executed_clock: &Arc<RwLock<AEClock<ProcessId>>>,
        time: &dyn SysTime,
    ) {
        assert_eq!(executor_index, 1);
        // cleanup previous cleanups that failed
        let failed = std::mem::take(&mut self.failed_cleanup);
        self.do_cleanup(executor_index, failed, pending_index, executed_clock);

        let now = time.millis() / EPOCH_MILLIS;
        if let Some((epoch, _)) = self.to_cleanup.get(0) {
            // compute age of this epoch
            let epoch_age = now - epoch;
            log!(
                "p{}: VertexIndex::maybe_cleanup now {} | epoch {} | age {}",
                self.process_id,
                now,
                epoch,
                epoch_age
            );
            // if epoch is age is higher than the cleanup age, then try to clean
            // it up
            if epoch_age >= EPOCH_CLEANUP_AGE {
                // get dots to be cleaned up
                let (_, dots) = self
                    .to_cleanup
                    .pop_front()
                    .expect("there should be a front to cleanup");

                // try to cleanup
                self.do_cleanup(
                    executor_index,
                    dots,
                    pending_index,
                    executed_clock,
                );
            }
        }
    }

    fn do_cleanup(
        &mut self,
        executor_index: usize,
        dots: Vec<Dot>,
        pending_index: &PendingIndex,
        executed_clock: &Arc<RwLock<AEClock<ProcessId>>>,
    ) {
        assert_eq!(executor_index, 1);
        log!("p{}: VertexIndex::do_cleanup {:?}", self.process_id, dots);
        for dot in dots {
            // cleanup previous cleanups that failed
            if pending_index.contains(&dot) {
                log!("p{}: VertexIndex::do_cleanup cleanup failed for {:?} as there are pending commands", self.process_id, dot);
                // if there are commands waiting on this command, do not cleanup
                self.failed_cleanup.push(dot);
            } else {
                if let Some(vertex) = self.remote.get(&dot) {
                    // if the dot exists as remote, check that it's not being
                    // used by tarjan's finder
                    let vertex = vertex.lock();
                    if vertex.id == 0 {
                        // in this case, it's not being used and thus it is safe
                        // to remove it
                        self.remote.remove(&dot);
                        assert!(executed_clock
                            .write()
                            .add(&dot.source(), dot.sequence()));
                    } else {
                        log!("p{}: VertexIndex::do_cleanup cleanup failed for {:?} as it's being used in finder", self.process_id, dot);
                        self.failed_cleanup.push(dot);
                    }
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
    to_try: Arc<RwLock<HashSet<Dot>>>,
}

impl PendingIndex {
    pub fn new() -> Self {
        Self {
            index: Arc::new(Shared::new()),
            to_try: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Indexes a new `dot` as a child of `dep_dot`:
    /// - when `dep_dot` is executed, we'll try to execute `dot` as `dep_dot`
    ///   was a dependency and maybe now `dot` can be executed
    pub fn index(&self, dep_dot: Dot, dot: Dot) {
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
    pub fn remove(&self, dep_dot: &Dot) -> Option<HashSet<Dot>> {
        self.index
            .remove(dep_dot)
            .map(|(_, dots)| dots.into_inner())
    }

    pub fn get_to_try(&self) -> HashSet<Dot> {
        std::mem::take(&mut self.to_try.write())
    }

    fn add_to_try(&self, dots: HashSet<Dot>) {
        self.to_try.write().extend(dots);
    }
}
