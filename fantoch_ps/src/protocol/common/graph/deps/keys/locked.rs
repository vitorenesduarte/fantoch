use super::KeyDeps;
use crate::shared::Shared;
use fantoch::command::Command;
use fantoch::id::{Dot, ShardId};
use fantoch::kvs::Key;
use fantoch::HashSet;
use parking_lot::RwLock;
use std::sync::Arc;

type Latest = RwLock<Option<Dot>>;

#[derive(Debug, Clone)]
pub struct LockedKeyDeps {
    shard_id: ShardId,
    latest_dots: Arc<Shared<Key, Latest>>,
    noop_latest_dot: Arc<Latest>,
}

impl KeyDeps for LockedKeyDeps {
    /// Create a new `LockedKeyDeps` instance.
    fn new(shard_id: ShardId) -> Self {
        Self {
            shard_id,
            latest_dots: Arc::new(Shared::new()),
            noop_latest_dot: Arc::new(RwLock::new(None)),
        }
    }

    fn add_cmd(
        &mut self,
        dot: Dot,
        cmd: &Command,
        past: Option<HashSet<Dot>>,
    ) -> HashSet<Dot> {
        // we start with past in case there's one, or bottom otherwise
        let deps = match past {
            Some(past) => past,
            None => HashSet::new(),
        };
        self.do_add_cmd(dot, cmd, deps)
    }

    fn add_noop(&mut self, dot: Dot) -> HashSet<Dot> {
        // start with an empty set of dependencies
        let deps = HashSet::new();
        self.do_add_noop(dot, deps)
    }

    #[cfg(test)]
    fn cmd_deps(&self, cmd: &Command) -> HashSet<Dot> {
        let mut deps = HashSet::new();
        self.maybe_add_noop_latest(&mut deps);
        self.do_cmd_deps(cmd, &mut deps);
        deps
    }

    #[cfg(test)]
    fn noop_deps(&self) -> HashSet<Dot> {
        let mut deps = HashSet::new();
        self.maybe_add_noop_latest(&mut deps);
        self.do_noop_deps(&mut deps);
        deps
    }

    fn parallel() -> bool {
        true
    }
}

impl LockedKeyDeps {
    fn maybe_add_noop_latest(&self, deps: &mut HashSet<Dot>) {
        // for this operation we only need a read lock
        if let Some(dot) = *self.noop_latest_dot.read() {
            deps.insert(dot);
        }
    }

    fn do_add_cmd(
        &self,
        dot: Dot,
        cmd: &Command,
        mut deps: HashSet<Dot>,
    ) -> HashSet<Dot> {
        // iterate through all command keys, grab a write lock, get their
        // current latest and set ourselves to be the new latest
        cmd.keys(self.shard_id).for_each(|key| {
            // get latest command on this key
            let entry = self.latest_dots.get_or(key, || RwLock::default());
            // grab a write lock
            let mut guard = entry.write();
            // set self to be the new latest
            if let Some(dep) = guard.replace(dot) {
                // if there was a previous latest, then it's a dependency
                deps.insert(dep);
            }
        });

        // include latest noop, if any
        // TODO: when adding recovery, check that the interleaving of the
        // following and the previous loop, and how it interacts with
        // `do_add_noop` is correct
        self.maybe_add_noop_latest(&mut deps);

        // and finally return the computed deps
        deps
    }

    fn do_add_noop(&self, dot: Dot, mut deps: HashSet<Dot>) -> HashSet<Dot> {
        // grab a write lock to the noop latest and:
        // - add ourselves to the deps:
        //   * during the next iteration a new key in the map might be created
        //     and we may miss it
        //   * by first setting ourselves to be the noop latest we make sure
        //     that, even though we will not see that newly created key, that
        //     key will see us
        // grab a write lock and set self to be the new latest
        if let Some(dep) = self.noop_latest_dot.write().replace(dot) {
            // if there was a previous latest, then it's a dependency
            deps.insert(dep);
        }

        // compute deps for this noop
        self.do_noop_deps(&mut deps);

        deps
    }

    fn do_noop_deps(&self, deps: &mut HashSet<Dot>) {
        // iterate through all keys, grab a read lock, and include their latest
        // in the final `deps`
        self.latest_dots.iter().for_each(|entry| {
            // grab a read lock and take the dot there as a dependency
            if let Some(dep) = *entry.value().read() {
                deps.insert(dep);
            }
        });
    }

    #[cfg(test)]
    fn do_cmd_deps(&self, cmd: &Command, deps: &mut HashSet<Dot>) {
        cmd.keys(self.shard_id).for_each(|key| {
            // get latest command on this key
            let entry = self.latest_dots.get_or(key, || RwLock::default());
            // grab a read lock
            let guard = entry.read();
            // take the dot there as a dependency
            if let Some(dep) = *guard {
                deps.insert(dep);
            }
        });
    }
}
