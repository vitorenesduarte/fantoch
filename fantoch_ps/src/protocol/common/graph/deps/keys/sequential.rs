use super::KeyDeps;
use fantoch::command::Command;
use fantoch::id::{Dot, ShardId};
use fantoch::kvs::Key;
use fantoch::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequentialKeyDeps {
    shard_id: ShardId,
    latest_dots: HashMap<Key, Dot>,
    noop_latest_dot: Option<Dot>,
}

impl KeyDeps for SequentialKeyDeps {
    /// Create a new `SequentialKeyDeps` instance.
    fn new(shard_id: ShardId) -> Self {
        Self {
            shard_id,
            latest_dots: HashMap::new(),
            noop_latest_dot: None,
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
        false
    }
}

impl SequentialKeyDeps {
    fn maybe_add_noop_latest(&self, deps: &mut HashSet<Dot>) {
        if let Some(dot) = self.noop_latest_dot {
            deps.insert(dot);
        }
    }

    fn do_add_cmd(
        &mut self,
        dot: Dot,
        cmd: &Command,
        mut deps: HashSet<Dot>,
    ) -> HashSet<Dot> {
        // iterate through all command keys, get their current latest and set
        // ourselves to be the new latest
        cmd.keys(self.shard_id).for_each(|key| {
            // get latest command on this key
            match self.latest_dots.get_mut(key) {
                Some(dep) => {
                    // if there was a previous latest, then it's a dependency
                    deps.insert(*dep);
                    // set self to be the new latest
                    *dep = dot;
                }
                None => {
                    // set self to be the new latest
                    self.latest_dots.entry(key.clone()).or_insert(dot);
                }
            };
        });

        // include latest noop, if any
        self.maybe_add_noop_latest(&mut deps);

        // and finally return the computed deps
        deps
    }

    fn do_add_noop(
        &mut self,
        dot: Dot,
        mut deps: HashSet<Dot>,
    ) -> HashSet<Dot> {
        // set self to be the new latest
        if let Some(dep) = self.noop_latest_dot.replace(dot) {
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
        self.latest_dots.values().for_each(|dep| {
            // take the dot as a dependency
            deps.insert(*dep);
        });
    }

    #[cfg(test)]
    fn do_cmd_deps(&self, cmd: &Command, deps: &mut HashSet<Dot>) {
        cmd.keys(self.shard_id).for_each(|key| {
            // get latest command on this key
            if let Some(dep) = self.latest_dots.get(key) {
                // if there is a latest, then it's a dependency
                deps.insert(*dep);
            }
        });
    }
}
