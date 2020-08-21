use super::{Dependency, KeyDeps};
use fantoch::command::Command;
use fantoch::id::{Dot, ShardId};
use fantoch::kvs::Key;
use fantoch::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequentialKeyDeps {
    shard_id: ShardId,
    latest_deps: HashMap<Key, Dependency>,
    noop_latest_dep: Option<Dependency>,
}

impl KeyDeps for SequentialKeyDeps {
    /// Create a new `SequentialKeyDeps` instance.
    fn new(shard_id: ShardId) -> Self {
        Self {
            shard_id,
            latest_deps: HashMap::new(),
            noop_latest_dep: None,
        }
    }

    fn add_cmd(
        &mut self,
        dot: Dot,
        cmd: &Command,
        past: Option<HashSet<Dependency>>,
    ) -> HashSet<Dependency> {
        // we start with past in case there's one, or bottom otherwise
        let deps = match past {
            Some(past) => past,
            None => HashSet::new(),
        };
        self.do_add_cmd(dot, cmd, deps)
    }

    fn add_noop(&mut self, dot: Dot) -> HashSet<Dependency> {
        // start with an empty set of dependencies
        let deps = HashSet::new();
        self.do_add_noop(dot, deps)
    }

    #[cfg(test)]
    fn cmd_deps(&self, cmd: &Command) -> HashSet<Dot> {
        let mut deps = HashSet::new();
        self.maybe_add_noop_latest(&mut deps);
        self.do_cmd_deps(cmd, &mut deps);
        super::extract_dots(deps)
    }

    #[cfg(test)]
    fn noop_deps(&self) -> HashSet<Dot> {
        let mut deps = HashSet::new();
        self.maybe_add_noop_latest(&mut deps);
        self.do_noop_deps(&mut deps);
        super::extract_dots(deps)
    }

    fn parallel() -> bool {
        false
    }
}

impl SequentialKeyDeps {
    fn maybe_add_noop_latest(&self, deps: &mut HashSet<Dependency>) {
        if let Some(dep) = self.noop_latest_dep.as_ref() {
            deps.insert(dep.clone());
        }
    }

    fn do_add_cmd(
        &mut self,
        dot: Dot,
        cmd: &Command,
        mut deps: HashSet<Dependency>,
    ) -> HashSet<Dependency> {
        // iterate through all command keys, get their current latest and set
        // ourselves to be the new latest
        let new_dep = Dependency::from_cmd(dot, cmd);
        cmd.keys(self.shard_id).for_each(|key| {
            // get latest command on this key
            match self.latest_deps.get_mut(key) {
                Some(dep) => {
                    // if there was a previous latest, then it's a dependency
                    deps.insert(dep.clone());
                    // set self to be the new latest
                    *dep = new_dep.clone();
                }
                None => {
                    // set self to be the new latest
                    self.latest_deps
                        .entry(key.clone())
                        .or_insert(new_dep.clone());
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
        mut deps: HashSet<Dependency>,
    ) -> HashSet<Dependency> {
        // set self to be the new latest
        if let Some(dep) =
            self.noop_latest_dep.replace(Dependency::from_noop(dot))
        {
            // if there was a previous latest, then it's a dependency
            deps.insert(dep);
        }

        // compute deps for this noop
        self.do_noop_deps(&mut deps);

        deps
    }

    fn do_noop_deps(&self, deps: &mut HashSet<Dependency>) {
        // iterate through all keys, grab a read lock, and include their latest
        // in the final `deps`
        self.latest_deps.values().for_each(|dep| {
            // take the dot as a dependency
            deps.insert(dep.clone());
        });
    }

    #[cfg(test)]
    fn do_cmd_deps(&self, cmd: &Command, deps: &mut HashSet<Dependency>) {
        cmd.keys(self.shard_id).for_each(|key| {
            // get latest command on this key
            if let Some(dep) = self.latest_deps.get(key) {
                // if there is a latest, then it's a dependency
                deps.insert(dep.clone());
            }
        });
    }
}
