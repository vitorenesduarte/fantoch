use super::{Dependency, KeyDeps, LatestDep, LatestRWDep};
use fantoch::command::Command;
use fantoch::id::{Dot, ShardId};
use fantoch::kvs::Key;
use fantoch::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequentialKeyDeps {
    shard_id: ShardId,
    deps_nfr: bool,
    latest: HashMap<Key, LatestRWDep>,
    latest_noop: LatestDep,
}

impl KeyDeps for SequentialKeyDeps {
    /// Create a new `SequentialKeyDeps` instance.
    fn new(shard_id: ShardId, deps_nfr: bool) -> Self {
        Self {
            shard_id,
            deps_nfr,
            latest: HashMap::new(),
            latest_noop: None,
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
        if let Some(dep) = self.latest_noop.as_ref() {
            deps.insert(dep.clone());
        }
    }

    fn do_add_cmd(
        &mut self,
        dot: Dot,
        cmd: &Command,
        mut deps: HashSet<Dependency>,
    ) -> HashSet<Dependency> {
        // create cmd dep
        let cmd_dep = Dependency::from_cmd(dot, cmd);

        // flag indicating whether the command is read-only
        let read_only = cmd.read_only();

        // iterate through all command keys, get their current latest and set
        // ourselves to be the new latest
        cmd.keys(self.shard_id).for_each(|key| {
            // get latest read and write on this key
            let latest_rw = match self.latest.get_mut(key) {
                Some(value) => value,
                None => self.latest.entry(key.clone()).or_default(),
            };

            super::maybe_add_deps(
                read_only,
                self.deps_nfr,
                latest_rw,
                &mut deps,
            );

            // finally, store the command
            if read_only {
                // if a command is read-only, then added it as the latest read
                latest_rw.read = Some(cmd_dep.clone());
            } else {
                // otherwise, add it as the latest write
                // if a command is not read-only, then it should depend on the
                // latest read and latest write, and it should be added as the
                // latest write
                latest_rw.write = Some(cmd_dep.clone());
            }
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
        if let Some(dep) = self.latest_noop.replace(Dependency::from_noop(dot))
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
        self.latest.values().for_each(|latest_rw| {
            if let Some(rdep) = latest_rw.read.as_ref() {
                deps.insert(rdep.clone());
            }
            if let Some(wdep) = latest_rw.write.as_ref() {
                deps.insert(wdep.clone());
            }
        });
    }

    #[cfg(test)]
    fn do_cmd_deps(&self, cmd: &Command, deps: &mut HashSet<Dependency>) {
        // flag indicating whether the command is read-only
        let read_only = cmd.read_only();

        cmd.keys(self.shard_id).for_each(|key| {
            // get latest command on this key
            if let Some(latest_rw) = self.latest.get(key) {
                super::maybe_add_deps(
                    read_only,
                    self.deps_nfr,
                    latest_rw,
                    deps,
                );
            }
        });
    }
}
