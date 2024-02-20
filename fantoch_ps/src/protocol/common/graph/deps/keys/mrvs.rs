
//TODO: Is not implemented
use fantoch::id::{Dot, ShardId};


#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultiRecordValues {
    shard_id: ShardId,
    nfr: bool,
    //more ....
}


impl KeyDeps for MultiRecordValuesKeyValues {
    
    /// Create a new `MultiRecordValuesKeyValues` instance.
    fn new(shard_id: ShardId, nfr: bool) -> Self {
        Self {
            shard_id,
            nfr,
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
        //
    }

    fn cmd_deps(&self, cmd: &Command) -> HashSet<Dot> {
        //
    }

    fn noop_deps(&self) -> HashSet<Dot> {
        //
    }

    fn parallel() -> bool {
        false
    }
}

impl MultiRecordValuesKeyValues {

    // Almost equal at super::maybe_add_deps
    fn maybe_add_deps(
        read_only: bool,
        nfr: bool,
        latest_rw: &LatestRWDep,
        deps: &mut HashSet<Dependency>,
    ) {
        //FIXME: se for read -> basta ir ao write
        //FIXME: se for write -> tem deps em todos
        // independently of whether the command is read-only or not, all commands
        // depend on writes
        if let Some(wdep) = latest_rw.write.as_ref() {
            deps.insert(wdep.clone());
        }
    
        // if the command is not read-only, and the NFR optimization is not enabled,
        // then the command should also depend on the latest read;
        // in other words:
        //  ----------------------------------
        // | read_only | NFR   | add read dep |
        //  ----------------------------------
        // | true      | _     | NO           |
        // | false     | true  | NO           |
        // | false     | false | YES          |
        //  ----------------------------------
        if !read_only && !nfr {
            if let Some(rdep) = latest_rw.read.as_ref() {
                deps.insert(rdep.clone());
            }
        }
        // in sum:
        // - reads never depend on reads, and
        // - writes always depend on reads (unless NFR is enabled, in which case,
        //   they don't)
    }

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
        // we only support single-key read commands with NFR
        assert!(if self.nfr && read_only {
            cmd.total_key_count() == 1
        } else {
            true
        });

        // iterate through all command keys, get their current latest and set
        // ourselves to be the new latest
        cmd.keys(self.shard_id).for_each(|key| {
            // get latest read and write on this key
            let latest_rw = match self.latest.get_mut(key) {
                Some(value) => value,
                None => self.latest.entry(key.clone()).or_default(),
            };

            maybe_add_deps(read_only, self.nfr, latest_rw, &mut deps);

            // finally, store the command
            if read_only {
                // if a command is read-only, then added it as the latest read
                latest_rw.read = Some(cmd_dep.clone());
            } else {
                // otherwise, add it as the latest write
                latest_rw.write = Some(cmd_dep.clone());
            }
        });

        // always include latest noop, if any
        self.maybe_add_noop_latest(&mut deps);

        // and finally return the computed deps
        deps
    }
}