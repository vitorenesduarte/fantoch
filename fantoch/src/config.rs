use crate::id::ProcessId;

#[derive(Debug, Clone, Copy)]
pub struct Config {
    /// number of processes
    n: usize,
    /// number of tolerated faults
    f: usize,
    /// defines whether newt should employ tiny quorums or not
    newt_tiny_quorums: bool,
    /// defines whether we can assume if the conflict relation is transitive
    transitive_conflicts: bool,
    /// if enabled, then execution is skipped
    execute_at_commit: bool,
    // starting leader process
    leader: Option<ProcessId>,
    /// defines the number of `Protocol` workers
    workers: usize,
    /// defines the number of `Executor` workers
    executors: usize,
}

impl Config {
    /// Create a new `Config`.
    /// The first argument `n` represents the number of processes in the system.
    /// The second argument `f` represents the number of faults tolerated by the
    /// system.
    pub fn new(n: usize, f: usize) -> Self {
        if f > n / 2 {
            println!("WARNING: f={} is larger than a minority with n={}", f, n);
        }
        // by default, `newt_tiny_quorums = false`
        let newt_tiny_quorums = false;
        // by default, `transitive_conflicts = false`
        let transitive_conflicts = false;
        // by default, execution is not skipped
        let execute_at_commit = false;
        // by default, there's no leader
        let leader = None;
        // by default there's one worker for `Protocol` and one worker for
        // `Executor`
        let workers = 1;
        let executors = 1;
        Self {
            n,
            f,
            newt_tiny_quorums,
            transitive_conflicts,
            execute_at_commit,
            leader,
            workers,
            executors,
        }
    }

    /// Retrieve the number of processes.
    pub fn n(&self) -> usize {
        self.n
    }

    /// Retrieve the number of faults tolerated.
    pub fn f(&self) -> usize {
        self.f
    }

    /// Checks whether newt tiny quorums is enabled or not.
    pub fn newt_tiny_quorums(&self) -> bool {
        self.newt_tiny_quorums
    }

    /// Changes the value of `newt_tiny_quorums`.
    pub fn set_newt_tiny_quorums(&mut self, newt_tiny_quorums: bool) {
        self.newt_tiny_quorums = newt_tiny_quorums;
    }

    /// Checks whether we can assume that conflicts are transitive.
    pub fn transitive_conflicts(&self) -> bool {
        self.transitive_conflicts
    }

    /// Changes the value of `transitive_conflicts`.
    pub fn set_transitive_conflicts(&mut self, transitive_conflicts: bool) {
        self.transitive_conflicts = transitive_conflicts;
    }

    /// Checks whether execution is to be skipped.
    pub fn execute_at_commit(&self) -> bool {
        self.execute_at_commit
    }

    /// Changes the value of `execute_at_commit`.
    pub fn set_execute_at_commit(&mut self, execute_at_commit: bool) {
        self.execute_at_commit = execute_at_commit;
    }

    /// Checks whether a starting leader has been defined.
    pub fn leader(&self) -> Option<ProcessId> {
        self.leader
    }

    /// Sets the starting leader.
    pub fn set_leader(&mut self, leader: ProcessId) {
        self.leader = Some(leader);
    }

    /// Checks the number of `Protocol` workers.
    pub fn workers(&self) -> usize {
        self.workers
    }

    /// Changes the value of `Protocol` workers.
    pub fn set_workers(&mut self, workers: usize) {
        self.workers = workers;
    }

    /// Checks the number of `Executor`'s.
    pub fn executors(&self) -> usize {
        self.executors
    }

    /// Changes the value of `Executor`'s.
    pub fn set_executors(&mut self, executors: usize) {
        self.executors = executors;
    }
}

impl Config {
    /// Computes `Basic` quorum size.
    pub fn basic_quorum_size(&self) -> usize {
        self.f + 1
    }

    /// Computes `FPaxos` quorum size.
    pub fn fpaxos_quorum_size(&self) -> usize {
        self.f + 1
    }

    /// Computes `Atlas` fast and write quorum sizes.
    pub fn atlas_quorum_sizes(&self) -> (usize, usize) {
        let n = self.n;
        let f = self.f;
        let fast_quorum_size = (n / 2) + f;
        let write_quorum_size = f + 1;
        (fast_quorum_size, write_quorum_size)
    }

    /// Computes `EPaxos` fast and write quorum sizes.
    pub fn epaxos_quorum_sizes(&self) -> (usize, usize) {
        let n = self.n;
        // ignore config.f() since EPaxos always tolerates a minority of
        // failures
        let f = n / 2;
        let fast_quorum_size = f + ((f + 1) / 2 as usize);
        let write_quorum_size = f + 1;
        (fast_quorum_size, write_quorum_size)
    }

    /// Computes `Caesar` fast and write quorum sizes.
    pub fn caesar_quorum_sizes(&self) -> (usize, usize) {
        let n = self.n;
        let fast_quorum_size = (3 * n) / 4;
        let write_quorum_size = (n / 2) + 1;
        (fast_quorum_size, write_quorum_size)
    }

    /// Computes `Newt` fast quorum size, stability threshold and write quorum
    /// size.
    ///
    /// The threshold should be n - q + 1, where n is the number of processes
    /// and q the size of the quorum used to compute clocks. In `Newt` e.g.
    /// with tiny quorums, although the fast quorum is 2f (which would
    /// suggest q = 2f), in fact q = f + 1. The quorum size of 2f ensures that
    /// all clocks are computed from f + 1 processes. So, n - q + 1 = n - (f
    /// + 1) + 1 = n - f.
    ///
    /// In general, the stability threshold is given by:
    ///   "n - (fast_quorum_size - f + 1) + 1 = n - fast_quorum_size + f"
    /// - this ensures that the stability threshold plus the minimum number of
    ///   processes where clocks are computed (i.e. fast_quorum_size - f + 1) is
    ///   greater than n
    pub fn newt_quorum_sizes(&self) -> (usize, usize, usize) {
        let n = self.n;
        let f = self.f;
        let minority = n / 2;
        let (fast_quorum_size, stability_threshold) = if self.newt_tiny_quorums
        {
            (2 * f, n - f)
        } else {
            (minority + f, minority + 1)
        };
        let write_quorum_size = f + 1;
        (fast_quorum_size, write_quorum_size, stability_threshold)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config() {
        // n and f
        let n = 5;
        let f = 1;

        // config
        let mut config = Config::new(n, f);

        assert_eq!(config.n(), n);
        assert_eq!(config.f(), f);

        // by default, newt tiny quorums is false
        assert!(!config.newt_tiny_quorums());

        // if we change it to false, remains false
        config.set_newt_tiny_quorums(false);
        assert!(!config.newt_tiny_quorums());

        // if we change it to true, it becomes true
        config.set_newt_tiny_quorums(true);
        assert!(config.newt_tiny_quorums());

        // by default, transitive conflicts is false
        assert!(!config.transitive_conflicts());

        // if we change it to false, remains false
        config.set_transitive_conflicts(false);
        assert!(!config.transitive_conflicts());

        // if we change it to true, it becomes true
        config.set_transitive_conflicts(true);
        assert!(config.transitive_conflicts());

        // by deafult, execute at commit is false
        assert!(!config.execute_at_commit());
        // but that can change
        config.set_execute_at_commit(true);
        assert!(config.execute_at_commit());

        // by default, there's no leader
        assert!(config.leader().is_none());
        // but that can change
        let leader = 1;
        config.set_leader(leader);
        assert_eq!(config.leader(), Some(leader));

        // by default, there's one protocol worker and one executor worker
        assert_eq!(config.workers(), 1);
        assert_eq!(config.executors(), 1);

        // change their values and check they have changed
        config.set_workers(10);
        config.set_executors(20);
        assert_eq!(config.workers(), 10);
        assert_eq!(config.executors(), 20);
    }

    #[test]
    fn basic_parameters() {
        let config = Config::new(7, 1);
        assert_eq!(config.basic_quorum_size(), 2);

        let config = Config::new(7, 2);
        assert_eq!(config.atlas_quorum_sizes(), (5, 3));
        assert_eq!(config.basic_quorum_size(), 3);

        let config = Config::new(7, 3);
        assert_eq!(config.basic_quorum_size(), 4);
    }

    #[test]
    fn atlas_parameters() {
        let config = Config::new(7, 1);
        assert_eq!(config.atlas_quorum_sizes(), (4, 2));

        let config = Config::new(7, 2);
        assert_eq!(config.atlas_quorum_sizes(), (5, 3));

        let config = Config::new(7, 3);
        assert_eq!(config.atlas_quorum_sizes(), (6, 4));
    }

    #[test]
    fn epaxos_parameters() {
        let ns = vec![3, 5, 7, 9, 11, 13, 15, 17];
        // expected pairs of fast and write quorum sizes
        let expected = vec![
            (2, 2),
            (3, 3),
            (5, 4),
            (6, 5),
            (8, 6),
            (9, 7),
            (11, 8),
            (12, 9),
        ];

        let fs: Vec<_> = ns
            .into_iter()
            .map(|n| {
                // this f value won't be used
                let f = 0;
                let config = Config::new(n, f);
                config.epaxos_quorum_sizes()
            })
            .collect();
        assert_eq!(fs, expected);
    }

    #[test]
    fn caesar_parameters() {
        let ns = vec![3, 5, 7, 9, 11];
        // expected pairs of fast and write quorum sizes
        let expected = vec![(2, 2), (3, 3), (5, 4), (6, 5), (8, 6)];

        let fs: Vec<_> = ns
            .into_iter()
            .map(|n| {
                // this f value won't be used
                let f = 0;
                let config = Config::new(n, f);
                config.caesar_quorum_sizes()
            })
            .collect();
        assert_eq!(fs, expected);
    }

    #[test]
    fn newt_parameters() {
        // tiny quorums = false
        let mut config = Config::new(7, 1);
        config.set_newt_tiny_quorums(false);
        assert_eq!(config.newt_quorum_sizes(), (4, 2, 4));

        let mut config = Config::new(7, 2);
        config.set_newt_tiny_quorums(false);
        assert_eq!(config.newt_quorum_sizes(), (5, 3, 4));

        // tiny quorums = true
        let mut config = Config::new(7, 1);
        config.set_newt_tiny_quorums(true);
        assert_eq!(config.newt_quorum_sizes(), (2, 2, 6));

        let mut config = Config::new(7, 2);
        config.set_newt_tiny_quorums(true);
        assert_eq!(config.newt_quorum_sizes(), (4, 3, 5));
    }
}
