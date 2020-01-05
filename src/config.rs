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
        Self {
            n,
            f,
            newt_tiny_quorums,
            transitive_conflicts,
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

    /// Checks whether whether we can assume that conflicts are transitive.
    pub fn transitive_conflicts(&self) -> bool {
        self.transitive_conflicts
    }

    /// Changes the value of `transitive_conflicts`.
    pub fn set_transitive_conflicts(&mut self, transitive_conflicts: bool) {
        self.transitive_conflicts = transitive_conflicts;
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
    }
}
