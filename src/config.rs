#[derive(Debug, Clone, Copy)]
pub struct Config {
    /// number of processes
    n: usize,
    /// number of tolerated faults
    f: usize,
    /// defines whether newt should employ tiny quorums or not
    newt_tiny_quorums: bool,
}

impl Config {
    /// Create a new `Config`.
    /// The first argument `n` represents the number of processes in the system.
    /// The second argument `f` represents the number of faults tolerated by the
    /// system.
    pub fn new(n: usize, f: usize) -> Self {
        if f > n / 2 {
            println!("WARNING: f={} is larger than a majority with n={}", f, n);
        }
        // by default, `newt_tiny_quorums = false`
        let newt_tiny_quorums = false;
        Self {
            n,
            f,
            newt_tiny_quorums,
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
        let config = Config::new(n, f);

        assert_eq!(config.n(), n);
        assert_eq!(config.f(), f);
    }

    #[test]
    fn newt_tiny_quorums() {
        // n and f
        let n = 5;
        let f = 1;

        // config
        let mut config = Config::new(n, f);

        // by default, false
        assert!(!config.newt_tiny_quorums());

        // if we change it to false, remains false
        config.set_newt_tiny_quorums(false);
        assert!(!config.newt_tiny_quorums());

        // if we change it to true, it becomes true
        config.set_newt_tiny_quorums(true);
        assert!(config.newt_tiny_quorums());
    }
}
