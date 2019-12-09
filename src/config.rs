#[derive(Debug, Clone, Copy)]
pub struct Config {
    /// number of procs
    n: usize,
    /// number of tolerated faults
    f: usize,
}

impl Config {
    /// Create a new `Config`.
    /// The first argument `n` represents the number of procs in the system.
    /// The second argument `f` represents the number of faults tolerated by the
    /// system.
    pub fn new(n: usize, f: usize) -> Self {
        assert!(f <= n / 2);
        Self { n, f }
    }

    /// Retrieve the number of procs.
    pub fn n(&self) -> usize {
        self.n
    }

    /// Retrieve the number of faults tolerated.
    pub fn f(&self) -> usize {
        self.f
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
    #[should_panic]
    fn config_panic() {
        let n = 5;
        // with f = 1 and f = 2, there should be no panic
        assert_eq!(Config::new(n, 1).f(), 1);
        assert_eq!(Config::new(n, 2).f(), 2);
        // now with f = 3 it should panic
        let _config = Config::new(n, 3);
    }
}
