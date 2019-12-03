#[derive(Debug, Clone)]
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
}
