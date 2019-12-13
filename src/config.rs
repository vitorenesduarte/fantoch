#[derive(Debug, Clone, Copy)]
pub struct Config {
    /// number of processes
    n: usize,
    /// number of tolerated faults
    f: usize,
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
        Self { n, f }
    }

    /// Retrieve the number of processes.
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
