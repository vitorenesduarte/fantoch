pub trait SysTime {
    /// Returns the current time.
    fn time(&self) -> u64;
}

#[derive(Default)]
pub struct SimTime {
    time: u64,
}

impl SimTime {
    /// Creates a new simulation time.
    pub fn new() -> Self {
        Default::default()
    }

    /// Increases simulation time by `tick`.
    pub fn tick(&mut self, tick: u64) {
        self.time += tick;
    }
}

impl SysTime for SimTime {
    fn time(&self) -> u64 {
        self.time
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn sim_time() {
        // create new simulation time
        let mut time = SimTime::new();
        assert_eq!(time.time(), 0);

        // first tick
        let tick = 10;
        time.tick(tick);
        assert_eq!(time.time(), 10);

        // second tick
        let tick = 6;
        time.tick(tick);
        assert_eq!(time.time(), 16);
    }
}
