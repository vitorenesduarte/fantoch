use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub trait SysTime: Send + 'static + Sync /* TODO why is Sync needed here */ {
    fn millis(&self) -> u64;
    fn micros(&self) -> u64;
}

// TODO find a better name
pub struct RunTime;

impl RunTime {
    fn duration_since_unix_epoch(&self) -> Duration {
        let now = SystemTime::now();
        now.duration_since(UNIX_EPOCH)
            .expect("we're way past UNIX EPOCH")
    }
}

impl SysTime for RunTime {
    fn millis(&self) -> u64 {
        self.duration_since_unix_epoch().as_millis() as u64
    }

    fn micros(&self) -> u64 {
        self.duration_since_unix_epoch().as_micros() as u64
    }
}

#[derive(Default)]
pub struct SimTime {
    micros: u64,
}

impl SimTime {
    /// Creates a new simulation time.
    pub fn new() -> Self {
        Self { micros: 0 }
    }

    // Increases simulation time by `millis`.
    pub fn add_millis(&mut self, millis: u64) {
        self.micros += Self::millis_to_micros(millis);
    }

    /// Sets simulation time.
    pub fn set_millis(&mut self, new_time_millis: u64) {
        let new_time_micros = Self::millis_to_micros(new_time_millis);
        // make sure time is monotonic
        assert!(self.micros <= new_time_micros);
        self.micros = new_time_micros;
    }

    fn millis_to_micros(millis: u64) -> u64 {
        millis * 1000
    }

    fn micros_to_millis(micros: u64) -> u64 {
        micros / 1000
    }
}

impl SysTime for SimTime {
    fn micros(&self) -> u64 {
        self.micros
    }

    fn millis(&self) -> u64 {
        Self::micros_to_millis(self.micros)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sim_now() {
        // create new simulation time
        let mut time = SimTime::new();
        assert_eq!(time.micros(), 0);

        // first tick
        let tick = 10;
        time.add_millis(tick);
        assert_eq!(time.millis(), 10);

        // second tick
        let tick = 6;
        time.add_millis(tick);
        assert_eq!(time.millis(), 16);

        // set time at 20
        time.set_millis(20);
        assert_eq!(time.millis(), 20);
    }

    #[test]
    #[should_panic]
    fn sim_time_should_be_monotonic() {
        // create new simulation time
        let mut time = SimTime::new();

        // set time at 20
        time.set_millis(20);
        assert_eq!(time.micros(), 20);

        // set time at 19
        // should panic!
        time.set_millis(19);
    }
}
