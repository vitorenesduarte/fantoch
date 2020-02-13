use std::time::{SystemTime, UNIX_EPOCH};

pub trait SysTime: Send + 'static + Sync /* TODO why is Sync needed here */ {
    /// Returns the current time.
    fn now(&self) -> u128;
}

// TODO find a better name
pub struct RunTime;

impl SysTime for RunTime {
    fn now(&self) -> u128 {
        let now = SystemTime::now();
        now.duration_since(UNIX_EPOCH)
            .expect("we're way past UNIX EPOCH")
            .as_micros()
    }
}

#[derive(Default)]
pub struct SimTime {
    time: u128,
}

impl SimTime {
    /// Creates a new simulation time.
    pub fn new() -> Self {
        Default::default()
    }

    /// Increases simulation time by `tick`.
    pub fn tick(&mut self, tick: u128) {
        self.time += tick;
    }

    /// Sets simulation time to `new_time`.
    pub fn set_time(&mut self, new_time: u128) {
        // make sure time is monotonic
        assert!(self.time <= new_time);
        self.time = new_time;
    }
}

impl SysTime for SimTime {
    fn now(&self) -> u128 {
        self.time
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sim_now() {
        // create new simulation time
        let mut time = SimTime::new();
        assert_eq!(time.now(), 0);

        // first tick
        let tick = 10;
        time.tick(tick);
        assert_eq!(time.now(), 10);

        // second tick
        let tick = 6;
        time.tick(tick);
        assert_eq!(time.now(), 16);

        // set time at 20
        time.set_time(20);
        assert_eq!(time.now(), 20);
    }

    #[test]
    #[should_panic]
    fn sim_time_should_be_monotonic() {
        // create new simulation time
        let mut time = SimTime::new();

        // set time at 20
        time.set_time(20);
        assert_eq!(time.now(), 20);

        // set time at 19
        // should panic!
        time.set_time(19);
    }
}
