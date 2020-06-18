use std::time::{SystemTime, UNIX_EPOCH};

pub trait SysTime: Send + 'static + Sync /* TODO why is Sync needed here */ {
    /// Returns the current time in milliseconds.
    fn now(&self) -> u64;
}

// TODO find a better name
pub struct RunTime;

impl SysTime for RunTime {
    fn now(&self) -> u64 {
        let now = SystemTime::now();
        let micros = now
            .duration_since(UNIX_EPOCH)
            .expect("we're way past UNIX EPOCH")
            .as_millis();
        // TODO check following is not needed to make we don't truncate
        // const MAX_U64: u128 = u64::max_value() as u128;
        // if micros > MAX_U64 {
        //     panic!("current time (millis) doesn't fit in 64bits");
        // }
        micros as u64
    }
}

#[derive(Default)]
pub struct SimTime {
    time: u64,
}

impl SimTime {
    /// Creates a new simulation time.
    pub fn new() -> Self {
        // With many many operations, it can happen that logical clocks are
        // higher that simulation time (if it starts at 0), and in that case,
        // the real time feature of newt doesn't work.
        // - By initializing it the the milliseconds since the unix epoch, we
        // "make sure" that such situation never arises.
        // - TODO assert that logical clocks are never higher that time.
        // - In fact, with 1024, n = 5, tiny quorums and a clock bump interval
        //   of 50ms, I can see commands being committed with a timestamp 9000
        //   higher than the simulation time.
        let time = RunTime.now();
        Self { time }
    }

    /// Increases simulation time by `tick`.
    pub fn tick(&mut self, tick: u64) {
        self.time += tick;
    }

    /// Sets simulation time to `new_time`.
    pub fn set_time(&mut self, new_time: u64) {
        // make sure time is monotonic
        assert!(self.time <= new_time);
        self.time = new_time;
    }
}

impl SysTime for SimTime {
    fn now(&self) -> u64 {
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
