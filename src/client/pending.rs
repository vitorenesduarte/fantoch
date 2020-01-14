use crate::id::Rifl;
use crate::time::SysTime;
use std::collections::HashMap;

#[derive(Default)]
pub struct Pending {
    /// mapping from Rifl to command start time
    pending: HashMap<Rifl, u128>,
}

impl Pending {
    /// Create a new `Pending`
    pub fn new() -> Self {
        Default::default()
    }

    /// Start a command given its rifl.
    pub fn start(&mut self, rifl: Rifl, time: &dyn SysTime) {
        // compute start time
        let start_time = time.now();
        // add to pending and check it has never been added before
        // TODO: replace with `.expect_none` once it's stabilized
        if self.pending.insert(rifl, start_time).is_some() {
            panic!("the same rifl can't be inserted twice in client pending list of commands");
        }
    }

    /// End a command returns command latency and the time it was returned.
    pub fn end(&mut self, rifl: Rifl, time: &dyn SysTime) -> (u64, u64) {
        // get start time
        let start_time = self
            .pending
            .remove(&rifl)
            .expect("can't end a command if a command has not started");
        // compute end time
        let end_time = time.now();
        // make sure time is monotonic
        assert!(start_time <= end_time);
        // compute latency
        let latency = end_time - start_time;
        // (both should fit in u64)
        (latency as u64, end_time as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::RiflGen;
    use crate::time::SimTime;

    #[test]
    fn pending_flow() {
        // create pending
        let mut pending = Pending::new();

        // create rifl gen and 3 rifls
        let source = 10;
        let mut rifl_gen = RiflGen::new(source);
        let rifl1 = rifl_gen.next_id();
        let rifl2 = rifl_gen.next_id();
        let rifl3 = rifl_gen.next_id();

        // create sys time
        let mut time = SimTime::new();

        // start first rifl at time 0
        pending.start(rifl1, &time);

        // start second rifl at time 10
        time.tick(10);
        pending.start(rifl2, &time);

        // end first rifl at time 11
        time.tick(1);
        let (latency, return_time) = pending.end(rifl1, &time);
        assert_eq!(latency, 11);
        assert_eq!(return_time, 11);

        // start third rifl at time 15
        time.tick(4);
        pending.start(rifl3, &time);

        // end third rifl at time 16
        time.tick(1);
        let (latency, return_time) = pending.end(rifl3, &time);
        assert_eq!(latency, 1);
        assert_eq!(return_time, 16);

        // end second rifl at time 20
        time.tick(4);
        let (latency, return_time) = pending.end(rifl2, &time);
        assert_eq!(latency, 10);
        assert_eq!(return_time, 20);
    }

    #[test]
    #[should_panic]
    fn double_start() {
        // create pending
        let mut pending = Pending::new();

        // create rifl gen and 1 rifl
        let source = 10;
        let mut rifl_gen = RiflGen::new(source);
        let rifl1 = rifl_gen.next_id();

        // create sys time
        let time = SimTime::new();

        // start rifl1 twice
        pending.start(rifl1, &time);
        // should panic!
        pending.start(rifl1, &time);
    }

    #[test]
    #[should_panic]
    fn double_end() {
        // create pending
        let mut pending = Pending::new();

        // create rifl gen and 1 rifl
        let source = 10;
        let mut rifl_gen = RiflGen::new(source);
        let rifl1 = rifl_gen.next_id();

        // create sys time
        let time = SimTime::new();

        // end rifl1 before starting it (basically a double end)
        // should panic!
        pending.end(rifl1, &time);
    }
}
