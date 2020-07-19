use crate::time::{SimTime, SysTime};
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::time::Duration;

pub struct Schedule<A: Eq> {
    queue: BinaryHeap<Reverse<QueueEntry<A>>>,
}

#[derive(PartialEq, Eq)]
struct QueueEntry<A: Eq> {
    schedule_time: u64,
    action: A,
}

impl<A: Eq> Ord for QueueEntry<A> {
    fn cmp(&self, other: &Self) -> Ordering {
        // simply compare their schedule time
        self.schedule_time.cmp(&other.schedule_time)
    }
}

impl<A: Eq> PartialOrd for QueueEntry<A> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<A: Eq> Schedule<A> {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            queue: BinaryHeap::new(),
        }
    }

    /// Schedule a new `ScheduleAction` at a certain `time`.
    pub fn schedule(&mut self, time: &SimTime, delay: Duration, action: A) {
        // compute schedule time
        let schedule_time = time.millis() + delay.as_millis() as u64;

        // create new queue entry
        let entry = QueueEntry {
            schedule_time,
            action,
        };
        // push new entry to the queue
        self.queue.push(Reverse(entry));
    }

    /// Retrieve the next scheduled action.
    pub fn next_action(&mut self, time: &mut SimTime) -> Option<A> {
        // get the next actions
        self.queue.pop().map(|entry| {
            // advance simulation time
            time.set_millis(entry.0.schedule_time);
            // return only the action
            entry.0.action
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedule_flow() {
        // create simulation time and schedule
        let mut time = SimTime::new();
        let mut schedule: Schedule<String> = Schedule::new();

        // check min time is none and there are no next actions
        assert!(schedule.next_action(&mut time).is_none());

        // schedule "a" with a delay 10
        schedule.schedule(&time, Duration::from_millis(10), String::from("a"));

        // check "a" is the next action, simulation time is now 10
        let next = schedule
            .next_action(&mut time)
            .expect("there should be a next action");
        assert_eq!(next, String::from("a"));
        assert_eq!(time.millis(), 10);
        assert!(schedule.next_action(&mut time).is_none());

        // schedule "b" with a delay 7, "c" with delay 2
        schedule.schedule(&time, Duration::from_millis(7), String::from("b"));
        schedule.schedule(&time, Duration::from_millis(2), String::from("c"));

        // check "c" is the next action, simulation time is now 12
        let next = schedule
            .next_action(&mut time)
            .expect("there should be a next action");
        assert_eq!(next, String::from("c"));
        assert_eq!(time.millis(), 12);

        // schedule "d" with a delay 2, "e" with delay 5
        schedule.schedule(&time, Duration::from_millis(2), String::from("d"));
        schedule.schedule(&time, Duration::from_millis(5), String::from("e"));

        // check "d" is the next action, simulation time is now 14
        let next = schedule
            .next_action(&mut time)
            .expect("there should be a next action");
        assert_eq!(next, String::from("d"));
        assert_eq!(time.millis(), 14);

        // check "b" and "e" are the next actions, simulation time is now 17
        let next = schedule
            .next_action(&mut time)
            .expect("there should be a next action");
        assert!(next == String::from("b") || next == String::from("e"));
        assert_eq!(time.millis(), 17);
        let next = schedule
            .next_action(&mut time)
            .expect("there should be a next action");
        assert!(next == String::from("b") || next == String::from("e"));
        assert_eq!(time.millis(), 17);
    }
}
