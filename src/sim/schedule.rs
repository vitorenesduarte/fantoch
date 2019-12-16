use crate::time::{SimTime, SysTime};
use std::collections::BTreeMap;

pub struct Schedule<A> {
    // mapping from scheduled time to list of scheduled actions
    schedule: BTreeMap<u64, Vec<A>>,
    min_time: Option<u64>,
}

impl<A> Schedule<A> {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            schedule: BTreeMap::new(),
            min_time: None,
        }
    }

    /// Schedule a new `ScheduleAction` at a certain `time`.
    pub fn schedule(&mut self, time: &SimTime, delay: u64, action: A) {
        // compute schedule time
        let time = time.now() + delay;

        // get already scheduled actions for this `time` and insert new `action`
        let actions = self.schedule.entry(time).or_insert_with(Vec::new);
        actions.push(action);

        // update `self.min_time`
        let min_time = self.min_time.get_or_insert(time);
        *min_time = std::cmp::min(*min_time, time);
    }

    /// Retrieve the next list of schedule actions.
    pub fn next_actions(&mut self, time: &mut SimTime) -> Option<Vec<A>> {
        let actions = self.min_time.map(|min_time| {
            // advance simulation time
            time.set_time(min_time);

            // get actions scheduled for `min_time`
            self.schedule
                .remove(&min_time)
                .expect("this time must exist in the schedule")
        });

        // update `self.min_time`
        self.min_time = self.schedule.iter().map(|(min_time, _)| *min_time).next();

        // return next actions
        actions
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
        assert!(schedule.min_time.is_none());
        assert!(schedule.next_actions(&mut time).is_none());

        // schedule "a" with a delay 10 and check min time
        schedule.schedule(&time, 10, String::from("a"));
        assert_eq!(schedule.min_time, Some(10));

        // check "a" is the next action, simulation time is now 10, and min time is none
        let next = schedule
            .next_actions(&mut time)
            .expect("there should be next actions");
        assert_eq!(next, vec![String::from("a")]);
        assert_eq!(time.now(), 10);
        assert!(schedule.min_time.is_none());
        assert!(schedule.next_actions(&mut time).is_none());

        // schedule "b" with a delay 7, "c" with delay 2, and check min time
        schedule.schedule(&time, 7, String::from("b"));
        assert_eq!(schedule.min_time, Some(17));
        schedule.schedule(&time, 2, String::from("c"));
        assert_eq!(schedule.min_time, Some(12));

        // check "c" is the next action, simulation time is now 12, and min time is 17
        let next = schedule
            .next_actions(&mut time)
            .expect("there should be next actions");
        assert_eq!(next, vec![String::from("c")]);
        assert_eq!(time.now(), 12);
        assert_eq!(schedule.min_time, Some(17));

        // schedule "d" with a delay 2, "e" with delay 5, and check min time
        schedule.schedule(&time, 2, String::from("d"));
        assert_eq!(schedule.min_time, Some(14));
        schedule.schedule(&time, 5, String::from("e"));
        assert_eq!(schedule.min_time, Some(14));

        // check "d" is the next action, simulation time is now 14, and min time is 17
        let next = schedule
            .next_actions(&mut time)
            .expect("there should be next actions");
        assert_eq!(next, vec![String::from("d")]);
        assert_eq!(time.now(), 14);
        assert_eq!(schedule.min_time, Some(17));

        // check "b" and "e" are the next actions, simulation time is now 17, and min time is none
        let next = schedule
            .next_actions(&mut time)
            .expect("there should be next actions");
        assert_eq!(next, vec![String::from("b"), String::from("e")]);
        assert_eq!(time.now(), 17);
        assert_eq!(schedule.min_time, None);
    }
}
