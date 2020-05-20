use crate::time::{SimTime, SysTime};
use std::collections::BTreeMap;

pub struct Schedule<A> {
    // mapping from scheduled time to list of scheduled actions
    schedule: BTreeMap<u128, Vec<A>>,
}

impl<A> Schedule<A> {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            schedule: BTreeMap::new(),
        }
    }

    /// Schedule a new `ScheduleAction` at a certain `time`.
    pub fn schedule(&mut self, time: &SimTime, delay: u128, action: A) {
        // compute schedule time
        let schedule_time = time.now() + delay;

        // get already scheduled actions for this `time` and insert new `action`
        let actions =
            self.schedule.entry(schedule_time).or_insert_with(Vec::new);
        actions.push(action);
    }

    /// Retrieve the next list of schedule actions.
    pub fn next_actions(&mut self, time: &mut SimTime) -> Option<Vec<A>> {
        // get the next actions
        self.schedule.pop_first().map(|(min_time, actions)| {
            // advance simulation time
            time.set_time(min_time);
            // return only the actions
            actions
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
        assert!(schedule.next_actions(&mut time).is_none());

        // schedule "a" with a delay 10
        schedule.schedule(&time, 10, String::from("a"));

        // check "a" is the next action, simulation time is now 10
        let next = schedule
            .next_actions(&mut time)
            .expect("there should be next actions");
        assert_eq!(next, vec![String::from("a")]);
        assert_eq!(time.now(), 10);
        assert!(schedule.next_actions(&mut time).is_none());

        // schedule "b" with a delay 7, "c" with delay 2
        schedule.schedule(&time, 7, String::from("b"));
        schedule.schedule(&time, 2, String::from("c"));

        // check "c" is the next action, simulation time is now 12
        let next = schedule
            .next_actions(&mut time)
            .expect("there should be next actions");
        assert_eq!(next, vec![String::from("c")]);
        assert_eq!(time.now(), 12);

        // schedule "d" with a delay 2, "e" with delay 5
        schedule.schedule(&time, 2, String::from("d"));
        schedule.schedule(&time, 5, String::from("e"));

        // check "d" is the next action, simulation time is now 14
        let next = schedule
            .next_actions(&mut time)
            .expect("there should be next actions");
        assert_eq!(next, vec![String::from("d")]);
        assert_eq!(time.now(), 14);

        // check "b" and "e" are the next actions, simulation time is now 17
        let next = schedule
            .next_actions(&mut time)
            .expect("there should be next actions");
        assert_eq!(next, vec![String::from("b"), String::from("e")]);
        assert_eq!(time.now(), 17);
    }
}
