use crate::time::SimTime;
use std::collections::BTreeMap;

pub struct Schedule<A> {
    // mapping from scheduled time to list of scheduled actions
    schedule: BTreeMap<u64, Vec<A>>,
    min_time: Option<u64>,
}

impl<A> Schedule<A> {
    pub fn new() -> Self {
        Self {
            schedule: BTreeMap::new(),
            min_time: None,
        }
    }

    /// Schedule a new `ScheduleAction` at a certain `time`.
    pub fn schedule(&mut self, time: u64, action: A) {
        // get already scheduled actions for this `time`
        let actions = self.schedule.entry(time).or_insert_with(Vec::new);
        // insert new action
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
        // create simulation time
        let mut time = SimTime::new();

        // create schedule
        let mut schedule: Schedule<u64> = Schedule::new();
        // check there are no next actions
        assert!(schedule.next_actions(&mut time).is_none());
    }
}
