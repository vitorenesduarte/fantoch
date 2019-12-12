use crate::command::CommandResult;
use crate::id::{ClientId, ProcId};
use crate::newt::Message;
use crate::time::SimTime;
use std::collections::BTreeMap;

pub enum ScheduleAction {
    SendToProc(ProcId, Message),
    SendToClient(ClientId, CommandResult),
}

#[derive(Default)]
pub struct Schedule {
    // mapping from scheduled time to list of schedule actions
    schedule: BTreeMap<u64, Vec<ScheduleAction>>,
    min_time: Option<u64>,
}

impl Schedule {
    pub fn new() -> Self {
        Default::default()
    }

    /// Schedule a new `ScheduleAction` at a certain `time`.
    pub fn schedule(&mut self, time: u64, action: ScheduleAction) {
        // get already scheduled actions for this `time`
        let actions = self.schedule.entry(time).or_insert_with(Vec::new);
        // insert new action
        actions.push(action);
        // update `self.min_time`
        let min_time = self.min_time.get_or_insert(time);
        *min_time = std::cmp::min(*min_time, time);
    }

    /// Retrieve the next list of schedule actions.
    pub fn next_actions(&mut self, time: &mut SimTime) -> Vec<ScheduleAction> {
        if let Some(min_time) = self.min_time {
            // advance simulation time
            time.set_time(min_time);

            // get actions scheduled for `min_time`
            let actions = self
                .schedule
                .remove(&min_time)
                .expect("this time must exist in the schedule");
            // update `self.min_time`
            self.min_time = self.schedule.iter().map(|(min_time, _)| *min_time).next();
            // return next actions
            actions
        } else {
            Vec::new()
        }
    }
}
