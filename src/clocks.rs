use crate::base::ProcId;
use crate::command::{Command, Object};
use crate::votes::{ProcVotes, VoteRange};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Clocks {
    id: ProcId,
    clocks: HashMap<Object, usize>,
}

impl Clocks {
    /// Create a new `Clocks` instance.
    pub fn new(id: ProcId) -> Self {
        Clocks {
            id,
            clocks: HashMap::new(),
        }
    }

    /// Compute the clock of this command.
    pub fn clock(&self, cmd: &Command) -> usize {
        // compute the maximum between all clocks of the objects touched by this
        // command
        cmd.objects()
            .iter()
            .map(|object| self.object_clock(object))
            .max()
            .unwrap_or(0)
    }

    /// Retrives the current clock of some object.
    fn object_clock(&self, obj: &Object) -> usize {
        self.clocks.get(obj).cloned().unwrap_or(0)
    }

    /// Computes `ProcVotes`.
    pub fn proc_votes(&self, cmd: &Command, clock: usize) -> ProcVotes {
        cmd.objects_clone()
            .into_iter()
            .map(|object| {
                // vote from the current clock value + 1 until the highest vote
                // (i.e. the maximum between all object's clocks)
                let vr = VoteRange::new(
                    self.id,
                    self.object_clock(&object) + 1,
                    clock,
                );
                (object, vr)
            })
            .collect()
    }

    /// Bump all objects clocks to `clock`.
    pub fn bump_to(&mut self, cmd: &Command, clock: usize) {
        for object in cmd.objects_clone() {
            self.clocks.insert(object, clock);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::clocks::Clocks;
    use crate::command::{Command, Object};

    #[test]
    fn clocks_flow() {
        // create clocks
        let mut clocks = Clocks::new(0);

        // objects and commands
        let object_a = Object::new("A");
        let object_b = Object::new("B");
        let command_a = Command::new(vec![object_a.clone()]);
        let command_b = Command::new(vec![object_b.clone()]);
        let command_ab = Command::new(vec![object_a.clone(), object_b.clone()]);

        // -------------------------
        // first clock for command a
        let clock = clocks.clock(&command_a);
        assert_eq!(clock, 0);

        // newt behaviour: current clock + 1
        let clock = clock + 1;

        // get proc votes
        let proc_votes = clocks.proc_votes(&command_a, clock);
        assert_eq!(proc_votes.len(), 1); // single object
        assert_eq!(proc_votes.get(&object_a).unwrap().votes(), vec![1]);

        // bump clocks
        clocks.bump_to(&command_a, clock);

        // -------------------------
        // second clock for command a
        let clock = clocks.clock(&command_a);
        assert_eq!(clock, 1);

        // newt behaviour: current clock + 1
        let clock = clock + 1;

        // get proc votes
        let proc_votes = clocks.proc_votes(&command_a, clock);
        assert_eq!(proc_votes.len(), 1); // single object
        assert_eq!(proc_votes.get(&object_a).unwrap().votes(), vec![2]);

        // bump clocks
        clocks.bump_to(&command_a, clock);

        // -------------------------
        // first clock for command ab
        let clock = clocks.clock(&command_ab);
        assert_eq!(clock, 2);

        // newt behaviour: current clock + 1
        let clock = clock + 1;

        // get proc votes
        let proc_votes = clocks.proc_votes(&command_ab, clock);
        assert_eq!(proc_votes.len(), 2); // two objects
        assert_eq!(proc_votes.get(&object_a).unwrap().votes(), vec![3]);
        assert_eq!(proc_votes.get(&object_b).unwrap().votes(), vec![1, 2, 3]);

        // bump clock
        clocks.bump_to(&command_ab, clock);

        // -------------------------
        // first clock for command b
        let clock = clocks.clock(&command_b);
        assert_eq!(clock, 3);

        // newt behaviour: current clock + 1
        let clock = clock + 1;

        // get proc votes
        let proc_votes = clocks.proc_votes(&command_a, clock);
        assert_eq!(proc_votes.len(), 1); // single object
        assert_eq!(proc_votes.get(&object_a).unwrap().votes(), vec![4]);
    }
}
