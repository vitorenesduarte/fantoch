// This module contains the definition of `TarjanSCCFinder` and `FinderResult`.
mod tarjan;

/// This module contains the definition of `VertexIndex` and `PendingIndex`.
mod index;

use crate::command::Command;
use crate::id::{Dot, ProcessId};
use crate::kvs::Key;
use crate::log;
use crate::protocol::atlas::queue::index::{PendingIndex, VertexIndex};
use crate::protocol::atlas::queue::tarjan::{FinderResult, TarjanSCCFinder, Vertex, SCC};
use crate::util;
use std::collections::HashSet;
use std::mem;
use threshold::{AEClock, VClock};

#[allow(dead_code)]
pub struct Queue {
    executed_clock: AEClock<ProcessId>,
    vertex_index: VertexIndex,
    pending_index: PendingIndex,
    to_execute: Vec<Command>,
}

impl Queue {
    /// Create a new `Queue`.
    #[allow(dead_code)]
    pub fn new(n: usize) -> Self {
        // create bottom executed clock
        let ids = util::process_ids(n);
        let executed_clock = AEClock::with(ids);
        // create indexes
        let vertex_index = VertexIndex::new();
        let pending_index = PendingIndex::new();
        // create to execute
        let to_execute = Vec::new();
        Self {
            executed_clock,
            vertex_index,
            pending_index,
            to_execute,
        }
    }

    /// Returns new commands ready to be executed.
    #[allow(dead_code)]
    #[must_use]
    pub fn to_execute(&mut self) -> Vec<Command> {
        let mut ready = Vec::new();
        mem::swap(&mut ready, &mut self.to_execute);
        ready
    }

    /// Add a new command with its clock to the queue.
    #[allow(dead_code)]
    pub fn add(&mut self, dot: Dot, cmd: Command, clock: VClock<ProcessId>) {
        log!("Queue::add {:?} {:?}", dot, clock);
        // create new vertex for this command
        let vertex = Vertex::new(dot, cmd, clock);

        // index vertex
        self.index(vertex);

        // try to find a new scc
        self.find_scc(dot);
    }

    fn index(&mut self, vertex: Vertex) {
        // index in pending index
        self.pending_index.index(&vertex);

        // index in vertex index and check if it hasn't been indexed before
        assert!(self.vertex_index.index(vertex));
    }

    fn find_scc(&mut self, dot: Dot) {
        log!("Queue:find_scc {:?}", dot);

        // TODO the following check is needed for `test_add_3`
        // - the way we're tracking pending commands may lead to a situation where we see two
        //   commands pending, we start the finder with one of them, find an SCC where both are
        //   there, and then we start a second finder with the second command; since this second
        //   command was found to be in an SCC with the first one, it won't be indexed
        if self.executed_clock.contains(&dot.source(), dot.sequence()) {
            return;
        }

        // execute tarjan's algorithm
        let mut finder = TarjanSCCFinder::new();
        let finder_result = finder.strong_connect(dot, &self.executed_clock, &self.vertex_index);

        // get sccs
        let sccs = finder.finalize(&self.vertex_index);

        // create set of keys in ready SCCs
        let mut keys = HashSet::new();

        if finder_result == FinderResult::Found {
            sccs.into_iter().for_each(|scc| {
                self.save_scc(scc, &mut keys);
            });
        }

        // try pending commands given the keys touched by ready SCCs
        self.try_pending(keys);
    }

    fn save_scc(&mut self, scc: SCC, keys: &mut HashSet<Key>) {
        scc.into_iter().for_each(|dot| {
            log!("Queue:save_scc removing {:?} from indexes", dot);

            // update executed clock
            self.executed_clock.add(&dot.source(), dot.sequence());

            // remove from vertex index
            let vertex = self
                .vertex_index
                .remove(&dot)
                .expect("dots from an SCC should exist");

            // remove from pending index
            self.pending_index.remove(&vertex);

            // update the set of keys
            // TODO can we avoid cloning here?
            keys.extend(vertex.command().keys().cloned());

            // add vertex to commands to be executed
            self.to_execute.push(vertex.into_command())
        })
    }

    fn try_pending(&mut self, keys: HashSet<Key>) {
        keys.into_iter().for_each(|key| {
            // try to find SCCs for each pending dot that has the same color of any command in the
            // ready SCCs
            // TODO we could optimize this process by maintaining a list of visited dots, as it is
            // done in the java implementation
            let pending = self
                .pending_index
                .pending(&key)
                .expect("key must exist in the pending index");

            for dot in pending {
                self.find_scc(dot);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::Rifl;
    use permutator::Permutation;

    #[test]
    fn simple() {
        // create queue
        let n = 2;
        let mut queue = Queue::new(n);

        // cmd 0
        let dot_0 = Dot::new(1, 1);
        let cmd_0 = Command::put(Rifl::new(1, 1), String::from("A"), String::new());
        let clock_0 = util::vclock(vec![0, 1]);

        // cmd 1
        let dot_1 = Dot::new(2, 1);
        let cmd_1 = Command::put(Rifl::new(2, 1), String::from("A"), String::new());
        let clock_1 = util::vclock(vec![1, 0]);

        // add cmd 0
        queue.add(dot_0, cmd_0.clone(), clock_0);
        // check commands ready to be executed
        assert!(queue.to_execute().is_empty());

        // add cmd 1
        queue.add(dot_1, cmd_1.clone(), clock_1);
        // check commands ready to be executed
        assert_eq!(queue.to_execute(), vec![cmd_0, cmd_1]);
    }
    #[test]
    fn test_add_1() {
        // {1, 2}, [2, 2]
        let dot_a = Dot::new(1, 2);
        let clock_a = util::vclock(vec![2, 2]);

        // {1, 1}, [3, 2]
        let dot_b = Dot::new(1, 1);
        let clock_b = util::vclock(vec![3, 2]);

        // {1, 5}, [6, 2]
        let dot_c = Dot::new(1, 5);
        let clock_c = util::vclock(vec![6, 2]);

        // {1, 6}, [6, 3]
        let dot_d = Dot::new(1, 6);
        let clock_d = util::vclock(vec![6, 3]);

        // {1, 3}, [3, 3]
        let dot_e = Dot::new(1, 3);
        let clock_e = util::vclock(vec![3, 3]);

        // {2, 2}, [0, 2]
        let dot_f = Dot::new(2, 2);
        let clock_f = util::vclock(vec![0, 2]);

        // {2, 1}, [4, 3]
        let dot_g = Dot::new(2, 1);
        let clock_g = util::vclock(vec![4, 3]);

        // {1, 4}, [6, 2]
        let dot_h = Dot::new(1, 4);
        let clock_h = util::vclock(vec![6, 2]);

        // {2, 3}, [6, 3]
        let dot_i = Dot::new(2, 3);
        let clock_i = util::vclock(vec![6, 3]);

        // create args
        let args = vec![
            (dot_a, clock_a),
            (dot_b, clock_b),
            (dot_c, clock_c),
            (dot_d, clock_d),
            (dot_e, clock_e),
            (dot_f, clock_f),
            (dot_g, clock_g),
            (dot_h, clock_h),
            (dot_i, clock_i),
        ];

        let n = 2;
        shuffle_it(n, args);
    }

    #[test]
    fn test_add_2() {
        // {2, 4}, [3, 4]
        let dot_a = Dot::new(2, 4);
        let clock_a = util::vclock(vec![3, 4]);

        // {2, 3}, [0, 3]
        let dot_b = Dot::new(2, 3);
        let clock_b = util::vclock(vec![0, 3]);

        // {1, 3}, [3, 3]
        let dot_c = Dot::new(1, 3);
        let clock_c = util::vclock(vec![3, 3]);

        // {1, 1}, [3, 4]
        let dot_d = Dot::new(1, 1);
        let clock_d = util::vclock(vec![3, 4]);

        // {2, 2}, [0, 2]
        let dot_e = Dot::new(2, 2);
        let clock_e = util::vclock(vec![0, 2]);

        // {1, 2}, [3, 3]
        let dot_f = Dot::new(1, 2);
        let clock_f = util::vclock(vec![3, 3]);

        // {2, 1}, [3, 3]
        let dot_g = Dot::new(2, 1);
        let clock_g = util::vclock(vec![3, 3]);

        // create args
        let args = vec![
            (dot_a, clock_a),
            (dot_b, clock_b),
            (dot_c, clock_c),
            (dot_d, clock_d),
            (dot_e, clock_e),
            (dot_f, clock_f),
            (dot_g, clock_g),
        ];

        let n = 2;
        shuffle_it(n, args);
    }

    #[test]
    fn test_add_3() {
        // {3, 2}, [1, 0, 2]
        let dot_a = Dot::new(3, 2);
        let clock_a = util::vclock(vec![1, 0, 2]);

        // {3, 3}, [1, 1, 3]
        let dot_b = Dot::new(3, 3);
        let clock_b = util::vclock(vec![1, 1, 3]);

        // {3, 1}, [1, 1, 3]
        let dot_c = Dot::new(3, 1);
        let clock_c = util::vclock(vec![1, 1, 3]);

        // {1, 1}, [1, 0, 0]
        let dot_d = Dot::new(1, 1);
        let clock_d = util::vclock(vec![1, 0, 0]);

        // {2, 1}, [1, 1, 2]
        let dot_e = Dot::new(2, 1);
        let clock_e = util::vclock(vec![1, 1, 2]);

        // create args
        let args = vec![
            (dot_a, clock_a),
            (dot_b, clock_b),
            (dot_c, clock_c),
            (dot_d, clock_d),
            (dot_e, clock_e),
        ];

        let n = 3;
        shuffle_it(n, args);
    }

    #[test]
    fn test_add_4() {
        // {1, 5}, [5]
        let dot_a = Dot::new(1, 5);
        let clock_a = util::vclock(vec![5]);

        // {1, 4}, [6]
        let dot_b = Dot::new(1, 4);
        let clock_b = util::vclock(vec![6]);

        // {1, 1}, [5]
        let dot_c = Dot::new(1, 1);
        let clock_c = util::vclock(vec![5]);

        // {1, 2}, [6]
        let dot_d = Dot::new(1, 2);
        let clock_d = util::vclock(vec![6]);

        // {1, 3}, [5]
        let dot_e = Dot::new(1, 3);
        let clock_e = util::vclock(vec![5]);

        // {1, 6}, [6]
        let dot_f = Dot::new(1, 6);
        let clock_f = util::vclock(vec![6]);

        // create args
        let args = vec![
            (dot_a, clock_a),
            (dot_b, clock_b),
            (dot_c, clock_c),
            (dot_d, clock_d),
            (dot_e, clock_e),
            (dot_f, clock_f),
        ];

        let n = 1;
        shuffle_it(n, args);
    }

    #[test]
    fn test_add_5() {
        // {1, 1}, [1, 1]
        let dot_a = Dot::new(1, 1);
        let clock_a = util::vclock(vec![1, 1]);

        // {1, 2}, [2, 0]
        let dot_b = Dot::new(1, 2);
        let clock_b = util::vclock(vec![2, 0]);

        // {2, 1}, [1, 1]
        let dot_c = Dot::new(2, 1);
        let clock_c = util::vclock(vec![1, 1]);

        // create args
        let args = vec![(dot_a, clock_a), (dot_b, clock_b), (dot_c, clock_c)];

        let n = 2;
        shuffle_it(n, args);
    }

    #[test]
    fn test_add_6() {
        // {1, 1}, [1, 0]
        let dot_a = Dot::new(1, 1);
        let clock_a = util::vclock(vec![1, 0]);

        // {1, 2}, [4, 1]
        let dot_b = Dot::new(1, 2);
        let clock_b = util::vclock(vec![4, 1]);

        // {1, 3}, [3, 0]
        let dot_c = Dot::new(1, 3);
        let clock_c = util::vclock(vec![3, 0]);

        // {1, 4}, [4, 0]
        let dot_d = Dot::new(1, 4);
        let clock_d = util::vclock(vec![4, 0]);

        // {2, 1}, [1, 1]
        let dot_e = Dot::new(2, 1);
        let clock_e = util::vclock(vec![1, 1]);

        // {2, 2}, [3, 2]
        let dot_f = Dot::new(2, 2);
        let clock_f = util::vclock(vec![3, 2]);

        // create args
        let args = vec![
            (dot_a, clock_a),
            (dot_b, clock_b),
            (dot_c, clock_c),
            (dot_d, clock_d),
            (dot_e, clock_e),
            (dot_f, clock_f),
        ];

        let n = 2;
        shuffle_it(n, args);
    }

    fn shuffle_it(n: usize, mut args: Vec<(Dot, VClock<ProcessId>)>) {
        let total_order = check_termination(n, args.clone());
        args.permutation().for_each(|permutation| {
            let sorted = check_termination(n, permutation);
            assert_eq!(total_order, sorted);
        });
    }

    fn check_termination(n: usize, args: Vec<(Dot, VClock<ProcessId>)>) -> Vec<Rifl> {
        // create queue
        let mut queue = Queue::new(n);
        let mut all_rifls = HashSet::new();
        let mut sorted = Vec::new();

        args.into_iter().for_each(|(dot, clock)| {
            // create command rifl from its dot
            let rifl = Rifl::new(dot.source(), dot.sequence());

            // create command
            let key = String::from("black");
            let value = String::from("");
            let cmd = Command::put(rifl, key, value);

            // add to the set of all rifls
            assert!(all_rifls.insert(rifl));

            // add it to the queue
            queue.add(dot, cmd, clock);

            // get ready to execute
            let to_execute = queue.to_execute();

            // for each command ready to be executed
            to_execute.iter().for_each(|cmd| {
                // get its rifl
                let rifl = cmd.rifl();

                // remove it from the set of rifls
                assert!(all_rifls.remove(&cmd.rifl()));

                // and add it to the sorted results
                sorted.push(rifl);
            });
        });

        // the set of all rifls should be empty
        if !all_rifls.is_empty() {
            panic!("the set of all rifls should be empty");
        }

        // return sorted commands
        sorted
    }
}
