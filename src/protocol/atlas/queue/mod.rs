// This module contains the definition of `TarjanSCCFinder` and `FinderResult`.
mod tarjan;

/// This module contains the definition of `VertexIndex` and `PendingIndex`.
mod index;

use crate::command::Command;
use crate::id::{Dot, ProcessId};
use crate::kvs::Key;
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
    #[must_use]
    pub fn to_execute(&mut self) -> Vec<Command> {
        let mut ready = Vec::new();
        mem::swap(&mut ready, &mut self.to_execute);
        ready
    }

    /// Add a new command with its clock to the queue.
    #[allow(dead_code)]
    pub fn add(&mut self, dot: Dot, cmd: Command, clock: VClock<ProcessId>) {
        println!("Queue::add {:?} {:?}", dot, clock);
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

    #[test]
    fn simple() {
        // create key
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
}
