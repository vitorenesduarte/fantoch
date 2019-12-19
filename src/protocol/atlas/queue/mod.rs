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

    /// Add a new command with its clock to the queue.
    #[must_use]
    #[allow(dead_code)]
    pub fn add(&mut self, dot: Dot, cmd: Command, clock: VClock<ProcessId>) -> Vec<Command> {
        // create new vertex for this command
        let vertex = Vertex::new(dot, cmd, clock);

        // index vertex
        self.index(vertex);

        // find a new scc
        let keys = self.find_scc(dot);

        //
        vec![]
    }

    fn index(&mut self, vertex: Vertex) {
        // index in pending index
        self.pending_index.index(&vertex);

        // index in vertex index and check if it hasn't been indexed before
        assert!(self.vertex_index.index(vertex));
    }

    fn find_scc(&mut self, dot: Dot) -> HashSet<Key> {
        // execute tarjan's algorithm
        let mut finder = TarjanSCCFinder::new();
        let finder_result = finder.strong_connect(dot, &self.executed_clock, &self.vertex_index);

        // create set of keys in ready SCCs
        let mut keys = HashSet::new();

        if finder_result == FinderResult::Found {
            finder
                .finalize(&self.vertex_index)
                .into_iter()
                .for_each(|scc| {
                    self.save_scc(scc, &mut keys);
                });
        }

        keys
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
}
