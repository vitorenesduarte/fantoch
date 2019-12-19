use crate::id::Dot;
use crate::kvs::Key;
use crate::protocol::atlas::queue::Vertex;
use std::cell::UnsafeCell;
use std::collections::{HashMap, HashSet};

#[derive(Default)]
pub struct VertexIndex {
    index: HashMap<Dot, UnsafeCell<Vertex>>,
}

impl VertexIndex {
    pub fn new() -> Self {
        Default::default()
    }

    /// Indexes a new vertex, returning whether a vertex with this dot was already indexed or not.
    pub fn index(&mut self, vertex: Vertex) -> bool {
        let res = self.index.insert(vertex.dot(), UnsafeCell::new(vertex));
        res.is_none()
    }

    /// Returns a reference to an indexed vertex (if previously indexed).
    pub fn get(&self, dot: &Dot) -> Option<&Vertex> {
        // TODO is this unsafe needed?
        self.index.get(dot).map(|cell| unsafe { &*cell.get() })
    }

    /// Returns a mutable reference to an indexed vertex (if previously indexed).
    pub fn get_mut(&self, dot: &Dot) -> Option<&mut Vertex> {
        self.index.get(dot).map(|cell| unsafe { &mut *cell.get() })
    }

    /// Removes a vertex from the index.
    pub fn remove(&mut self, dot: &Dot) -> Option<Vertex> {
        self.index.remove(dot).map(|cell| cell.into_inner())
    }
}

#[derive(Default)]
pub struct PendingIndex {
    index: HashMap<Key, HashSet<Dot>>,
}

impl PendingIndex {
    pub fn new() -> Self {
        Default::default()
    }

    /// Indexes a new vertex.
    pub fn index(&mut self, vertex: &Vertex) {
        self.update_index(vertex, |pending| {
            // add to pending and check it was not there
            assert!(pending.insert(vertex.dot()));
        });
    }

    /// Finds all pending dots for a given key.
    pub fn pending(&self, key: &Key) -> Option<HashSet<Dot>> {
        self.index.get(key).cloned()
    }

    /// Removes a vertex from the index.
    pub fn remove(&mut self, vertex: &Vertex) {
        self.update_index(vertex, |pending| {
            // remove from pending and check it was there
            assert!(pending.remove(&vertex.dot()));
        });
    }

    /// Generic function to be used to update a set of pending commands associated with each color
    /// touched by a given command.
    fn update_index<F>(&mut self, vertex: &Vertex, mut update: F)
    where
        F: FnMut(&mut HashSet<Dot>),
    {
        vertex.command().keys().for_each(|key| {
            // get current set of pending commands for this key
            let pending = match self.index.get_mut(key) {
                Some(pending) => pending,
                None => self.index.entry(key.clone()).or_insert(HashSet::new()),
            };
            // update pending
            update(pending)
        });
    }
}
