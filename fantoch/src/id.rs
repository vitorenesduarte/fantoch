use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// process ids
pub type ProcessId = u8;
pub type Dot = Id<ProcessId>;
pub type DotGen = IdGen<ProcessId>;
pub type AtomicDotGen = AtomicIdGen<ProcessId>;

// client ids
// for info on RIFL see: http://sigops.org/sosp/sosp15/current/2015-Monterey/printable/126-lee.pdf
pub type ClientId = u64;
pub type Rifl = Id<ClientId>;
pub type RiflGen = IdGen<ClientId>;

// shard ids
pub type ShardId = u64;

#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct Id<S> {
    source: S,
    sequence: u64,
}

impl<S> Id<S>
where
    S: Copy,
{
    /// Creates a new identifier Id.
    pub fn new(source: S, sequence: u64) -> Self {
        Self { source, sequence }
    }

    /// Retrieves the source that created this `Id`.
    pub fn source(&self) -> S {
        self.source
    }

    /// Retrieves the sequence consumed by this `Id`.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
}

impl<S> fmt::Debug for Id<S>
where
    S: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({:?}, {})", self.source, self.sequence)
    }
}

impl Dot {
    pub fn target_shard(&self, n: usize) -> ShardId {
        ((self.source() - 1) as usize / n) as ShardId
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdGen<S> {
    source: S,
    last_sequence: u64,
}

impl<S> IdGen<S>
where
    S: Copy,
{
    /// Creates a new generator of `Id`.
    pub fn new(source: S) -> Self {
        Self {
            source,
            last_sequence: 0,
        }
    }

    /// Retrieves source.
    pub fn source(&self) -> S {
        self.source
    }

    /// Generates the next `Id`.
    pub fn next_id(&mut self) -> Id<S> {
        self.last_sequence += 1;
        Id::new(self.source, self.last_sequence)
    }
}

#[derive(Clone)]
pub struct AtomicIdGen<S> {
    source: S,
    last_sequence: Arc<AtomicU64>,
}

impl<S> AtomicIdGen<S>
where
    S: Copy,
{
    /// Creates a new generator of `Id`.
    pub fn new(source: S) -> Self {
        Self {
            source,
            last_sequence: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Retrieves source.
    pub fn source(&self) -> S {
        self.source
    }

    /// Generates the next `Id`.
    pub fn next_id(&self) -> Id<S> {
        // TODO can the ordering be `Ordering::Relaxed`?
        let previous = self.last_sequence.fetch_add(1, Ordering::SeqCst);
        Id::new(self.source, previous + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_id() {
        type MyGen = IdGen<u64>;

        // create id generator
        let source = 10;
        let mut gen = MyGen::new(source);

        // check source
        assert_eq!(gen.source(), source);

        // check the `id` generated for `id_count` ids
        let id_count = 100;

        for seq in 1..=id_count {
            // generate id
            let id = gen.next_id();

            // check `id`
            assert_eq!(id.source(), source);
            assert_eq!(id.sequence(), seq);
        }
    }

    #[test]
    fn atomic_next_id() {
        type MyAtomicGen = AtomicIdGen<u64>;

        // create id generator
        let source = 10;
        let gen = MyAtomicGen::new(source);

        // check source
        assert_eq!(gen.source(), source);

        // check the `id` generated for `id_count` ids
        let id_count = 100;

        for seq in 1..=id_count {
            // generate id
            let id = gen.next_id();

            // check `id`
            assert_eq!(id.source(), source);
            assert_eq!(id.sequence(), seq);
        }
    }
}
