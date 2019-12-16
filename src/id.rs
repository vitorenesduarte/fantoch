use std::fmt;

// process ids
pub type ProcessId = u64;
pub type Dot = Id<ProcessId>;
pub type DotGen = IdGen<ProcessId>;

// client ids
// for info on RIFL see: http://sigops.org/sosp/sosp15/current/2015-Monterey/printable/126-lee.pdf
pub type ClientId = u64;
pub type Rifl = Id<ClientId>;
pub type RiflGen = IdGen<ClientId>;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Id<S> {
    source: S,
    seq: u64,
}

impl<S> Id<S>
where
    S: Copy,
{
    /// Creates a new identifier Id.
    pub fn new(source: S, seq: u64) -> Self {
        Self { source, seq }
    }

    /// Retrieves the source that created this `Id`.
    pub fn source(&self) -> S {
        self.source
    }
}

impl<S> fmt::Debug for Id<S>
where
    S: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({:?}, {})", self.source, self.seq)
    }
}

pub struct IdGen<S> {
    source: S,
    last_seq: u64,
}

impl<S> IdGen<S>
where
    S: Copy,
{
    /// Creates a new generator of `Id`.
    pub fn new(source: S) -> Self {
        Self {
            source,
            last_seq: 0,
        }
    }

    /// Retrives source.
    pub fn source(&self) -> S {
        self.source
    }

    /// Generates the next `Id`.
    pub fn next_id(&mut self) -> Id<S> {
        self.last_seq += 1;
        Id::new(self.source, self.last_seq)
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

        // generate `n` ids and check the `id` generated
        let n = 100;

        for seq in 1..=n {
            // generate id
            let id = gen.next_id();

            // check `id`
            assert_eq!(id.source, source);
            assert_eq!(id.seq, seq);
        }
    }
}
