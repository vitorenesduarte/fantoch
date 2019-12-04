#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

    /// Generates the next `Id`.
    pub fn next(&mut self) -> Id<S> {
        self.last_seq += 1;
        Id::new(self.source, self.last_seq)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next() {
        type MyGen = IdGen<u64>;

        // create id generator
        let source = 10;
        let mut gen = MyGen::new(source);

        for seq in 1..10 {
            // generate id
            let id = gen.next();
            assert_eq!(id.source, source);
            assert_eq!(id.seq, seq);
        }
    }
}
