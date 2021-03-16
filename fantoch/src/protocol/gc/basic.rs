use crate::id::Dot;
use crate::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasicGCTrack {
    n: usize,
    dot_to_count: HashMap<Dot, usize>,
}

impl BasicGCTrack {
    pub fn new(n: usize) -> Self {
        Self {
            n,
            dot_to_count: HashMap::new(),
        }
    }

    /// Records this command, returning a bool indicating whethe it is stable.
    #[must_use]
    pub fn record(&mut self, dot: Dot) -> bool {
        let count = self.dot_to_count.entry(dot).or_default();
        *count += 1;
        if *count == self.n {
            self.dot_to_count.remove(&dot);
            true
        } else {
            false
        }
    }
}
