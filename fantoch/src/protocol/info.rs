use crate::id::{Dot, ProcessId};
use std::collections::HashMap;

pub trait Info {
    fn new(
        process_id: ProcessId,
        n: usize,
        f: usize,
        fast_quorum_size: usize,
    ) -> Self;
}

// `CommandsInfo` contains `CommandInfo` for each `Dot`.
#[derive(Clone)]
pub struct CommandsInfo<I> {
    process_id: ProcessId,
    n: usize,
    f: usize,
    fast_quorum_size: usize,
    dot_to_info: HashMap<Dot, I>,
}

impl<I> CommandsInfo<I>
where
    I: Info,
{
    pub fn new(
        process_id: ProcessId,
        n: usize,
        f: usize,
        fast_quorum_size: usize,
    ) -> Self {
        Self {
            process_id,
            n,
            f,
            fast_quorum_size,
            dot_to_info: HashMap::new(),
        }
    }

    // Returns the `Info` associated with `Dot`.
    // If no `Info` is associated, an empty `Info` is returned.
    pub fn get(&mut self, dot: Dot) -> &mut I {
        // TODO borrow everything we need so that the borrow checker does not
        // complain
        let process_id = self.process_id;
        let n = self.n;
        let f = self.f;
        let fast_quorum_size = self.fast_quorum_size;
        self.dot_to_info
            .entry(dot)
            .or_insert_with(|| I::new(process_id, n, f, fast_quorum_size))
    }

    // Remove `Info` associated with `Dot`.
    pub fn remove(&mut self, dot: Dot) {
        self.dot_to_info.remove(&dot);
    }
}
