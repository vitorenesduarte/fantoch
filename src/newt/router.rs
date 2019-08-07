use crate::base::ProcId;
use crate::newt::{Message, Newt, ToSend};
use std::cell::RefCell;
use std::collections::HashMap;

pub struct Router {
    procs: HashMap<ProcId, RefCell<Newt>>,
}

impl Router {
    /// Create a new `Router`.
    pub fn new() -> Self {
        Router {
            procs: HashMap::new(),
        }
    }

    /// Set a `Newt` process in the `Router` by storing it in a `RefCell`.
    /// - from this call onwards, the process can be mutated through this
    ///   `Router` by borrowing it mutabily, as done in the route methods.
    pub fn set_proc(&mut self, proc_id: ProcId, newt: Newt) {
        self.procs.insert(proc_id, RefCell::new(newt));
    }

    /// Route a message to some process.
    pub fn route(&self, proc_id: &ProcId, msg: Message) -> ToSend {
        let mut newt = self.procs.get(proc_id).unwrap().borrow_mut();
        newt.handle(msg)
    }

    /// Route a message to some target.
    pub fn route_to_many(&self, to_send: ToSend) -> Vec<ToSend> {
        if let Some((msg, target)) = to_send {
            target
                .into_iter()
                .map(|proc_id| self.route(&proc_id, msg.clone()))
                .collect()
        } else {
            vec![]
        }
    }
}
