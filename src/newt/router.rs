use crate::base::ProcId;
use crate::newt::{Message, Newt, ToSend};
use std::cell::RefCell;
use std::collections::HashMap;

pub struct Router {
    procs: HashMap<ProcId, RefCell<Newt>>,
}

impl Router {
    pub fn new() -> Self {
        Router {
            procs: HashMap::new(),
        }
    }

    pub fn set_proc(&mut self, proc_id: ProcId, newt: Newt) {
        self.procs.insert(proc_id, RefCell::new(newt));
    }

    pub fn route(&self, proc_id: &ProcId, msg: Message) -> ToSend {
        let mut newt = self.procs.get(proc_id).unwrap().borrow_mut();
        newt.handle(msg)
    }

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
