use crate::base::{Client, ClientId, ProcId, Rifl};
use crate::command::{MultiCommand, MultiCommandResult};
use crate::newt::{Message, Newt, ToSend};
use std::cell::RefCell;
use std::collections::HashMap;

pub struct Router {
    procs: HashMap<ProcId, RefCell<Newt>>,
    clients: HashMap<ClientId, RefCell<Client>>,
}

impl Router {
    /// Create a new `Router`.
    pub fn new() -> Self {
        Router {
            procs: HashMap::new(),
            clients: HashMap::new(),
        }
    }

    /// Set a `Newt` process in the `Router` by storing it in a `RefCell`.
    /// - from this call onwards, the process can be mutated through this
    ///   `Router` by borrowing it mutabily, as done in the route methods.
    pub fn set_proc(&mut self, proc_id: ProcId, newt: Newt) {
        self.procs.insert(proc_id, RefCell::new(newt));
    }

    /// Set a `Client` process in the `Router` by storing it in a `RefCell`.
    /// - from this call onwards, the process can be mutated through this
    ///   `Router` by borrowing it mutabily, as done in the route methods.
    pub fn set_client(&mut self, client_id: ClientId, client: Client) {
        self.clients.insert(client_id, RefCell::new(client));
    }

    /// Route a message to some target.
    pub fn route(&self, to_send: ToSend) -> Vec<ToSend> {
        match to_send {
            ToSend::Nothing => vec![],
            ToSend::Procs(msg, target) => target
                .into_iter()
                .map(|proc_id| self.route_to_proc(proc_id, msg.clone()))
                .collect(),
            ToSend::Clients(results) => results
                .into_iter()
                .map(|(client, commands)| {
                    self.route_to_client(client, commands)
                })
                .flat_map(|(proc_id, new_commands)| {
                    new_commands.into_iter().map(move |cmd| {
                        ToSend::Procs(Message::Submit { cmd }, vec![proc_id])
                    })
                })
                .collect(),
        }
    }

    /// Route a message to some process.
    pub fn route_to_proc(&self, proc_id: ProcId, msg: Message) -> ToSend {
        let mut newt = self.procs.get(&proc_id).unwrap().borrow_mut();
        newt.handle(msg)
    }

    /// Route a message to some client.
    pub fn route_to_client(
        &self,
        client_id: ClientId,
        commands: Vec<(Rifl, MultiCommandResult)>,
    ) -> (ProcId, Vec<MultiCommand>) {
        let mut client = self.clients.get(&client_id).unwrap().borrow_mut();
        client.handle(commands)
    }
}
