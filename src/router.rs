use crate::base::ProcId;
use crate::client::{Client, ClientId, Rifl};
use crate::command::{MultiCommand, MultiCommandResult};
use crate::newt::{Message, Newt, ToSend};
use std::cell::Cell;
use std::collections::HashMap;

#[allow(dead_code)] // TODO remove me
pub struct Router {
    procs: HashMap<ProcId, Cell<Newt>>,
    clients: HashMap<ClientId, Cell<Client>>,
}

#[allow(dead_code)] // TODO remove me
impl Router {
    /// Create a new `Router`.
    pub fn new() -> Self {
        Router {
            procs: HashMap::new(),
            clients: HashMap::new(),
        }
    }

    /// Set a `Newt` process in the `Router` by storing it in a `Cell`.
    /// - from this call onwards, the process can be mutated through this
    ///   `Router` by borrowing it mutabily, as done in the route methods.
    pub fn set_proc(&mut self, proc_id: ProcId, newt: Newt) {
        self.procs.insert(proc_id, Cell::new(newt));
    }

    /// Set a `Client` process in the `Router` by storing it in a `Cell`.
    /// - from this call onwards, the process can be mutated through this
    ///   `Router` by borrowing it mutabily, as done in the route methods.
    pub fn set_client(&mut self, client_id: ClientId, client: Client) {
        self.clients.insert(client_id, Cell::new(client));
    }

    /// Route a message to some target.
    pub fn route(&mut self, to_send: ToSend) -> Vec<ToSend> {
        match to_send {
            ToSend::Nothing => vec![],
            ToSend::Procs(msg, target) => target
                .into_iter()
                .map(|proc_id| self.route_to_proc(proc_id, msg.clone()))
                .collect(),
            ToSend::Clients(results) => results
                .into_iter()
                .filter_map(|(client, commands)| {
                    self.route_to_client(client, commands)
                })
                .map(|(proc_id, cmd)| {
                    ToSend::Procs(Message::Submit { cmd }, vec![proc_id])
                })
                .collect(),
        }
    }

    /// Route a message to some process.
    pub fn route_to_proc(&mut self, proc_id: ProcId, msg: Message) -> ToSend {
        let newt = self.procs.get_mut(&proc_id).unwrap().get_mut();
        newt.handle(msg)
    }

    /// Route a message to some client.
    pub fn route_to_client(
        &mut self,
        client_id: ClientId,
        commands: Vec<(Rifl, MultiCommandResult)>,
    ) -> Option<(ProcId, MultiCommand)> {
        let client = self.clients.get_mut(&client_id).unwrap().get_mut();
        client.handle(commands)
    }
}
