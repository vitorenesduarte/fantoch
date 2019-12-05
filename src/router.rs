use crate::base::ProcId;
use crate::client::{Client, ClientId};
use crate::command::{Command, CommandResult};
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

    /// Registers a `Newt` process in the `Router` by storing it in a `Cell`.
    /// - from this call onwards, the process can be mutated through this
    ///   `Router` by borrowing it mutabily, as done in the route methods.
    pub fn register_proc(&mut self, newt: Newt) {
        // get identifier
        let id = newt.id();
        // insert it
        let res = self.procs.insert(id, Cell::new(newt));
        // check it has never been inserted before
        assert!(res.is_none())
    }

    /// Registers a `Client` process in the `Router` by storing it in a `Cell`.
    /// - from this call onwards, the process can be mutated through this
    ///   `Router` by borrowing it mutabily, as done in the route methods.
    pub fn register_client(&mut self, client: Client) {
        // get identifier
        let id = client.id();
        // insert it
        let res = self.clients.insert(id, Cell::new(client));
        // check it has never been inserted before
        assert!(res.is_none())
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
                .filter_map(|cmd_result| {
                    let client = cmd_result.rifl().source();
                    self.route_to_client(client, cmd_result)
                })
                .map(|(proc_id, cmd)| {
                    ToSend::Procs(Message::Submit { cmd }, vec![proc_id])
                })
                .collect(),
        }
    }

    /// Route a message to some process.
    pub fn route_to_proc(&mut self, proc_id: ProcId, msg: Message) -> ToSend {
        self.procs
            .get_mut(&proc_id)
            .unwrap_or_else(|| {
                panic!("proc {} should have been set before", proc_id);
            })
            .get_mut()
            .handle(msg)
    }

    /// Route a message to some client.
    pub fn route_to_client(
        &mut self,
        client_id: ClientId,
        cmd_result: CommandResult,
    ) -> Option<(ProcId, Command)> {
        self.clients
            .get_mut(&client_id)
            .unwrap_or_else(|| {
                panic!("client {} should have been set before", client_id);
            })
            .get_mut()
            .handle(cmd_result)
    }
}
