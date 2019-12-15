use crate::client::Client;
use crate::command::{CommandResult};
use crate::id::{ClientId, ProcessId};
use crate::protocol::{Process, ToSend};
use crate::time::SysTime;
use std::cell::Cell;
use std::collections::HashMap;

pub struct Simulation<P> {
    processes: HashMap<ProcessId, Cell<P>>,
    clients: HashMap<ClientId, Cell<Client>>,
}

impl<P> Simulation<P>
where
    P: Process,
{
    /// Create a new `Simulation`.
    pub fn new() -> Self {
        Simulation {
            processes: HashMap::new(),
            clients: HashMap::new(),
        }
    }

    /// Registers a new process in the `Simulation` by storing it in a `Cell`.
    /// - from this call onwards, the process can be mutated through this `Simulation` by borrowing
    ///   it mutabily, as done in the route methods.
    pub fn register_process(&mut self, process: P) {
        // get identifier
        let id = process.id();
        // insert it
        let res = self.processes.insert(id, Cell::new(process));
        // check it has never been inserted before
        assert!(res.is_none())
    }

    /// Registers a `Client` process in the `Simulation` by storing it in a `Cell`.
    /// - from this call onwards, the process can be mutated through this `Simulation` by borrowing
    ///   it mutabily, as done in the route methods.
    pub fn register_client(&mut self, client: Client) {
        // get identifier
        let id = client.id();
        // insert it
        let res = self.clients.insert(id, Cell::new(client));
        // check it has never been inserted before
        assert!(res.is_none())
    }

    /// Starts all clients registered in the router.
    pub fn start_clients(&mut self, time: &dyn SysTime) -> Vec<(ClientId, ToSend<P::Message>)> {
        self.clients
            .iter_mut()
            .map(|(_, client)| {
                let client = client.get_mut();
                // start client
                let (process_id, cmd) = client.start(time);
                // create `ToSend`
                let to_send = ToSend::ToCoordinator(process_id, cmd);
                (client.id(), to_send)
            })
            .collect()
    }

    /// Forward a `ToSend`.
    pub fn forward_to_processes(&mut self, to_send: ToSend<P::Message>) -> Vec<ToSend<P::Message>> {
        match to_send {
            ToSend::ToCoordinator(process_id, cmd) => {
                let to_send = self.get_process(process_id).submit(cmd);
                vec![to_send]
            }
            ToSend::ToProcesses(from, processes, msg) => processes
                .into_iter()
                .map(|process_id| self.get_process(process_id).handle(from, msg.clone()))
                .collect(),
            ToSend::Nothing => vec![],
        }
    }

    /// Forward a list of `CommandResult`.
    pub fn forward_to_clients(
        &mut self,
        results: Vec<CommandResult>,
        time: &dyn SysTime,
    ) -> Vec<ToSend<P::Message>> {
        results
            .into_iter()
            .map(|cmd_result| {
                // get client id
                let client_id = cmd_result.rifl().source();
                self.forward_to_client(client_id, cmd_result, time)
            })
            .collect()
    }

    /// Forward a `CommandResult` to some client.
    pub fn forward_to_client(
        &mut self,
        client_id: ClientId,
        cmd_result: CommandResult,
        time: &dyn SysTime,
    ) -> ToSend<P::Message> {
        // route command result
        self.get_client(client_id)
            .handle(cmd_result, time)
            .map_or(ToSend::Nothing, |(process_id, cmd)| {
                ToSend::ToCoordinator(process_id, cmd)
            })
    }

    /// Returns the process registered with this identifier.
    /// It panics if the process is not registered.
    pub fn get_process(&mut self, process_id: ProcessId) -> &mut P {
        self.processes
            .get_mut(&process_id)
            .unwrap_or_else(|| {
                panic!("proc {} should have been set before", process_id);
            })
            .get_mut()
    }

    /// Returns the client registered with this identifier.
    /// It panics if the client is not registered.
    pub fn get_client(&mut self, client_id: ClientId) -> &mut Client {
        self.clients
            .get_mut(&client_id)
            .unwrap_or_else(|| {
                panic!("client {} should have been set before", client_id);
            })
            .get_mut()
    }
}
