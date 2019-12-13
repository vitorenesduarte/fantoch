use crate::client::Client;
use crate::command::{Command, CommandResult};
use crate::id::{ClientId, ProcessId};
use crate::planet::Region;
use crate::protocol::{Process, ToSend};
use crate::stats::Stats;
use crate::time::SysTime;
use std::cell::Cell;
use std::collections::HashMap;

pub struct Router<P> {
    processes: HashMap<ProcessId, Cell<P>>,
    clients: HashMap<ClientId, Cell<Client>>,
}

impl<P> Router<P>
where
    P: Process,
{
    /// Create a new `Router`.
    pub fn new() -> Self {
        Self {
            processes: HashMap::new(),
            clients: HashMap::new(),
        }
    }

    /// Registers a new process in the `Router` by storing it in a `Cell`.
    /// - from this call onwards, the process can be mutated through this `Router` by borrowing it
    ///   mutabily, as done in the route methods.
    pub fn register_process(&mut self, process: P) {
        // get identifier
        let id = process.id();
        // insert it
        let res = self.processes.insert(id, Cell::new(process));
        // check it has never been inserted before
        assert!(res.is_none())
    }

    /// Registers a `Client` process in the `Router` by storing it in a `Cell`.
    /// - from this call onwards, the process can be mutated through this `Router` by borrowing it
    ///   mutabily, as done in the route methods.
    pub fn register_client(&mut self, client: Client) {
        // get identifier
        let id = client.id();
        // insert it
        let res = self.clients.insert(id, Cell::new(client));
        // check it has never been inserted before
        assert!(res.is_none())
    }

    /// Returns an iterator of mutable references to each registered client.
    pub fn start_clients(&mut self, time: &dyn SysTime) -> Vec<(Region, ToSend<P::Message>)> {
        self.clients
            .iter_mut()
            .map(|(_, client)| {
                let client = client.get_mut();
                // get client region
                // TODO can we avoid cloning here?
                let client_region = client.region().clone();
                // start client
                let (process_id, cmd) = client.start(time);
                // create `ToSend`
                let to_send = ToSend::ToCoordinator(process_id, cmd);
                (client_region, to_send)
            })
            .collect()
    }

    /// Route a message to some target.
    pub fn route(
        &mut self,
        to_send: ToSend<P::Message>,
        time: &dyn SysTime,
    ) -> Vec<ToSend<P::Message>> {
        match to_send {
            ToSend::ToCoordinator(process_id, cmd) => {
                let to_send = self.process_submit(process_id, cmd);
                vec![to_send]
            }
            ToSend::ToProcesses(procs, msg) => procs
                .into_iter()
                .map(|process_id| self.route_to_process(process_id, msg.clone()))
                .collect(),
            ToSend::ToClients(results) => results
                .into_iter()
                .map(|cmd_result| {
                    // get client id
                    let client_id = cmd_result.rifl().source();
                    self.route_to_client(client_id, cmd_result, time)
                })
                .collect(),
            ToSend::Nothing => vec![],
        }
    }

    /// Route a message to some process.
    pub fn route_to_process(
        &mut self,
        process_id: ProcessId,
        msg: P::Message,
    ) -> ToSend<P::Message> {
        self.processes
            .get_mut(&process_id)
            .unwrap_or_else(|| {
                panic!("proc {} should have been set before", process_id);
            })
            .get_mut()
            .handle(msg)
    }

    /// Submit a command to some process.
    pub fn process_submit(&mut self, process_id: ProcessId, cmd: Command) -> ToSend<P::Message> {
        self.processes
            .get_mut(&process_id)
            .unwrap_or_else(|| {
                panic!("proc {} should have been set before", process_id);
            })
            .get_mut()
            .submit(cmd)
    }

    /// Route a message to some client.
    pub fn route_to_client(
        &mut self,
        client_id: ClientId,
        cmd_result: CommandResult,
        time: &dyn SysTime,
    ) -> ToSend<P::Message> {
        // route command result
        self.clients
            .get_mut(&client_id)
            .unwrap_or_else(|| {
                panic!("client {} should have been set before", client_id);
            })
            .get_mut()
            .handle(cmd_result, time)
            .map_or(ToSend::Nothing, |(process_id, cmd)| {
                ToSend::ToCoordinator(process_id, cmd)
            })
    }

    /// Retrieves client's stats.
    /// TODO does this need to be mut?
    pub fn client_stats(&mut self, client_id: ClientId) -> Stats {
        self.clients
            .get_mut(&client_id)
            .unwrap_or_else(|| {
                panic!("client {} should have been set before", client_id);
            })
            .get_mut()
            .stats()
    }
}
