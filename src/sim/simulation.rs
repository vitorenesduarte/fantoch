use crate::client::Client;
use crate::command::{Command, CommandResult};
use crate::id::{ClientId, ProcessId};
use crate::protocol::{Process, ToSend};
use crate::time::SysTime;
use std::cell::Cell;
use std::collections::HashMap;

pub struct Simulation<P: Process> {
    processes: HashMap<ProcessId, Cell<P>>,
    executors: HashMap<ProcessId, Cell<P::Executor>>,
    clients: HashMap<ClientId, Cell<Client>>,
}

impl<P> Simulation<P>
where
    P: Process,
{
    /// Create a new `Simulation`.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Simulation {
            processes: HashMap::new(),
            executors: HashMap::new(),
            clients: HashMap::new(),
        }
    }

    /// Registers a `Process` in the `Simulation` by storing it in a `Cell`.
    pub fn register_process(&mut self, process: P, executor: P::Executor) {
        // get identifier
        let id = process.id();

        // register process and check it has never been registered before
        let res = self.processes.insert(id, Cell::new(process));
        assert!(res.is_none());

        // register executor and check it has never been registered before
        let res = self.executors.insert(id, Cell::new(executor));
        assert!(res.is_none());
    }

    /// Registers a `Client` in the `Simulation` by storing it in a `Cell`.
    pub fn register_client(&mut self, client: Client) {
        // get identifier
        let id = client.id();

        // register client and check it has never been registerd before
        let res = self.clients.insert(id, Cell::new(client));
        assert!(res.is_none());
    }

    /// Starts all clients registered in the router.
    pub fn start_clients(
        &mut self,
        time: &dyn SysTime,
    ) -> Vec<(ClientId, Option<(ProcessId, Command)>)> {
        self.clients
            .iter_mut()
            .map(|(_, client)| {
                let client = client.get_mut();
                // start client
                let submit = client.start(time);
                (client.id(), submit)
            })
            .collect()
    }

    /// Forward a `ToSend`.
    pub fn forward_to_processes(&mut self, to_send: ToSend<P::Message>) -> Vec<ToSend<P::Message>> {
        // extract `ToSend` arguments
        let ToSend { from, target, msg } = to_send;
        target
            .into_iter()
            .filter_map(|process_id| self.get_process(process_id).handle(from, msg.clone()))
            .collect()
    }

    /// Forward a `CommandResult`.
    pub fn forward_to_client(
        &mut self,
        cmd_result: CommandResult,
        time: &dyn SysTime,
    ) -> Option<(ProcessId, Command)> {
        let client_id = cmd_result.rifl().source();
        self.get_client(client_id).handle(cmd_result, time)
    }

    /// Returns the process registered with this identifier.
    /// It panics if the process is not registered.
    pub fn get_process(&mut self, process_id: ProcessId) -> &mut P {
        self.processes
            .get_mut(&process_id)
            .unwrap_or_else(|| {
                panic!("process {} should have been registered before", process_id);
            })
            .get_mut()
    }

    /// Returns the executor registered with this identifier.
    /// It panics if the executor is not registered.
    pub fn get_executor(&mut self, process_id: ProcessId) -> &mut P::Executor {
        self.executors
            .get_mut(&process_id)
            .unwrap_or_else(|| {
                panic!("executor {} should have been registered before", process_id);
            })
            .get_mut()
    }

    /// Returns the client registered with this identifier.
    /// It panics if the client is not registered.
    pub fn get_client(&mut self, client_id: ClientId) -> &mut Client {
        self.clients
            .get_mut(&client_id)
            .unwrap_or_else(|| {
                panic!("client {} should have been registered before", client_id);
            })
            .get_mut()
    }
}
