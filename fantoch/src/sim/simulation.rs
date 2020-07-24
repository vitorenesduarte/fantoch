use crate::client::Client;
use crate::command::{Command, CommandResult};
use crate::id::{ClientId, ProcessId};
use crate::protocol::{Action, Protocol};
use crate::time::SimTime;
use crate::HashMap;
use std::cell::Cell;

pub struct Simulation<P: Protocol> {
    time: SimTime,
    processes: HashMap<ProcessId, Cell<(P, P::Executor)>>,
    clients: HashMap<ClientId, Cell<Client>>,
}

impl<P> Simulation<P>
where
    P: Protocol,
{
    /// Create a new `Simulation`.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Simulation {
            time: SimTime::new(),
            processes: HashMap::new(),
            clients: HashMap::new(),
        }
    }

    // Return a mutable reference to the simulation time.
    pub fn time(&mut self) -> &mut SimTime {
        &mut self.time
    }

    /// Registers a `Process` in the `Simulation` by storing it in a `Cell`.
    pub fn register_process(&mut self, process: P, executor: P::Executor) {
        // get identifier
        let id = process.id();

        // register process and check it has never been registered before
        let res = self.processes.insert(id, Cell::new((process, executor)));
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
    pub fn start_clients(&mut self) -> Vec<(ClientId, ProcessId, Command)> {
        let time = &self.time;
        self.clients
            .iter_mut()
            .map(|(_, client)| {
                let client = client.get_mut();
                // start client
                let (target_shard, cmd) = client
                    .next_cmd(time)
                    .expect("clients should submit at least one command");
                let process_id = client.shard_process(&target_shard);
                (client.id(), process_id, cmd)
            })
            .collect()
    }

    /// Forward a `ToSend`.
    pub fn forward_to_processes(
        &mut self,
        (process_id, action): (ProcessId, Action<P>),
    ) -> Vec<(ProcessId, Action<P>)> {
        match action {
            Action::ToSend { target, msg } => {
                // get self process and its shard id
                let (process, _, time) = self.get_process(process_id);
                assert_eq!(process.id(), process_id);
                let shard_id = process.shard_id();

                // handle first in self if self in target
                let local_actions = if target.contains(&process_id) {
                    // handle msg
                    process.handle(process_id, shard_id, msg.clone(), time)
                } else {
                    vec![]
                };

                let actions = target
                    .into_iter()
                    // make sure we don't handle again in self
                    .filter(|to| to != &process_id)
                    .flat_map(|to| {
                        // get target process
                        let (to_process, _, time) = self.get_process(to);
                        assert_eq!(to_process.id(), to);

                        // handle msg
                        to_process
                            .handle(process_id, shard_id, msg.clone(), time)
                            .into_iter()
                            .map(move |action| (to, action))
                    });

                // make sure that the first to_send is the one from self
                local_actions
                    .into_iter()
                    .map(|action| (process_id, action))
                    .chain(actions)
                    .collect()
            }
            action => {
                panic!("non supported action: {:?}", action);
            }
        }
    }

    /// Forward a `CommandResult`.
    pub fn forward_to_client(
        &mut self,
        cmd_result: CommandResult,
    ) -> Option<(ProcessId, Command)> {
        // get client id
        let client_id = cmd_result.rifl().source();
        // find client
        let (client, time) = self.get_client(client_id);
        // handle command result
        // TODO: we should aggregate command results if we have more than one
        // shard in simulation
        client.handle(vec![cmd_result], time);
        // and generate the next command
        client.next_cmd(time).map(|(target_shard, cmd)| {
            let target = client.shard_process(&target_shard);
            (target, cmd)
        })
    }

    /// Returns the process registered with this identifier.
    /// It panics if the process is not registered.
    pub fn get_process(
        &mut self,
        process_id: ProcessId,
    ) -> (&mut P, &mut P::Executor, &SimTime) {
        let (process, executor) = self
            .processes
            .get_mut(&process_id)
            .unwrap_or_else(|| {
                panic!(
                    "process {} should have been registered before",
                    process_id
                );
            })
            .get_mut();
        (process, executor, &self.time)
    }

    /// Returns the client registered with this identifier.
    /// It panics if the client is not registered.
    pub fn get_client(
        &mut self,
        client_id: ClientId,
    ) -> (&mut Client, &SimTime) {
        let client = self
            .clients
            .get_mut(&client_id)
            .unwrap_or_else(|| {
                panic!(
                    "client {} should have been registered before",
                    client_id
                );
            })
            .get_mut();
        (client, &self.time)
    }
}
