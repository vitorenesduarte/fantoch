use crate::client::{Client, Workload};
use crate::command::{Command, CommandResult};
use crate::config::Config;
use crate::id::{ClientId, ProcessId};
use crate::planet::{Planet, Region};
use crate::protocol::{Process, ToSend};
use crate::sim::Router;
use crate::sim::Schedule;
use crate::time::SimTime;
use std::collections::HashMap;

pub enum ScheduleAction<P: Process> {
    SubmitToProc(ProcessId, Command),
    SendToProc(ProcessId, P::Message),
    SendToClient(ClientId, CommandResult),
}

pub struct Runner<P: Process> {
    planet: Planet,
    router: Router<P>,
    time: SimTime,
    schedule: Schedule<ScheduleAction<P>>,
    // mapping from process identifier to its region
    process_to_region: HashMap<ProcessId, Region>,
    // mapping from client identifier to its region
    client_to_region: HashMap<ClientId, Region>,
}

impl<P> Runner<P>
where
    P: Process,
{
    /// Create a new `Runner` from a `planet`, a `config`, and two lists of regions:
    /// - `process_regions`: list of regions where processes are located
    /// - `client_regions`: list of regions where clients are located
    pub fn new<F>(
        planet: Planet,
        config: Config,
        create_process: F,
        workload: Workload,
        process_regions: Vec<Region>,
        client_regions: Vec<Region>,
    ) -> Self
    where
        F: Fn(ProcessId, Region, Planet, Config) -> P,
    {
        // check that we have the correct number of `process_regions`
        assert_eq!(process_regions.len(), config.n());

        // create router
        let mut router = Router::new();
        let mut processes = Vec::with_capacity(config.n());

        // create processes
        let to_discover: Vec<_> = process_regions
            .into_iter()
            .enumerate()
            .map(|(process_id, region)| {
                let process_id = process_id as u64;
                // create process and save it
                let process = create_process(process_id, region.clone(), planet.clone(), config);
                processes.push(process);

                (process_id, region)
            })
            .collect();

        // create processs to region mapping
        let process_to_region = to_discover.clone().into_iter().collect();

        // register processes
        processes.into_iter().for_each(|mut process| {
            // discover
            assert!(process.discover(to_discover.clone()));
            // and register it
            router.register_process(process);
        });

        // register clients and create client to region mapping
        let client_to_region = client_regions
            .into_iter()
            .enumerate()
            .map(|(client_id, region)| {
                let client_id = client_id as u64;
                // create client
                let mut client = Client::new(client_id, region.clone(), planet.clone(), workload);
                // discover
                assert!(client.discover(to_discover.clone()));
                // and register it
                router.register_client(client);
                (client_id, region)
            })
            .collect();

        // create runner
        Self {
            planet,
            router,
            time: SimTime::new(),
            schedule: Schedule::new(),
            process_to_region,
            client_to_region,
        }
    }

    /// Run the simulation.
    pub fn run(&mut self) {
        // start clients
        self.router
            .start_clients(&self.time)
            .into_iter()
            .for_each(|(client_region, to_send)| {
                // schedule client commands
                self.try_to_schedule(client_region, to_send);
            });

        // run the simulation while there are things scheduled
        while let Some(actions) = self.schedule.next_actions(&mut self.time) {
            // for each scheduled action
            actions.into_iter().for_each(|action| {
                match action {
                    ScheduleAction::SubmitToProc(process_id, cmd) => {
                        // get process's region
                        let process_region = self.process_region(process_id);
                        // submit to process and schedule output messages
                        let to_send = self.router.submit_to_process(process_id, cmd);
                        self.try_to_schedule(process_region, to_send);
                    }
                    ScheduleAction::SendToProc(process_id, msg) => {
                        // get process's region
                        let process_region = self.process_region(process_id);
                        // route to process and schedule output messages
                        let to_send = self.router.route_to_process(process_id, msg);
                        self.try_to_schedule(process_region, to_send);
                    }
                    ScheduleAction::SendToClient(client_id, cmd_result) => {
                        // get client's region
                        let client_region = self.client_region(client_id);
                        // route to client and schedule output command
                        let to_send = self
                            .router
                            .route_to_client(client_id, cmd_result, &self.time);
                        self.try_to_schedule(client_region, to_send);
                    }
                }
            })
        }
    }

    /// Try to schedule a `ToSend`. When scheduling, we shoud never route!
    fn try_to_schedule(&mut self, from: Region, to_send: ToSend<P::Message>) {
        match to_send {
            ToSend::ToCoordinator(process_id, cmd) => {
                // create action and schedule it
                let action = ScheduleAction::SubmitToProc(process_id, cmd);
                // get process's region
                let to = self.process_region(process_id);
                self.schedule_it(&from, &to, action);
            }
            ToSend::ToProcesses(target, msg) => {
                // for each process in target, schedule message delivery
                target.into_iter().for_each(|process_id| {
                    // create action and schedule it
                    let action = ScheduleAction::SendToProc(process_id, msg.clone());
                    // get process's region
                    let to = self.process_region(process_id);
                    self.schedule_it(&from, &to, action);
                });
            }
            ToSend::ToClients(cmd_results) => {
                // for each command result, schedule its delivery
                cmd_results.into_iter().for_each(|cmd_result| {
                    // create action and schedule it
                    let client_id = cmd_result.rifl().source();
                    let action = ScheduleAction::SendToClient(client_id, cmd_result);
                    // get client's region
                    let to = self.client_region(client_id);
                    self.schedule_it(&from, &to, action);
                });
            }
            ToSend::Nothing => {
                // nothing to do
            }
        }
    }

    fn schedule_it(&mut self, from: &Region, to: &Region, action: ScheduleAction<P>) {
        // compute distance between regions and schedule action
        let distance = self.distance(from, to);
        self.schedule.schedule(&self.time, distance, action);
    }

    /// Retrieves the region of process with identifier `process_id`.
    // TODO can we avoid cloning here?
    fn process_region(&self, process_id: ProcessId) -> Region {
        self.process_to_region
            .get(&process_id)
            .expect("process region should be known")
            .clone()
    }

    /// Retrieves the region of client with identifier `client_id`.
    // TODO can we avoid cloning here?
    fn client_region(&self, client_id: ClientId) -> Region {
        self.client_to_region
            .get(&client_id)
            .expect("client region should be known")
            .clone()
    }

    /// Computes the distance between two regions.
    fn distance(&self, from: &Region, to: &Region) -> u64 {
        self.planet
            .latency(from, to)
            .expect("both regions should exist on the planet")
    }
}
