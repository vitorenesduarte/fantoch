use crate::client::{Client, Workload};
use crate::command::CommandResult;
use crate::config::Config;
use crate::id::{ClientId, ProcId};
use crate::newt::Newt;
use crate::newt::{Message, ToSend};
use crate::planet::{Planet, Region};
use crate::sim::Router;
use crate::sim::Schedule;
use crate::time::SimTime;
use std::collections::HashMap;

pub enum ScheduleAction {
    SendToProc(ProcId, Message),
    SendToClient(ClientId, CommandResult),
}

pub struct Runner {
    planet: Planet,
    router: Router,
    time: SimTime,
    schedule: Schedule<ScheduleAction>,
    // mapping from process identifier to its region
    proc_to_region: HashMap<ProcId, Region>,
    // mapping from client identifier to its region
    client_to_region: HashMap<ClientId, Region>,
}

impl Runner {
    /// Create a new `Runner` from a `planet`, a `config`, and two lists of regions:
    /// - `proc_regions`: list of regions where processes are located
    /// - `client_regions`: list of regions where clients are located
    pub fn new(
        proc_regions: Vec<Region>,
        client_regions: Vec<Region>,
        planet: Planet,
        config: Config,
        workload: Workload,
    ) -> Self {
        // check that we have the correct number of `proc_regions`
        assert_eq!(proc_regions.len(), config.n());

        // create router
        let mut router = Router::new();

        // register procs
        let procs: Vec<_> = proc_regions
            .into_iter()
            .enumerate()
            .map(|(proc_id, region)| {
                let proc_id = proc_id as u64;
                // create proc
                let newt = Newt::new(proc_id, region.clone(), planet.clone(), config);
                // and register it
                router.register_proc(newt);
                (proc_id, region)
            })
            .collect();

        // create proc to region
        let proc_to_region = procs.clone().into_iter().collect();

        // register clients
        let client_to_region = client_regions
            .into_iter()
            .enumerate()
            .map(|(client_id, region)| {
                let client_id = client_id as u64;
                // create client
                let mut client = Client::new(client_id, region.clone(), planet.clone(), workload);
                // discover `procs`
                client.discover(procs.clone());
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
            proc_to_region,
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
                self.schedule_it(client_region, to_send);
            });
        // run the simulation while there are things scheduled
        while let Some(actions) = self.schedule.next_actions(&mut self.time) {
            // for each scheduled action
            actions.into_iter().for_each(|action| {
                match action {
                    ScheduleAction::SendToProc(proc_id, msg) => {
                        // route to process
                        let to_send = self.router.route_to_proc(proc_id, msg);
                        // schedule new message from process
                        let proc_region = self.proc_region(proc_id);
                        self.schedule_it(proc_region, to_send);
                    }
                    ScheduleAction::SendToClient(client_id, cmd_result) => {
                        // route to client
                        let to_send = self
                            .router
                            .route_to_client(client_id, cmd_result, &self.time);
                        // schedule new message from client
                        let client_region = self.client_region(client_id);
                        self.schedule_it(client_region, to_send);
                    }
                }
            })
        }
    }

    /// Schedule a `ToSend`. When scheduling, we shoud never route!
    fn schedule_it(&mut self, from: Region, to_send: ToSend) {
        match to_send {
            ToSend::Procs(msg, target) => {
                // for each process in target, schedule message delivery
                target.into_iter().for_each(|proc_id| {
                    // get target's region
                    let target_region = self.proc_region(proc_id);

                    // compute distance between regions, create action and schedule it
                    let distance = self.distance(&from, &target_region);
                    let action = ScheduleAction::SendToProc(proc_id, msg.clone());
                    self.schedule.schedule(&self.time, distance, action);
                });
            }
            ToSend::Clients(cmd_results) => {
                // for each command result, schedule its delivery
                cmd_results.into_iter().for_each(|cmd_result| {
                    // get client id
                    let client_id = cmd_result.rifl().source();
                    // get target's region
                    let target_region = self.client_region(client_id);

                    // route command result to the corresponding client
                    let distance = self.distance(&from, &target_region);
                    let action = ScheduleAction::SendToClient(client_id, cmd_result);
                    self.schedule.schedule(&self.time, distance, action);
                });
            }
            ToSend::Nothing => {
                // nothing to do
            }
        }
    }

    /// Retrieves the region of process with identifier `proc_id`.
    // TODO can we avoid cloning here?
    fn proc_region(&self, proc_id: ProcId) -> Region {
        self.proc_to_region
            .get(&proc_id)
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
