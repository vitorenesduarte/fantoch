use crate::client::{Client, Workload};
use crate::command::{Command, CommandResult};
use crate::config::Config;
use crate::id::{ClientId, ProcId};
use crate::planet::{Planet, Region};
use crate::proc::{Proc, ToSend};
use crate::sim::Router;
use crate::sim::Schedule;
use crate::time::SimTime;
use std::collections::HashMap;

pub enum ScheduleAction<P: Proc> {
    SubmitToProc(ProcId, Command),
    SendToProc(ProcId, P::Message),
    SendToClient(ClientId, CommandResult),
}

pub struct Runner<P: Proc> {
    planet: Planet,
    router: Router<P>,
    time: SimTime,
    schedule: Schedule<ScheduleAction<P>>,
    // mapping from process identifier to its region
    proc_to_region: HashMap<ProcId, Region>,
    // mapping from client identifier to its region
    client_to_region: HashMap<ClientId, Region>,
}

impl<P> Runner<P>
where
    P: Proc,
{
    /// Create a new `Runner` from a `planet`, a `config`, and two lists of regions:
    /// - `proc_regions`: list of regions where processes are located
    /// - `client_regions`: list of regions where clients are located
    pub fn new<F>(
        planet: Planet,
        config: Config,
        create_proc: F,
        workload: Workload,
        proc_regions: Vec<Region>,
        client_regions: Vec<Region>,
    ) -> Self
    where
        F: Fn(ProcId, Region, Planet, Config) -> P,
    {
        // check that we have the correct number of `proc_regions`
        assert_eq!(proc_regions.len(), config.n());

        // create router
        let mut router = Router::new();
        let mut procs = Vec::with_capacity(config.n());

        // create procs
        let to_discover: Vec<_> = proc_regions
            .into_iter()
            .enumerate()
            .map(|(proc_id, region)| {
                let proc_id = proc_id as u64;
                // create proc and save it
                let proc = create_proc(proc_id, region.clone(), planet.clone(), config);
                procs.push(proc);

                (proc_id, region)
            })
            .collect();

        // create proc to region mapping
        let proc_to_region = to_discover.clone().into_iter().collect();

        // register procs
        procs.into_iter().for_each(|mut proc| {
            // discover `procs`
            assert!(proc.discover(to_discover.clone()));
            // and register it
            router.register_proc(proc);
        });

        // register clients and create client to region mapping
        let client_to_region = client_regions
            .into_iter()
            .enumerate()
            .map(|(client_id, region)| {
                let client_id = client_id as u64;
                // create client
                let mut client = Client::new(client_id, region.clone(), planet.clone(), workload);
                // discover `procs`
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
                    ScheduleAction::SubmitToProc(proc_id, cmd) => {
                        // get proc's region
                        let proc_region = self.proc_region(proc_id);
                        // submit to process and schedule output messages
                        let to_send = self.router.submit_to_proc(proc_id, cmd);
                        self.schedule_it(proc_region, to_send);
                    }
                    ScheduleAction::SendToProc(proc_id, msg) => {
                        // get proc's region
                        let proc_region = self.proc_region(proc_id);
                        // route to process and schedule output messages
                        let to_send = self.router.route_to_proc(proc_id, msg);
                        self.schedule_it(proc_region, to_send);
                    }
                    ScheduleAction::SendToClient(client_id, cmd_result) => {
                        // get client's region
                        let client_region = self.client_region(client_id);
                        // route to client and schedule output command
                        let to_send = self
                            .router
                            .route_to_client(client_id, cmd_result, &self.time);
                        self.schedule_it(client_region, to_send);
                    }
                }
            })
        }
    }

    /// Schedule a `ToSend`. When scheduling, we shoud never route!
    fn schedule_it(&mut self, from: Region, to_send: ToSend<P::Message>) {
        match to_send {
            ToSend::ToCoordinator(proc_id, cmd) => {
                // create action and schedule it
                let action = ScheduleAction::SubmitToProc(proc_id, cmd);
                self.schedule_it_to_proc(&from, proc_id, action);
            }
            ToSend::ToProcs(target, msg) => {
                // for each process in target, schedule message delivery
                target.into_iter().for_each(|proc_id| {
                    // create action and schedule it
                    let action = ScheduleAction::SendToProc(proc_id, msg.clone());
                    self.schedule_it_to_proc(&from, proc_id, action);
                });
            }
            ToSend::ToClients(cmd_results) => {
                // for each command result, schedule its delivery
                cmd_results.into_iter().for_each(|cmd_result| {
                    // create action and schedule it
                    let client_id = cmd_result.rifl().source();
                    let action = ScheduleAction::SendToClient(client_id, cmd_result);
                    self.schedule_it_to_client(&from, client_id, action);
                });
            }
            ToSend::Nothing => {
                // nothing to do
            }
        }
    }

    fn schedule_it_to_proc(&mut self, from: &Region, proc_id: ProcId, action: ScheduleAction<P>) {
        // get proc's region
        let proc_region = self.proc_region(proc_id);

        // compute distance between regions and schedule action
        let distance = self.distance(from, &proc_region);
        self.schedule.schedule(&self.time, distance, action);
    }

    fn schedule_it_to_client(
        &mut self,
        from: &Region,
        client_id: ClientId,
        action: ScheduleAction<P>,
    ) {
        // get client's region
        let client_region = self.client_region(client_id);

        // compute distance between regions and schedule action
        let distance = self.distance(from, &client_region);
        self.schedule.schedule(&self.time, distance, action);
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
