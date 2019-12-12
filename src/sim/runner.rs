use crate::client::{Client, Workload};
use crate::config::Config;
use crate::id::{ClientId, ProcId};
use crate::newt::Newt;
use crate::newt::{Message, ToSend};
use crate::planet::{Planet, Region};
use crate::sim::Router;
use crate::time::SimTime;
use std::collections::HashMap;

#[allow(dead_code)]
pub struct Runner {
    time: SimTime,
    router: Router,
    proc_to_region: HashMap<ProcId, Region>,
    client_to_region: HashMap<ClientId, Region>,
}

#[allow(dead_code)]
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

        // create simulation time
        let time = SimTime::new();

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
            time,
            router,
            proc_to_region,
            client_to_region,
        }
    }

    /// Run the simulation.
    pub fn run(&mut self) {
        // borrow self.time to satisfy the borrow checker
        let time = &self.time;
        self.router
            .clients()
            .map(|client| {
                // start all clients at time 0
                client.start(time)
            })
            // TODO can we avoid collecting here?
            .collect::<Vec<_>>()
            .into_iter()
            .for_each(|(proc_id, cmd)| {
                // create submit message
                let submit = Message::Submit { cmd };
                // route it to the correct process
                let to_send = self.router.route_to_proc(proc_id, submit);
                // and schedule its output
                self.schedule(to_send);
            });
    }

    /// Schedule a `ToSend`.
    fn schedule(&mut self, to_send: ToSend) {
        match to_send {
            ToSend::Procs(msg, target) => {
                // nothing new to send
            }
            ToSend::Clients(cmd_results) => {
                // route command results to clients:
                // - if there are new commands to be submitted, schedule their arrival in
                //   coordinators
                // - we need to schedule because the client maybe be connected to a coordinator that
                //   is far away from the client
                cmd_results
                    .into_iter()
                    .map(|cmd_result| {
                        // route each command result to the corresponding client
                        self.router.route_to_client(cmd_result, &self.time)
                    })
                    // TODO can we avoid collecting here?
                    .collect::<Vec<_>>()
                    .into_iter()
                    .for_each(|to_send| self.schedule(to_send))
            }
            ToSend::Nothing => {
                // nothing to do
            }
        }
    }
}
