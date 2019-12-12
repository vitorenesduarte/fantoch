use crate::client::{Client, Workload};
use crate::config::Config;
use crate::newt::Message;
use crate::newt::Newt;
use crate::planet::{Planet, Region};
use crate::sim::Router;
use crate::time::SimTime;

#[allow(dead_code)]
pub struct Runner {
    time: SimTime,
    router: Router,
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

        // register clients
        client_regions
            .into_iter()
            .enumerate()
            .for_each(|(client_id, region)| {
                // create client
                let mut client = Client::new(client_id as u64, region, planet.clone(), workload);
                // discover `procs`
                client.discover(procs.clone());
                // and register it
                router.register_client(client);
            });

        // create runner
        Self { time, router }
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
            // TODO can we avoid collecting here? borrow checker complains if we don't
            .collect::<Vec<_>>()
            .into_iter()
            .map(|(proc_id, cmd)| {
                // create submit message
                let submit = Message::Submit { cmd };
                self.router.route_to_proc(proc_id, submit);
            })
            .for_each(|x| {
                println!("{:?}", x);
            });
    }
}
