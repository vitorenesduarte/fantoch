use crate::client::{Client, Workload};
use crate::config::Config;
use crate::newt::Newt;
use crate::planet::{Planet, Region};
use crate::sim::Router;

#[allow(dead_code)]
pub struct Runner {
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

        // create router
        let mut router = Router::new();

        // register procs
        proc_regions
            .into_iter()
            .enumerate()
            .for_each(|(proc_id, region)| {
                // create proc
                let newt = Newt::new(proc_id as u64, region, planet.clone(), config);
                // and register it
                router.register_proc(newt);
            });

        // register clients
        client_regions
            .into_iter()
            .enumerate()
            .for_each(|(client_id, region)| {
                // create client
                let client = Client::new(client_id as u64, region, planet.clone(), workload);
                // and register it
                router.register_client(client);
            });

        // create runner
        Self { router }
    }
}
