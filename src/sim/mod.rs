// This module contains the definition of `Simulation`.
pub mod simulation;

// This module contains the definition of `Schedule`.
pub mod schedule;

// This module contains the definition of `Runner`.
pub mod runner;

// Re-exports.
pub use runner::Runner;
pub use schedule::Schedule;
pub use simulation::Simulation;

use crate::client::Workload;
use crate::config::Config;
use crate::planet::{Planet, Region};
use crate::protocol::Process;
use std::collections::HashMap;

pub fn run_simulation<P: Process>(
    config: Config,
    workload: Workload,
    clients_per_region: usize,
    process_regions: Vec<Region>,
    client_regions: Vec<Region>,
    planet: Planet,
) -> HashMap<Region, (usize, Vec<u64>)> {
    // function that creates ping pong processes
    let create_process =
        |process_id, region, planet, config| P::new(process_id, region, planet, config);

    // create runner
    let mut runner = Runner::new(
        planet,
        config,
        create_process,
        workload,
        clients_per_region,
        process_regions,
        client_regions,
    );

    // run simulation and clients latencies
    runner.run()
}
