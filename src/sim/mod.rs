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
use crate::metrics::Histogram;
use crate::planet::{Planet, Region};
use crate::protocol::Protocol;
use std::collections::HashMap;

pub fn run_simulation<P: Protocol>(
    config: Config,
    workload: Workload,
    clients_per_region: usize,
    process_regions: Vec<Region>,
    client_regions: Vec<Region>,
    planet: Planet,
) -> HashMap<Region, (usize, Histogram)> {
    // create runner
    let mut runner: Runner<P> = Runner::new(
        planet,
        config,
        workload,
        clients_per_region,
        process_regions,
        client_regions,
    );

    // run simulation and clients latencies
    runner.run()
}
