mod ping;

use color_eyre::Report;
use rusoto_core::Region;

const INSTANCE_TYPE: &str = "t3.medium";
const MAX_SPOT_INSTANCE_REQUEST_WAIT: u64 = 120; // seconds
const MAX_INSTANCE_DURATION: usize = 1; // hours
const EXPERIMENT_DURATION: usize = 60; // seconds

#[tokio::main]
async fn main() -> Result<(), Report> {
    // init logging
    tracing_subscriber::fmt::init();

    // all AWS regions:
    // - TODO: add missing regions in the next release o rusoto
    let regions = vec![
        Region::AfSouth1,
        Region::ApEast1,
        Region::ApNortheast1,
        // Region::ApNortheast2, special-region
        Region::ApSouth1,
        Region::ApSoutheast1,
        Region::ApSoutheast2,
        Region::CaCentral1,
        Region::EuCentral1,
        Region::EuNorth1,
        Region::EuSouth1,
        Region::EuWest1,
        Region::EuWest2,
        Region::EuWest3,
        Region::MeSouth1,
        Region::SaEast1,
        Region::UsEast1,
        Region::UsEast2,
        Region::UsWest1,
        Region::UsWest2,
    ];

    let regions = vec![Region::UsWest1, Region::UsWest2];

    ping::ping_experiment(
        regions,
        INSTANCE_TYPE,
        MAX_SPOT_INSTANCE_REQUEST_WAIT,
        MAX_INSTANCE_DURATION,
        EXPERIMENT_DURATION,
    )
    .await
}
