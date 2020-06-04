mod ping;

use color_eyre::Report;
use rusoto_core::Region;

const INSTANCE_TYPE: &str = "m5.large";
const MAX_SPOT_INSTANCE_REQUEST_WAIT_SECS: u64 = 120; // 2 minutes
const MAX_INSTANCE_DURATION_HOURS: usize = 1;
const EXPERIMENT_DURATION_SECS: usize = 30 * 60; // 30 minutes

#[tokio::main]
async fn main() -> Result<(), Report> {
    // init logging
    tracing_subscriber::fmt::init();

    // all AWS regions
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

    ping::ping_experiment(
        regions,
        INSTANCE_TYPE,
        MAX_SPOT_INSTANCE_REQUEST_WAIT_SECS,
        MAX_INSTANCE_DURATION_HOURS,
        EXPERIMENT_DURATION_SECS,
    )
    .await
}
