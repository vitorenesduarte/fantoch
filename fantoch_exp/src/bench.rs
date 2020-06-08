use crate::util;
use color_eyre::Report;
use rusoto_core::Region;
use std::time::Duration;
use tsunami::providers::aws;
use tsunami::Tsunami;

const DEBUG_SECS: u64 = 30 * 60; // 30 minutes

pub async fn bench_experiment(
    regions: Vec<Region>,
    instance_type: impl ToString + Clone,
    max_spot_instance_request_wait_secs: u64,
    max_instance_duration_hours: usize,
    branch: &'static str,
) -> Result<(), Report> {
    let mut launcher: aws::Launcher<_> = Default::default();
    let result = bench_experiment_run(
        &mut launcher,
        regions,
        instance_type,
        max_spot_instance_request_wait_secs,
        max_instance_duration_hours,
        branch,
    )
    .await;
    println!("experiment result: {:?}", result);
    // if result.is_err() {
    tokio::time::delay_for(tokio::time::Duration::from_secs(DEBUG_SECS)).await;
    // }
    // make sure we always terminate
    launcher.terminate_all().await?;
    result
}

pub async fn bench_experiment_run(
    launcher: &mut aws::Launcher<rusoto_credential::DefaultCredentialsProvider>,
    regions: Vec<Region>,
    instance_type: impl ToString + Clone,
    max_spot_instance_request_wait_secs: u64,
    max_instance_duration_hours: usize,
    branch: &'static str,
) -> Result<(), Report> {
    let mut descriptors = Vec::with_capacity(regions.len());
    for region in &regions {
        // get region name
        let name = region.name().to_string();

        // create setup
        let setup = aws::Setup::default()
            .instance_type(instance_type.clone())
            .region_with_ubuntu_ami(region.clone())
            .await?
            .setup(util::fantoch_setup(branch));

        // save setup
        descriptors.push((name, setup))
    }

    // spawn and connect
    launcher.set_max_instance_duration(max_instance_duration_hours);
    let max_wait =
        Some(Duration::from_secs(max_spot_instance_request_wait_secs));
    launcher.spawn(descriptors, max_wait).await?;
    let vms = launcher.connect_all().await?;
    Ok(())
}
