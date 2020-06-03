use color_eyre::Report;
use rusoto_core::Region;
use std::collections::HashMap;
use std::fs::File;
use std::time::Duration;
use tracing::instrument;
use tsunami::providers::aws;
use tsunami::{Machine, Tsunami};

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
    let all_regions = vec![
        // Region::AfSouth1,
        Region::ApEast1,
        Region::ApNortheast1,
        // Region::ApNortheast2, special-region
        Region::ApSouth1,
        Region::ApSoutheast1,
        Region::ApSoutheast2,
        Region::CaCentral1,
        Region::EuCentral1,
        Region::EuNorth1,
        // Region::EuSouth1,
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

    let all_regions = vec![Region::EuWest2, Region::UsWest2];

    let mut descriptors = Vec::with_capacity(all_regions.len());
    for region in &all_regions {
        let name = String::from(region.name());
        let setup = aws::Setup::default()
            .instance_type(INSTANCE_TYPE)
            .region_with_ubuntu_ami(region.clone())
            .await?
            .setup(|ssh| {
                Box::pin(async move {
                    let update = ssh
                        .command("sudo")
                        .arg("apt")
                        .arg("update")
                        .status()
                        .await;
                    if let Err(e) = update {
                        tracing::warn!("apt update failed: {}", e);
                    };
                    Ok(())
                })
            });
        descriptors.push((name, setup))
    }

    println!("--> descriptors {:?}", descriptors);

    let mut launcher: tsunami::providers::aws::Launcher<_> = Default::default();
    launcher.set_max_instance_duration(MAX_INSTANCE_DURATION);
    println!("--> launcher {:?}", launcher);

    launcher
        .spawn(
            descriptors,
            Some(Duration::from_secs(MAX_SPOT_INSTANCE_REQUEST_WAIT)),
        )
        .await?;
    let vms = launcher.connect_all().await?;

    println!("--> vms {:?}", vms);

    let pings = all_regions
        .clone()
        .into_iter()
        .zip(all_regions.clone().into_iter())
        .map(|(from, to)| {
            let from = vms.get(from.name()).unwrap();
            let to = vms.get(to.name()).unwrap();
            ping(from, to)
        });

    let mut results = HashMap::new();
    for (from, to, result) in futures::future::join_all(pings).await? {
        // find the line of interest
        let mut aggregate: Vec<_> = results
            .lines()
            .into_iter()
            .filter(|line| line.contains("min/avg/max"))
            .collect();
        assert_eq!(aggregate.len(), 1);
        let aggregate = aggregate.pop().unwrap();

        println!("{} -> {}: {}", from.name(), to.name(), aggregate);

        // create new ping stats
        let stats = format!("{}:{}", aggregate, to.name());

        // save stats
        results
            .entry(from.name())
            .or_insert_with(Vec::new)
            .push(stats);
    }

    for (region, mut stats) in results {
        // sort ping results
        stats.sort();
        let content = stats.join("\n");

        let file = File::create(format!("{}.dat", region.name()))
            .expect("it should be possible to create dat file");
        file.write(content);
    }

    launcher.terminate_all().await?;
    Ok(())
}

#[instrument]
async fn ping<'a>(
    from: &'a Machine<'a>,
    to: &'a Machine<'a>,
) -> Result<(&'a Machine<'a>, &'a Machine<'a>, String), Report> {
    let to_ip = &to.public_ip;

    let out = from
        .ssh
        .command("ping")
        // specify the duration of the experiment
        .arg("-w")
        .arg(EXPERIMENT_DURATION.to_string())
        // specify the ping timeout: never
        .arg("-W")
        .arg(0.to_string())
        .arg(&to_ip)
        .output()
        .await?;
    let stdout = String::from_utf8(out.stdout)?;
    Ok((from, to, stdout))
}
