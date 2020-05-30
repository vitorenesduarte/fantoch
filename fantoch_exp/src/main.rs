use color_eyre::Report;
use rusoto_core::Region;
use std::time::Duration;
use tracing::instrument;
use tsunami::providers::aws;
use tsunami::{Machine, Tsunami};

const INSTANCE_TYPE: &str = "t3-small";
const MAX_SPOT_INSTANCE_REQUEST_WAIT: u64 = 10; // seconds
const MAX_INSTANCE_DURATION: usize = 1; // hours

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
        Region::ApNortheast2,
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

    let mut launcher: tsunami::providers::aws::Launcher<_> = Default::default();
    launcher.set_max_instance_duration(MAX_INSTANCE_DURATION);
    launcher.spawn(
        descriptors,
        Some(Duration::from_secs(MAX_SPOT_INSTANCE_REQUEST_WAIT)),
    )
    .await?;
    let vms = launcher.connect_all().await?;

    let pings = all_regions
        .clone()
        .into_iter()
        .zip(all_regions.clone().into_iter())
        .map(|(from, to)| {
            let from = vms.get(from.name()).unwrap();
            let to = vms.get(to.name()).unwrap();
            ping(from, to)
        });

    for result in futures::future::join_all(pings).await {
        println!("{:?}", result);
    }

    launcher.terminate_all().await?;
    Ok(())
}

#[instrument]
async fn ping(from: &Machine<'_>, to: &Machine<'_>) -> Result<(), Report> {
    let to_ip = &to.public_ip;

    let out = from
        .ssh
        .command("ping")
        .arg("-c")
        .arg("10")
        .arg(&to_ip)
        .output()
        .await?;
    let stdout = std::string::String::from_utf8(out.stdout)?;
    tracing::info!(ping = %stdout, "ping");
    Ok(())
}
