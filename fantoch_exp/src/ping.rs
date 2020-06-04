use color_eyre::Report;
use rusoto_core::Region;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::time::Duration;
use tracing::instrument;
use tsunami::providers::aws;
use tsunami::{Machine, Tsunami};

const SERVER_ALIVE_INTERVAL_SECS: u64 = 60; //

pub async fn ping_experiment(
    regions: Vec<Region>,
    instance_type: &str,
    max_spot_instance_request_wait_secs: u64,
    max_instance_duration_hours: usize,
    experiment_duration_secs: usize,
) -> Result<(), Report> {
    let mut descriptors = Vec::with_capacity(regions.len());
    for region in &regions {
        let name = String::from(region.name());
        let setup = aws::Setup::default()
            .instance_type(instance_type)
            .region_with_ubuntu_ami(region.clone())
            .await?
            .ssh_setup(|ssh_builder| {
                ssh_builder.server_alive_interval(
                    std::time::Duration::from_secs(SERVER_ALIVE_INTERVAL_SECS),
                );
            })
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
    launcher.set_max_instance_duration(max_instance_duration_hours);

    let max_wait =
        Some(Duration::from_secs(max_spot_instance_request_wait_secs));
    if let Err(e) = launcher.spawn(descriptors, max_wait).await {
        launcher.terminate_all().await?;
        return Err(e);
    }

    let vms = launcher.connect_all().await?;

    let mut pings = Vec::with_capacity(regions.len() * regions.len());
    for from in &regions {
        for to in &regions {
            let from = vms.get(from.name()).unwrap();
            let to = vms.get(to.name()).unwrap();
            let ping = ping(from, to, experiment_duration_secs);
            pings.push(ping);
        }
    }

    let mut results = HashMap::new();
    for result in futures::future::join_all(pings).await {
        let (from, to, output) = result?;
        // find the line of interest
        let aggregate = output
            .lines()
            .into_iter()
            .find(|line| line.contains("min/avg/max"))
            .expect("there should fine a line matching min/avg/max");

        // aggregate is something like:
        // rtt min/avg/max/mdev = 0.175/0.193/0.283/0.025 ms
        let stats = aggregate.split(" ").nth(3).unwrap();
        let stats = format!("{}:{}", stats, to);

        // save stats
        results.entry(from).or_insert_with(Vec::new).push(stats);
    }

    for (region, mut stats) in results {
        // sort ping results
        stats.sort();
        let content = stats.join("\n");

        // save ping results to file
        let mut file = File::create(format!("{}.dat", region))?;
        file.write_all(content.as_bytes())?;
    }

    launcher.terminate_all().await?;
    Ok(())
}

#[instrument]
async fn ping<'a>(
    from: &'a Machine<'a>,
    to: &'a Machine<'a>,
    experiment_duration_secs: usize,
) -> Result<(String, String, String), Report> {
    println!(
        "starting ping from {} to {} during {} seconds",
        from.nickname, to.nickname, experiment_duration_secs
    );

    let out = from
        .ssh
        .command("ping")
        // specify the duration of the experiment in seconds
        .arg("-w")
        .arg(experiment_duration_secs.to_string())
        // specify the ping timeout: never
        .arg("-W")
        .arg("0")
        .arg(&to.public_ip)
        .output()
        .await?;

    let stdout = String::from_utf8(out.stdout)?;
    Ok((from.nickname.clone(), to.nickname.clone(), stdout))
}
