use color_eyre::eyre::WrapErr;
use color_eyre::Report;
use fantoch_exp::args;
use fantoch_exp::machine::Machine;
use rusoto_core::Region;
use std::time::Duration;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tracing::instrument;
use tracing_futures::Instrument;
use tsunami::providers::aws::LaunchMode;
use tsunami::Tsunami;

const LAUCH_MODE: LaunchMode = LaunchMode::DefinedDuration { hours: 1 };
const INSTANCE_TYPE: &str = "m3.xlarge";
const MAX_SPOT_INSTANCE_REQUEST_WAIT_SECS: u64 = 5 * 60; // 5 minutes

const PING_DURATION_SECS: usize = 30 * 60; // 30 minutes

/// This script should be called like: $ script hosts seconds output
/// - hosts: file where each line looks like "region::ip"
/// - seconds: number of seconds the ping will run
/// - output: the file where the output will be written
const SCRIPT: &str = "./../ping_exp_gcp/region_ping_loop.sh";
const HOSTS: &str = "./hosts";

#[tokio::main]
async fn main() -> Result<(), Report> {
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

    ping_experiment(
        LAUCH_MODE,
        regions,
        INSTANCE_TYPE,
        MAX_SPOT_INSTANCE_REQUEST_WAIT_SECS,
        PING_DURATION_SECS,
    )
    .await
}

async fn ping_experiment(
    launch_mode: LaunchMode,
    regions: Vec<Region>,
    instance_type: impl ToString + Clone,
    max_spot_instance_request_wait_secs: u64,
    experiment_duration_secs: usize,
) -> Result<(), Report> {
    let mut launcher: tsunami::providers::aws::Launcher<_> = Default::default();
    let result = ping_experiment_run(
        &mut launcher,
        launch_mode,
        regions,
        instance_type,
        max_spot_instance_request_wait_secs,
        experiment_duration_secs,
    )
    .await;
    tracing::info!("experiment result: {:?}", result);
    // make sure we always terminate
    launcher.terminate_all().await?;
    result
}

pub async fn ping_experiment_run(
    launcher: &mut tsunami::providers::aws::Launcher<
        rusoto_credential::DefaultCredentialsProvider,
    >,
    launch_mode: LaunchMode,
    regions: Vec<Region>,
    instance_type: impl ToString + Clone,
    max_spot_instance_request_wait_secs: u64,
    experiment_duration_secs: usize,
) -> Result<(), Report> {
    let mut descriptors = Vec::with_capacity(regions.len());
    for region in &regions {
        // get region name
        let name = region.name().to_string();

        // create setup
        let setup = tsunami::providers::aws::Setup::default()
            .instance_type(instance_type.clone())
            .region_with_ubuntu_ami(region.clone())
            .await?
            .setup(|vm| {
                Box::pin(async move {
                    let update = vm
                        .ssh
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

        // save setup
        descriptors.push((name, setup))
    }

    // spawn and connect
    launcher.set_mode(launch_mode);
    let max_wait =
        Some(Duration::from_secs(max_spot_instance_request_wait_secs));
    launcher.spawn(descriptors, max_wait).await?;
    let mut vms = launcher.connect_all().await?;

    // create HOSTS file content: each line should be "region::ip"
    // - create ping future for each region along the way
    let mut pings = Vec::with_capacity(regions.len());
    let hosts = regions
        .iter()
        .map(|region| {
            let region_name = region.name();
            let vm = vms.remove(region_name).unwrap();

            // compute host entry
            let host = format!("{}::{}", region_name, vm.public_ip);

            // create ping future
            let region_span =
                tracing::info_span!("region", name = ?region_name);
            let ping =
                ping(vm, experiment_duration_secs).instrument(region_span);
            pings.push(ping);

            // return host name
            host
        })
        .collect::<Vec<_>>()
        .join("\n");

    // create HOSTS file
    let mut file = File::create(HOSTS).await?;
    file.write_all(hosts.as_bytes()).await?;

    for result in futures::future::join_all(pings).await {
        let () = result?;
    }
    Ok(())
}

#[instrument]
async fn ping(
    vm: tsunami::Machine<'_>,
    experiment_duration_secs: usize,
) -> Result<(), Report> {
    tracing::info!(
        "will launch ping experiment with {} seconds",
        experiment_duration_secs
    );

    // files
    let script_file = "script.sh";
    let hosts_file = "hosts";
    let output_file = format!("{}.dat", vm.nickname);

    let vm = Machine::Tsunami(vm);

    // first copy both SCRIPT and HOSTS files to the machine
    vm.copy_to(SCRIPT, script_file)
        .await
        .wrap_err("copy_to script")?;
    vm.copy_to(HOSTS, hosts_file)
        .await
        .wrap_err("copy_to hosts")?;
    tracing::debug!("both files are copied to remote machine");

    // execute script remotely: "$ script.sh hosts seconds output"
    let args = args![hosts_file, experiment_duration_secs, output_file];
    let stdout = vm.script_exec(script_file, args).await?;
    tracing::debug!("script ended {}", stdout);

    // copy output file
    vm.copy_from(&output_file, &output_file)
        .await
        .wrap_err("copy_from")?;
    tracing::info!("output file is copied to local machine");
    Ok(())
}
