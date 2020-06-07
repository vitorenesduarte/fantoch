use color_eyre::Report;
use eyre::WrapErr;
use rusoto_core::Region;
use std::time::Duration;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::instrument;
use tsunami::providers::aws;
use tsunami::{Machine, Tsunami};

/// This script should be called like: $ bash script hosts seconds output
/// - hosts: file where each line looks like "region::ip"
/// - seconds: number of seconds the ping will run
/// - output: the file where the output will be written
const SCRIPT: &str = "./../ping_exp_gcp/region_ping_loop.sh";
const HOSTS: &str = "./hosts";

pub async fn ping_experiment(
    regions: Vec<Region>,
    instance_type: impl ToString + Clone,
    max_spot_instance_request_wait_secs: u64,
    max_instance_duration_hours: usize,
    experiment_duration_secs: usize,
) -> Result<(), Report> {
    let mut launcher: aws::Launcher<_> = Default::default();
    let result = ping_experiment_run(
        &mut launcher,
        regions,
        instance_type,
        max_spot_instance_request_wait_secs,
        max_instance_duration_hours,
        experiment_duration_secs,
    )
    .await;
    println!("experiment result: {:?}", result);
    // make sure we always terminate
    launcher.terminate_all().await?;
    result
}

pub async fn ping_experiment_run(
    launcher: &mut aws::Launcher<rusoto_credential::DefaultCredentialsProvider>,
    regions: Vec<Region>,
    instance_type: impl ToString + Clone,
    max_spot_instance_request_wait_secs: u64,
    max_instance_duration_hours: usize,
    experiment_duration_secs: usize,
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

        // save setup
        descriptors.push((name, setup))
    }

    // spawn and connect
    launcher.set_max_instance_duration(max_instance_duration_hours);
    let max_wait =
        Some(Duration::from_secs(max_spot_instance_request_wait_secs));
    launcher.spawn(descriptors, max_wait).await?;
    let vms = launcher.connect_all().await?;

    // create HOSTS file content: each line should be "region::ip"
    // - create ping future for each region along the way
    let mut pings = Vec::with_capacity(regions.len());
    let hosts = regions
        .iter()
        .map(|region| {
            let region_name = region.name();
            let vm = vms.get(region_name).unwrap();
            // create ping future
            pings.push(ping(vm, experiment_duration_secs));
            // create host entry
            format!("{}::{}", region_name, vm.public_ip)
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
    vm: &Machine<'_>,
    experiment_duration_secs: usize,
) -> Result<(), Report> {
    println!(
        "{}: will launch ping experiment with {} seconds",
        vm.nickname, experiment_duration_secs
    );

    // files
    let script_file = "script.sh";
    let hosts_file = "hosts";
    let output_file = format!("{}.dat", vm.nickname);

    // first copy both SCRIPT and HOSTS files to the machine
    copy_to(SCRIPT, (script_file, vm)).await?;
    copy_to(HOSTS, (hosts_file, vm)).await?;
    println!("{}: both files are copied to remote machine", vm.nickname);

    // execute script remotely: "$ bash SCRIPT HOSTS seconds output"
    let command = format!(
        "chmod u+x {}; ./{} {} {} {}",
        script_file,
        script_file,
        hosts_file,
        experiment_duration_secs,
        output_file
    );
    let out = vm
        .ssh
        .shell(command)
        .output()
        .await
        .wrap_err("chmod; script")?;
    let stdout = String::from_utf8(out.stdout)?;
    println!("{}: script ended {}", vm.nickname, stdout);

    // copy output file
    copy_from((&output_file, vm), &output_file).await?;
    println!("{}: output file is copied to local machine", vm.nickname);
    Ok(())
}

async fn copy_to(
    local_path: &str,
    (remote_path, vm): (&str, &Machine<'_>),
) -> Result<(), Report> {
    // get file contents
    let mut contents = String::new();
    File::open(local_path)
        .await?
        .read_to_string(&mut contents)
        .await?;
    // write them in remote machine
    let mut remote_file = vm.ssh.sftp().write_to(remote_path).await?;
    remote_file.write_all(contents.as_bytes()).await?;
    remote_file.close().await?;
    Ok(())
}

async fn copy_from(
    (remote_path, vm): (&str, &Machine<'_>),
    local_path: &str,
) -> Result<(), Report> {
    // get file contents from remote machine
    let mut contents = String::new();
    let mut remote_file = vm.ssh.sftp().read_from(remote_path).await?;
    remote_file.read_to_string(&mut contents).await?;
    remote_file.close().await?;
    // write them in file
    File::create(local_path)
        .await?
        .write_all(contents.as_bytes())
        .await?;
    Ok(())
}