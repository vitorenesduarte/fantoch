use super::Nickname;
use crate::exp::{self, Machines};
use crate::{FantochFeature, RunMode, Testbed};
use color_eyre::Report;
use std::collections::HashMap;
use std::time::Duration;
use tsunami::Tsunami;

pub async fn setup(
    launcher: &mut tsunami::providers::aws::Launcher<
        rusoto_credential::DefaultCredentialsProvider,
    >,
    regions: Vec<rusoto_core::Region>,
    server_instance_type: String,
    client_instance_type: String,
    shard_count: usize,
    max_spot_instance_request_wait_secs: u64,
    max_instance_duration_hours: usize,
    branch: String,
    run_mode: RunMode,
    features: Vec<FantochFeature>,
) -> Result<Machines<'_>, Report> {
    // create nicknames for all machines
    let nicknames = super::create_nicknames(shard_count, &regions);
    let mut vms = spawn_and_setup(
        launcher,
        nicknames,
        server_instance_type,
        client_instance_type,
        max_spot_instance_request_wait_secs,
        max_instance_duration_hours,
        branch,
        run_mode,
        features,
    )
    .await?;
    let placement = super::create_placement(shard_count, regions);
    todo!();
    // let servers = vms.remove(SERVER_TAG).expect("servers vms");
    // let clients = vms.remove(CLIENT_TAG).expect("client vms");
    // let machines = Machines::new(super::to_regions(regions), servers,
    // clients); Ok(machines)
}

async fn spawn_and_setup<'a>(
    launcher: &'a mut tsunami::providers::aws::Launcher<
        rusoto_credential::DefaultCredentialsProvider,
    >,
    nicknames: Vec<Nickname>,
    server_instance_type: String,
    client_instance_type: String,
    max_spot_instance_request_wait_secs: u64,
    max_instance_duration_hours: usize,
    branch: String,
    run_mode: RunMode,
    features: Vec<FantochFeature>,
) -> Result<Vec<(Nickname, tsunami::Machine<'a>)>, Report> {
    // create machine descriptors
    let mut descriptors = Vec::with_capacity(nicknames.len());
    for nickname in nicknames {
        // get instance name
        let name = nickname.to_string();

        // get instance type and region
        let instance_type = if nickname.shard_id.is_some() {
            // in this case, it's a server
            server_instance_type.clone()
        } else {
            // otherwise, it's a client
            client_instance_type.clone()
        };
        let region = nickname
            .region
            .name()
            .parse::<rusoto_core::Region>()
            .expect("creating a rusoto_core::Region should work");

        // create setup
        let setup = tsunami::providers::aws::Setup::default()
            .instance_type(instance_type)
            .region_with_ubuntu_ami(region)
            .await?
            .setup(exp::fantoch_setup(
                branch.clone(),
                run_mode,
                features.clone(),
                Testbed::Aws,
            ));

        // save setup
        descriptors.push((name, setup))
    }

    // spawn and connect
    launcher
        .set_max_instance_duration(max_instance_duration_hours)
        // make sure ports are open
        // TODO create VPC and use private ips
        .open_ports();
    let max_wait =
        Some(Duration::from_secs(max_spot_instance_request_wait_secs));
    launcher.spawn(descriptors, max_wait).await?;
    let vms = launcher.connect_all().await?;

    // return vms
    let vms = vms
        .into_iter()
        .map(|(name, vm)| {
            let nickname = Nickname::from_string(name);
            (nickname, vm)
        })
        .collect();
    Ok(vms)
}
