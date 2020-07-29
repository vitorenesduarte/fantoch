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
    launch_mode: tsunami::providers::aws::LaunchMode,
    regions: Vec<rusoto_core::Region>,
    shard_count: usize,
    server_instance_type: String,
    client_instance_type: String,
    max_spot_instance_request_wait_secs: u64,
    branch: String,
    run_mode: RunMode,
    features: Vec<FantochFeature>,
) -> Result<Machines<'_>, Report> {
    // create nicknames for all machines
    let nicknames = super::create_nicknames(shard_count, &regions);

    // setup machines
    let vms = spawn_and_setup(
        launcher,
        launch_mode,
        nicknames,
        server_instance_type,
        client_instance_type,
        max_spot_instance_request_wait_secs,
        branch,
        run_mode,
        features,
    )
    .await?;

    // create placement, servers, and clients
    let region_count = regions.len();
    let process_count = region_count * shard_count;
    let client_count = region_count;
    let placement = super::create_placement(shard_count, regions);
    let mut servers = HashMap::with_capacity(process_count);
    let mut clients = HashMap::with_capacity(client_count);

    for (Nickname { shard_id, region }, vm) in vms {
        match shard_id {
            Some(shard_id) => {
                let (process_id, _region_index) = placement
                    .get(&(region, shard_id))
                    .expect("region and shard id should exist in placement");
                assert!(servers.insert(*process_id, vm).is_none());
            }
            None => {
                // add to clients
                assert!(clients.insert(region, vm).is_none());
            }
        }
    }
    let machines = Machines::new(placement, servers, clients);
    Ok(machines)
}

async fn spawn_and_setup<'a>(
    launcher: &'a mut tsunami::providers::aws::Launcher<
        rusoto_credential::DefaultCredentialsProvider,
    >,
    launch_mode: tsunami::providers::aws::LaunchMode,
    nicknames: Vec<Nickname>,
    server_instance_type: String,
    client_instance_type: String,
    max_spot_instance_request_wait_secs: u64,
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
        .set_mode(launch_mode)
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
