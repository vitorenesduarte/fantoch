use super::{CLIENT_TAG, SERVER_TAG};
use crate::exp::{self, Machines, RunMode, Testbed};
use color_eyre::Report;
use std::collections::HashMap;
use std::time::Duration;
use tsunami::Tsunami;

pub async fn setup(
    launcher: &mut tsunami::providers::aws::Launcher<
        rusoto_credential::DefaultCredentialsProvider,
    >,
    server_instance_type: String,
    client_instance_type: String,
    regions: Vec<rusoto_core::Region>,
    max_spot_instance_request_wait_secs: u64,
    max_instance_duration_hours: usize,
    branch: String,
    run_mode: RunMode,
) -> Result<Machines<'_>, Report> {
    let tags = vec![
        (SERVER_TAG.to_string(), server_instance_type),
        (CLIENT_TAG.to_string(), client_instance_type),
    ];
    let mut vms = spawn_and_setup(
        launcher,
        tags,
        &regions,
        max_spot_instance_request_wait_secs,
        max_instance_duration_hours,
        branch,
        run_mode,
    )
    .await?;
    let servers = vms.remove(SERVER_TAG).expect("servers vms");
    let clients = vms.remove(CLIENT_TAG).expect("client vms");
    Ok(Machines {
        regions: super::to_regions(regions),
        servers,
        clients,
    })
}

async fn spawn_and_setup<'a>(
    launcher: &'a mut tsunami::providers::aws::Launcher<
        rusoto_credential::DefaultCredentialsProvider,
    >,
    tags: Vec<(String, String)>,
    regions: &'_ Vec<rusoto_core::Region>,
    max_spot_instance_request_wait_secs: u64,
    max_instance_duration_hours: usize,
    branch: String,
    run_mode: RunMode,
) -> Result<
    HashMap<String, HashMap<fantoch::planet::Region, tsunami::Machine<'a>>>,
    Report,
> {
    // create machine descriptors
    let mut descriptors = Vec::with_capacity(regions.len());
    for (tag, instance_type) in &tags {
        for region in regions {
            // get instance name
            let name = super::to_nickname(tag, region.name());

            // create setup
            let setup = tsunami::providers::aws::Setup::default()
                .instance_type(instance_type.clone())
                .region_with_ubuntu_ami(region.clone())
                .await?
                .setup(exp::fantoch_setup(
                    branch.clone(),
                    run_mode,
                    Testbed::Aws,
                ));

            // save setup
            descriptors.push((name, setup))
        }
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

    // mapping from tag, to a mapping from region to vm
    let mut results: HashMap<_, HashMap<_, _>> =
        HashMap::with_capacity(tags.len());
    for (name, vm) in vms {
        let (tag, region) = super::from_nickname(name);
        let res = results
            .entry(tag)
            .or_insert_with(|| HashMap::with_capacity(regions.len()))
            .insert(region, vm);
        assert!(res.is_none());
    }
    Ok(results)
}
