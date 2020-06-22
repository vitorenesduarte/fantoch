use crate::exp::{self, Machines, RunMode};
use color_eyre::Report;
use rusoto_core::Region;
use std::collections::HashMap;
use std::time::Duration;
use tsunami::Tsunami;

pub async fn spawn(
    launcher: &mut tsunami::providers::aws::Launcher<
        rusoto_credential::DefaultCredentialsProvider,
    >,
    server_instance_type: String,
    client_instance_type: String,
    regions: Vec<Region>,
    max_spot_instance_request_wait_secs: u64,
    max_instance_duration_hours: usize,
    branch: String,
    run_mode: RunMode,
) -> Result<Machines<'_>, Report> {
    let server_tag = "server";
    let client_tag = "client";
    let tags = vec![
        (server_tag.to_string(), server_instance_type),
        (client_tag.to_string(), client_instance_type),
    ];
    let (regions, mut vms) = do_spawn(
        launcher,
        tags,
        regions,
        max_spot_instance_request_wait_secs,
        max_instance_duration_hours,
        branch,
        run_mode,
    )
    .await?;
    let servers = vms.remove(server_tag).expect("servers vms");
    let clients = vms.remove(client_tag).expect("client vms");
    Ok(Machines {
        regions,
        servers,
        clients,
    })
}

async fn do_spawn(
    launcher: &mut tsunami::providers::aws::Launcher<
        rusoto_credential::DefaultCredentialsProvider,
    >,
    tags: Vec<(String, String)>,
    regions: Vec<Region>,
    max_spot_instance_request_wait_secs: u64,
    max_instance_duration_hours: usize,
    branch: String,
    run_mode: RunMode,
) -> Result<
    (
        Vec<String>,
        HashMap<String, HashMap<String, tsunami::Machine<'_>>>,
    ),
    Report,
> {
    let sep = "_";
    let to_name =
        |tag, region: &Region| format!("{}{}{}", tag, sep, region.name());
    let from_name = |name: String| {
        let parts: Vec<_> = name.split(sep).collect();
        assert_eq!(parts.len(), 2);
        (parts[0].to_string(), parts[1].to_string())
    };

    // create machine descriptors
    let mut descriptors = Vec::with_capacity(regions.len());
    for (tag, instance_type) in &tags {
        for region in &regions {
            // get instance name
            let name = to_name(tag, region);

            // create setup
            let setup = tsunami::providers::aws::Setup::default()
                .instance_type(instance_type.clone())
                .region_with_ubuntu_ami(region.clone())
                .await?
                .setup(exp::fantoch_setup(branch.clone(), run_mode));

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
        let (tag, region) = from_name(name);
        let res = results
            .entry(tag)
            .or_insert_with(|| HashMap::with_capacity(regions.len()))
            .insert(region, vm);
        assert!(res.is_none());
    }
    let regions = regions
        .into_iter()
        .map(|region| region.name().to_string())
        .collect();
    Ok((regions, results))
}
