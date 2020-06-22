use super::{CLIENT_TAG, SERVER_TAG};
use crate::exp::{self, Machines, RunMode};
use color_eyre::Report;
use eyre::WrapErr;
use rusoto_core::Region;
use std::collections::HashMap;

const MACHINE_IPS: &str = "./../exp/files/machine_ips";
const PRIVATE_KEY: &str = "~/.ssh/id_rsa";

pub async fn setup<'a>(
    regions: Vec<Region>,
    branch: String,
    run_mode: RunMode,
) -> Result<Machines<'a>, Report> {
    // compute the necessary number of machines
    let servers_count = regions.len();
    let clients_count = regions.len();
    let machine_count = servers_count + clients_count;

    // get ips and check that we have enough of them
    let content = tokio::fs::read_to_string(MACHINE_IPS).await?;
    let machines: Vec<_> = content.lines().take(machine_count).collect();
    assert_eq!(machines.len(), machine_count, "not enough machines");
    let mut machines_iter = machines.into_iter();

    // create one launcher per machine:
    // - TODO this is needed since tsunami's baremetal provider does not give a
    //   global tsunami launcher as the aws provider
    let mut launchers = Vec::with_capacity(machine_count);
    for _ in 0..machine_count {
        let launcher: tsunami::providers::baremetal::Machine =
            Default::default();
        launchers.push(launcher);
    }
    let mut launcher_iter = launchers.iter_mut();

    // setup machines
    let mut launches = Vec::with_capacity(machine_count);
    for region in regions.iter() {
        for tag in vec![SERVER_TAG, CLIENT_TAG] {
            // find one machine and a launcher for this machine
            let machine = machines_iter.next().unwrap();
            let launcher = launcher_iter.next().unwrap();

            // create baremetal setup
            let setup = baremetal_setup(machine, branch.clone(), run_mode)?;

            // save baremetal launch
            let launch = baremetal_launch(
                launcher,
                region.name().to_string(),
                tag.to_string(),
                setup,
            );
            launches.push(launch);
        }
    }

    // // mapping from tag, to a mapping from region to vm
    // let mut results: HashMap<_, HashMap<_, _>> =
    //     HashMap::with_capacity(tags.len());
    // for (name, vm) in vms {
    //     let (tag, region) = super::from_nickname(name);
    //     let res = results
    //         .entry(tag)
    //         .or_insert_with(|| HashMap::with_capacity(regions.len()))
    //         .insert(region, vm);
    //     assert!(res.is_none());
    // }

    // for result in futures::future::join_all(launches).await {
    //     let machine = result.wrap_err("baremetal launch")?;
    //     latencies.push(latency);
    // }

    todo!()
}

fn baremetal_setup(
    machine: &str,
    branch: String,
    run_mode: RunMode,
) -> Result<tsunami::providers::baremetal::Setup, Report> {
    let parts: Vec<_> = machine.split("@").collect();
    assert_eq!(parts.len(), 2, "machine should have the form username@addr");
    let username = parts[0].to_string();
    // TODO I think addr needs a port to work in tsunami
    let addr = parts[1];
    let setup =
        tsunami::providers::baremetal::Setup::new(addr, Some(username))?
            .key_path(PRIVATE_KEY)
            .setup(exp::fantoch_setup(branch, run_mode));
    Ok(setup)
}

async fn baremetal_launch<'a>(
    launcher: &'a mut tsunami::providers::baremetal::Machine,
    region: String,
    tag: String,
    setup: tsunami::providers::baremetal::Setup,
) -> Result<tsunami::Machine<'a>, Report> {
    // create launch descriptor
    let nickname = super::to_nickname(&tag, &region);
    let descriptor = tsunami::providers::LaunchDescriptor {
        region,
        max_wait: None,
        machines: vec![(nickname.clone(), setup)],
    };

    // do launch the machine
    use tsunami::providers::Launcher;
    launcher.launch(descriptor).await?;
    let mut machines = launcher.connect_all().await?;

    // check that a single machine was returned
    assert_eq!(
        machines.len(),
        1,
        "baremetal launched didn't return a single machine"
    );
    assert!(
        machines.contains_key(&nickname),
        "baremetal machines incorrectly identified"
    );
    // fetch the machine
    let machine = machines.remove(&nickname).unwrap();
    assert_eq!(
        machine.nickname, nickname,
        "baremetal machine has the wrong nickname"
    );
    Ok(machine)
}
