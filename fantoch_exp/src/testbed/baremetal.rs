use super::{CLIENT_TAG, SERVER_TAG};
use crate::exp::{self, Machines, RunMode};
use crate::util;
use color_eyre::Report;
use eyre::WrapErr;
use rusoto_core::Region;
use std::collections::HashMap;

const MACHINE: &str = "./../exp/files/machines";
const PRIVATE_KEY: &str = "~/.ssh/id_rsa";

pub async fn setup<'a>(
    launchers: &'a mut Vec<tsunami::providers::baremetal::Machine>,
    servers_count: usize,
    clients_count: usize,
    regions: Vec<Region>,
    branch: String,
    run_mode: RunMode,
) -> Result<Machines<'a>, Report> {
    let machines_count = servers_count + clients_count;

    // get ips and check that we have enough of them
    let content = tokio::fs::read_to_string(MACHINE).await?;
    let machines: Vec<_> = content.lines().take(machines_count).collect();
    assert_eq!(machines.len(), machines_count, "not enough machines");

    // get machine and launcher iterators
    let mut machines_iter = machines.into_iter();
    let mut launcher_iter = launchers.iter_mut();

    // setup machines
    let mut launches = Vec::with_capacity(machines_count);
    for region in regions.iter() {
        for tag in vec![SERVER_TAG, CLIENT_TAG] {
            // find one machine and a launcher for this machine
            let machine = machines_iter.next().unwrap();
            let launcher = launcher_iter.next().unwrap();

            // create baremetal setup
            let setup = baremetal_setup(machine, branch.clone(), run_mode)
                .await
                .wrap_err("baremetal setup")?;

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

    // mapping from tag, to a mapping from region to vm
    let mut servers = HashMap::with_capacity(servers_count);
    let mut clients = HashMap::with_capacity(clients_count);

    for result in futures::future::join_all(launches).await {
        let vm = result.wrap_err("baremetal launch")?;
        let (tag, region) = super::from_nickname(vm.nickname.clone());

        let res = match tag.as_str() {
            SERVER_TAG => servers.insert(region, vm),
            CLIENT_TAG => clients.insert(region, vm),
            tag => {
                panic!("unrecognized vm tag: {}", tag);
            }
        };
        assert!(res.is_none());
    }

    // check that we have enough machines
    assert_eq!(servers.len(), servers_count, "not enough server vms");
    assert_eq!(clients.len(), clients_count, "not enough client vms");

    Ok(Machines {
        regions: super::regions(regions),
        servers,
        clients,
    })
}

async fn baremetal_setup(
    machine: &str,
    branch: String,
    run_mode: RunMode,
) -> Result<tsunami::providers::baremetal::Setup, Report> {
    let parts: Vec<_> = machine.split("@").collect();
    assert_eq!(parts.len(), 2, "machine should have the form username@addr");
    let username = parts[0].to_string();
    // TODO I think addr needs a port to work in tsunami
    let hostname = parts[1].to_string();

    // fetch public ip
    let command = String::from("hostname - I");
    let hostname = util::exec(
        &username,
        &hostname,
        &std::path::PathBuf::from(PRIVATE_KEY),
        command,
    )
    .await
    .wrap_err("hostname -I")?;
    // hostname should be of the form "vitor.enes@apollo-2-1.imdea 10.10.5.61"
    let parts: Vec<_> = hostname.split(" ").collect();
    assert_eq!(
        parts.len(),
        2,
        "hostname should have the form username@hostname ip"
    );
    let ip = parts[1].to_string();

    let setup = tsunami::providers::baremetal::Setup::new(ip, Some(username))?
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
