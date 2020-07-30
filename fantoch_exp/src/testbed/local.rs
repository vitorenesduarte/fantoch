use super::Nickname;
use crate::machine::{Machine, Machines};
use crate::{FantochFeature, RunMode, Testbed};
use color_eyre::eyre::WrapErr;
use color_eyre::Report;
use std::collections::HashMap;

pub async fn setup<'a>(
    regions: Vec<rusoto_core::Region>,
    shard_count: usize,
    branch: String,
    run_mode: RunMode,
    features: Vec<FantochFeature>,
) -> Result<Machines<'a>, Report> {
    // setup local machine that will be the holder of all machines
    crate::machine::local_fantoch_setup(
        branch,
        run_mode,
        features,
        Testbed::Local,
    )
    .await
    .wrap_err("local setup")?;

    // create nicknames for all machines
    let nicknames = super::create_nicknames(shard_count, &regions);

    // create placement, servers, and clients
    let server_count = regions.len();
    let client_count = regions.len();
    let placement = super::create_placement(shard_count, regions);
    let mut servers = HashMap::with_capacity(server_count);
    let mut clients = HashMap::with_capacity(client_count);

    for Nickname { region, shard_id } in nicknames {
        let vm = Machine::Local;
        let unique_insert = match shard_id {
            Some(shard_id) => {
                // it's a server; find it's process id
                let (process_id, _region_index) =
                    placement.get(&(region, shard_id)).expect(
                        "pair region and shard id should exist in placement",
                    );
                servers.insert(*process_id, vm).is_none()
            }
            None => {
                // it's a client
                clients.insert(region, vm).is_none()
            }
        };
        assert!(unique_insert);
    }

    // check that we have enough machines
    assert_eq!(
        servers.len(),
        server_count * shard_count,
        "not enough server vms"
    );
    assert_eq!(clients.len(), client_count, "not enough client vms");

    let machines = Machines::new(placement, servers, clients);
    Ok(machines)
}
