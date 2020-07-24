use crate::args;
use crate::config::{Placement, RegionIndex};
use crate::util;
use crate::{FantochFeature, RunMode, Testbed};
use color_eyre::eyre::WrapErr;
use color_eyre::Report;
use fantoch::id::ProcessId;
use fantoch::planet::Region;
use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::pin::Pin;

/// This script should be called like: $ bash script branch
/// - branch: which `fantoch` branch to build
const SETUP_SCRIPT: &str = "exp_files/setup.sh";

pub struct Machines<'a> {
    placement: Placement,
    // potentially more than one process machine per region (if partial
    // replication)
    servers: HashMap<ProcessId, tsunami::Machine<'a>>,
    // only one client machine per region
    clients: HashMap<Region, tsunami::Machine<'a>>,
}

impl<'a> Machines<'a> {
    pub fn new(
        placement: Placement,
        servers: HashMap<ProcessId, tsunami::Machine<'a>>,
        clients: HashMap<Region, tsunami::Machine<'a>>,
    ) -> Self {
        assert_eq!(
            placement.len(),
            servers.len(),
            "placement and servers should have the same cardinality"
        );
        Self {
            placement,
            servers,
            clients,
        }
    }

    pub fn placement(&self) -> &Placement {
        &self.placement
    }

    pub fn servers(
        &self,
    ) -> impl Iterator<Item = (&ProcessId, &tsunami::Machine<'_>)> {
        self.servers.iter()
    }

    pub fn server(&self, process_id: &ProcessId) -> &tsunami::Machine<'_> {
        self.servers
            .get(process_id)
            .expect("server vm should exist")
    }

    pub fn clients(
        &self,
    ) -> impl Iterator<Item = (&Region, &tsunami::Machine<'_>)> {
        self.clients.iter()
    }

    pub fn vms(&self) -> impl Iterator<Item = &tsunami::Machine<'_>> {
        self.servers
            .iter()
            .map(|(_, vm)| vm)
            .chain(self.clients.iter().map(|(_, vm)| vm))
    }

    pub fn server_count(&self) -> usize {
        self.servers.len()
    }

    pub fn client_count(&self) -> usize {
        self.clients.len()
    }

    pub fn vm_count(&self) -> usize {
        self.server_count() + self.client_count()
    }

    pub fn process_region(&self, target_id: &ProcessId) -> &Region {
        for ((region, _shard_id), (process_id, _region_index)) in
            self.placement.iter()
        {
            if target_id == process_id {
                return region;
            }
        }
        panic!("process id should be in placement")
    }

    pub fn processes_in_region(
        &self,
        target_region: &Region,
    ) -> (Vec<ProcessId>, RegionIndex) {
        let mut ids = Vec::new();
        let mut region_indexes = Vec::new();

        // track the ids and region indexes for this region
        for ((region, _shard_id), (process_id, region_index)) in
            self.placement.iter()
        {
            if target_region == region {
                ids.push(*process_id);
                region_indexes.push(*region_index)
            }
        }

        // compute the region index
        region_indexes.sort();
        region_indexes.dedup();
        assert_eq!(
            region_indexes.len(),
            1,
            "there should be a single region index for each region"
        );
        let region_index = region_indexes.remove(0);

        (ids, region_index)
    }

    pub fn sorted_processes(
        &self,
        shard_count: usize,
        n: usize,
        process_id: ProcessId,
    ) -> Vec<ProcessId> {
        // find my region index
        let mut indexes: Vec<_> = self
            .placement
            .values()
            .filter_map(|(peer_id, region_index)| {
                if *peer_id == process_id {
                    Some(*region_index)
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(
            indexes.len(),
            1,
            "there should be a single region index for this processe"
        );
        let my_region_index = indexes.remove(0);

        let mut sorted_processes = Vec::new();
        // make sure we're the first process
        sorted_processes.push(process_id);

        // add the remaining processes
        for region_index in (my_region_index..=n).chain(1..my_region_index) {
            let region_ids = self.placement.values().filter_map(
                |(peer_id, peer_region_index)| {
                    // find all processes that are not self and are jin this
                    // region index
                    if *peer_id != process_id
                        && *peer_region_index == region_index
                    {
                        Some(*peer_id)
                    } else {
                        None
                    }
                },
            );
            sorted_processes.extend(region_ids);
        }

        // check that we have all process ids
        let expected: BTreeSet<_> =
            fantoch::util::all_process_ids(shard_count, n)
                .map(|(process_id, _)| process_id)
                .collect();
        let sorted_ids: BTreeSet<_> =
            sorted_processes.clone().into_iter().collect();
        assert_eq!(
            expected, sorted_ids,
            "sorted processes should contain all ids"
        );

        // return sorted processes
        sorted_processes
    }
}

pub fn fantoch_setup(
    branch: String,
    run_mode: RunMode,
    features: Vec<FantochFeature>,
    testbed: Testbed,
) -> Box<
    dyn for<'r> Fn(
            &'r tsunami::Machine<'_>,
        ) -> Pin<
            Box<dyn Future<Output = Result<(), Report>> + Send + 'r>,
        > + Send
        + Sync
        + 'static,
> {
    Box::new(move |vm| {
        let branch = branch.clone();
        let flamegraph = run_mode.is_flamegraph();
        let aws = testbed.is_aws();
        let features = features
            .clone()
            .into_iter()
            .map(|feature| feature.name())
            .collect::<Vec<_>>()
            .join(",");
        Box::pin(async move {
            // files
            let script_file = "setup.sh";

            // first copy file to the machine
            util::copy_to(SETUP_SCRIPT, (script_file, &vm))
                .await
                .wrap_err("copy_to setup script")?;

            // execute script remotely: "$ setup.sh branch"
            let mut done = false;
            while !done {
                let stdout = util::vm_script_exec(
                    script_file,
                    args![branch, flamegraph, aws, features, "2>&1"],
                    &vm,
                )
                .await?;
                tracing::debug!("full output:\n{}", stdout);
                // check if there was no warning about the packages we need
                let all_available = vec![
                    "build-essential",
                    "pkg-config",
                    "libssl-dev",
                    "chrony",
                    "perf-tools-unstable",
                    "linux-tools-common",
                    "linux-tools-generic",
                    "htop",
                    "dstat",
                    "lsof",
                ]
                .into_iter()
                .all(|package| {
                    let msg = format!("Package {} is not available", package);
                    !stdout.contains(&msg)
                });
                // check if commands we may need are actually installed
                let all_found = vec![
                    "Command 'dstat' not found",
                    "Command 'lsof' not found",
                    "flamegraph: command not found",
                    "chrony: command not found",
                ]
                .into_iter()
                .all(|msg| !stdout.contains(&msg));

                // we're done if no warnings and all commands are actually
                // installed
                done = all_available && all_found;
                if !done {
                    tracing::warn!(
                        "trying again since at least one package was not available"
                    );
                }
            }
            Ok(())
        })
    })
}

pub fn fantoch_bin_script(
    binary: &str,
    args: Vec<String>,
    run_mode: RunMode,
    output_file: impl ToString,
) -> String {
    let binary = run_mode.binary(binary);
    let args = args.join(" ");
    format!("{} {} > {} 2>&1", binary, args, output_file.to_string())
}
