use crate::args;
use crate::util;
use crate::{FantochFeature, RunMode, Testbed};
use color_eyre::eyre::WrapErr;
use color_eyre::Report;
use fantoch::id::ProcessId;
use fantoch::planet::Region;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

/// This script should be called like: $ bash script branch
/// - branch: which `fantoch` branch to build
const SETUP_SCRIPT: &str = "exp_files/build.sh";

pub struct Machines<'a> {
    regions: HashMap<Region, ProcessId>,
    servers: HashMap<Region, tsunami::Machine<'a>>,
    clients: HashMap<Region, tsunami::Machine<'a>>,
}

impl<'a> Machines<'a> {
    pub fn new(
        regions: HashMap<Region, ProcessId>,
        servers: HashMap<Region, tsunami::Machine<'a>>,
        clients: HashMap<Region, tsunami::Machine<'a>>,
    ) -> Self {
        Self {
            regions,
            servers,
            clients,
        }
    }

    pub fn regions(&self) -> &HashMap<Region, ProcessId> {
        &self.regions
    }

    pub fn servers(
        &self,
    ) -> impl Iterator<Item = (&Region, &tsunami::Machine<'_>)> {
        self.servers.iter()
    }

    pub fn clients(
        &self,
    ) -> impl Iterator<Item = (&Region, &tsunami::Machine<'_>)> {
        self.clients.iter()
    }

    pub fn vms(&self) -> impl Iterator<Item = &tsunami::Machine<'_>> {
        self.servers
            .iter()
            .chain(self.clients.iter())
            .map(|(_, vm)| vm)
    }

    pub fn region_count(&self) -> usize {
        self.regions.len()
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

    pub fn process_id(&self, region: &Region) -> ProcessId {
        *self
            .regions
            .get(region)
            .expect("region should exist with an assigned process id")
    }

    pub fn server(&self, region: &Region) -> &tsunami::Machine<'_> {
        self.servers.get(region).expect("server vm should exist")
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
