use crate::args;
use crate::config::{Placement, RegionIndex};
use crate::{FantochFeature, ProcessType, RunMode, Testbed};
use color_eyre::eyre::WrapErr;
use color_eyre::Report;
use fantoch::id::{ProcessId, ShardId};
use fantoch::planet::Region;
use std::collections::HashMap;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;

const SETUP_SCRIPT: &str = "exp_files/setup.sh";

pub enum Machine<'a> {
    Tsunami(tsunami::Machine<'a>),
    TsunamiRef(&'a tsunami::Machine<'a>),
    Local,
}

impl<'a> Machine<'a> {
    pub fn ip(&self) -> String {
        match self {
            Self::Tsunami(vm) => vm.public_ip.clone(),
            Self::TsunamiRef(vm) => vm.public_ip.clone(),
            Self::Local => String::from("127.0.0.1"),
        }
    }

    pub async fn exec(&self, command: impl ToString) -> Result<String, Report> {
        match self {
            Self::Tsunami(vm) => Self::tsunami_exec(vm, command).await,
            Self::TsunamiRef(vm) => Self::tsunami_exec(vm, command).await,
            Self::Local => {
                Self::exec_command(Self::create_command(command)).await
            }
        }
    }

    pub fn prepare_exec(
        &self,
        command: impl ToString,
    ) -> tokio::process::Command {
        match &self {
            Self::Tsunami(vm) => Self::tsunami_prepare_exec(vm, command),
            Self::TsunamiRef(vm) => Self::tsunami_prepare_exec(vm, command),
            Self::Local => Self::create_command(command),
        }
    }

    pub async fn script_exec(
        &self,
        path: &str,
        args: Vec<String>,
    ) -> Result<String, Report> {
        let args = args.join(" ");
        let command = format!("chmod u+x {} && ./{} {}", path, path, args);
        self.exec(command).await.wrap_err("chmod && ./script")
    }

    pub async fn copy_to(
        &self,
        local_path: impl AsRef<Path>,
        remote_path: impl AsRef<Path>,
    ) -> Result<(), Report> {
        match self {
            Self::Tsunami(vm) => {
                Self::tsunami_copy_to(vm, local_path, remote_path).await
            }
            Self::TsunamiRef(vm) => {
                Self::tsunami_copy_to(vm, local_path, remote_path).await
            }
            Self::Local => Self::local_copy(local_path, remote_path).await,
        }
    }

    pub async fn copy_from(
        &self,
        remote_path: impl AsRef<Path>,
        local_path: impl AsRef<Path>,
    ) -> Result<(), Report> {
        match self {
            Self::Tsunami(vm) => {
                Self::tsunami_copy_from(vm, remote_path, local_path).await
            }
            Self::TsunamiRef(vm) => {
                Self::tsunami_copy_from(vm, remote_path, local_path).await
            }
            Self::Local => Self::local_copy(remote_path, local_path).await,
        }
    }

    async fn tsunami_exec(
        vm: &tsunami::Machine<'_>,
        command: impl ToString,
    ) -> Result<String, Report> {
        Self::ssh_exec(
            vm.username.as_ref(),
            vm.public_ip.as_ref(),
            vm.private_key.as_ref().expect("private key should be set"),
            command,
        )
        .await
    }

    fn tsunami_prepare_exec(
        vm: &tsunami::Machine<'_>,
        command: impl ToString,
    ) -> tokio::process::Command {
        Self::prepare_ssh_exec(
            vm.username.as_ref(),
            vm.public_ip.as_ref(),
            vm.private_key.as_ref().expect("private key should be set"),
            command,
        )
    }

    async fn tsunami_copy_to(
        vm: &tsunami::Machine<'_>,
        local_path: impl AsRef<Path>,
        remote_path: impl AsRef<Path>,
    ) -> Result<(), Report> {
        let from = local_path.as_ref().display();
        let to = format!(
            "{}@{}:{}",
            vm.username,
            vm.public_ip,
            remote_path.as_ref().display()
        );
        let scp_command = format!(
            "scp -o StrictHostKeyChecking=no -i {} {} {}",
            vm.private_key
                .as_ref()
                .expect("private key should be set")
                .as_path()
                .display(),
            from,
            to,
        );
        tracing::debug!("{}", scp_command);
        Self::create_command(scp_command).status().await?;
        Ok(())
    }

    async fn tsunami_copy_from(
        vm: &tsunami::Machine<'_>,
        remote_path: impl AsRef<Path>,
        local_path: impl AsRef<Path>,
    ) -> Result<(), Report> {
        let from = format!(
            "{}@{}:{}",
            vm.username,
            vm.public_ip,
            remote_path.as_ref().display()
        );
        let to = local_path.as_ref().display();
        let scp_command = format!(
            "scp -o StrictHostKeyChecking=no -i {} {} {}",
            vm.private_key
                .as_ref()
                .expect("private key should be set")
                .as_path()
                .display(),
            from,
            to,
        );
        Self::create_command(scp_command).status().await?;
        Ok(())
    }

    async fn local_copy(
        from: impl AsRef<Path>,
        to: impl AsRef<Path>,
    ) -> Result<(), Report> {
        let cp_command =
            format!("cp {} {}", from.as_ref().display(), to.as_ref().display());
        Self::create_command(cp_command).status().await?;
        Ok(())
    }

    pub async fn ssh_exec(
        username: &str,
        public_ip: &str,
        private_key: &std::path::PathBuf,
        command: impl ToString,
    ) -> Result<String, Report> {
        Self::exec_command(Self::prepare_ssh_exec(
            username,
            public_ip,
            private_key,
            command,
        ))
        .await
    }

    fn prepare_ssh_exec(
        username: &str,
        public_ip: &str,
        private_key: &std::path::PathBuf,
        command: impl ToString,
    ) -> tokio::process::Command {
        let ssh_command = format!(
            "ssh -o StrictHostKeyChecking=no -i {} {}@{} {}",
            private_key.as_path().display(),
            username,
            public_ip,
            Self::escape(command)
        );
        Self::create_command(ssh_command)
    }

    fn create_command(command_arg: impl ToString) -> tokio::process::Command {
        let command_arg = command_arg.to_string();
        tracing::debug!("{}", command_arg);
        let mut command = tokio::process::Command::new("bash");
        command.arg("-c");
        command.arg(command_arg);
        command
    }

    async fn exec_command(
        mut command: tokio::process::Command,
    ) -> Result<String, Report> {
        let out = command.output().await.wrap_err("ssh command")?;
        let out = String::from_utf8(out.stdout)
            .wrap_err("output conversion to utf8")?
            .trim()
            .to_string();
        Ok(out)
    }

    fn escape(command: impl ToString) -> String {
        format!("\"{}\"", command.to_string())
    }
}

pub struct Machines<'a> {
    placement: Placement,
    // potentially more than one process machine per region (if partial
    // replication)
    servers: HashMap<ProcessId, Machine<'a>>,
    // only one client machine per region
    clients: HashMap<Region, Machine<'a>>,
}

impl<'a> Machines<'a> {
    pub fn new(
        placement: Placement,
        servers: HashMap<ProcessId, Machine<'a>>,
        clients: HashMap<Region, Machine<'a>>,
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

    pub fn servers(&self) -> impl Iterator<Item = (&ProcessId, &Machine<'_>)> {
        self.servers.iter()
    }

    pub fn server(&self, process_id: &ProcessId) -> &Machine<'_> {
        self.servers
            .get(process_id)
            .expect("server vm should exist")
    }

    pub fn clients(&self) -> impl Iterator<Item = (&Region, &Machine<'_>)> {
        self.clients.iter()
    }

    pub fn vms(&self) -> impl Iterator<Item = &Machine<'_>> {
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
        panic!("process with id {:?} should be in placement", target_id)
    }

    pub fn region_index(&self, target_region: &Region) -> usize {
        for ((region, _shard_id), (_process_id, region_index)) in
            self.placement.iter()
        {
            if target_region == region {
                return *region_index;
            }
        }
        panic!("region {:?} should be in placement", target_region)
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
        shard_id: ShardId,
        region_index: usize,
    ) -> Vec<(ProcessId, ShardId)> {
        let mut sorted_processes = Vec::new();
        // make sure we're the first process
        sorted_processes.push((process_id, shard_id));

        // for each region, add:
        // - all (but self), if same region
        // - the one some my shard, if different region
        for index in (region_index..=n).chain(1..region_index) {
            // select the processes in this region index;
            let index_processes =
                self.placement.iter().filter(|(_, (_, peer_region_index))| {
                    *peer_region_index == index
                });

            let region_ids: Vec<_> = if index == region_index {
                // select all (but self)
                index_processes
                    .filter_map(|((_, peer_shard_id), (peer_id, _))| {
                        if *peer_id != process_id {
                            Some((*peer_id, *peer_shard_id))
                        } else {
                            None
                        }
                    })
                    .collect()
            } else {
                // select the one process from my shard
                index_processes
                    .filter_map(|((_, peer_shard_id), (peer_id, _))| {
                        if *peer_shard_id == shard_id {
                            Some((*peer_id, *peer_shard_id))
                        } else {
                            None
                        }
                    })
                    .collect()
            };
            sorted_processes.extend(region_ids);
        }

        assert_eq!(
            sorted_processes.len(),
            n + shard_count - 1,
            "the number of sorted processes should be n + shards - 1"
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
    assert!(
        matches!(testbed, Testbed::Aws | Testbed::Baremetal),
        "fantoch_setup should only be called with the aws and baremetal testbeds"
    );

    Box::new(move |vm| {
        let vm = Machine::TsunamiRef(vm);
        let testbed = testbed.name();
        let mode = run_mode.name();
        let branch = branch.clone();
        let features = fantoch_features_as_arg(&features);
        Box::pin(async move {
            // files
            let script_file = "setup.sh";

            // first copy file to the machine
            vm.copy_to(SETUP_SCRIPT, script_file)
                .await
                .wrap_err("copy_to setup script")?;

            // execute setup script
            let mut done = false;
            while !done {
                let stdout = vm
                    .script_exec(
                        script_file,
                        args![testbed, mode, branch, features, "2>&1"],
                    )
                    .await?;
                tracing::trace!("full output:\n{}", stdout);
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

pub fn veleta_fantoch_setup() -> Box<
    dyn for<'r> Fn(
        &'r tsunami::Machine<'_>,
    ) -> Pin<
        Box<dyn Future<Output = Result<(), Report>> + Send + 'r>,
    > + Send
        + Sync
        + 'static,
> {
    // TODO: actually, we need to still do some setup on veleta machines like
    // increasing max number of open files, which was done "manually" in the
    // first time
    Box::new(|_| Box::pin(async { Ok(()) }))
}

pub async fn local_fantoch_setup(
    branch: String,
    run_mode: RunMode,
    features: Vec<FantochFeature>,
    testbed: Testbed,
) -> Result<(), Report> {
    assert!(
        testbed == Testbed::Local,
        "local_fantoch_setup should only be called with the local testbed"
    );

    let testbed = testbed.name();
    let mode = run_mode.name();
    let features = fantoch_features_as_arg(&features);
    let vm = Machine::Local;

    // execute setup script
    let stdout = vm
        .script_exec(
            SETUP_SCRIPT,
            args![testbed, mode, branch, features, "2>&1"],
        )
        .await?;
    tracing::trace!("full output:\n{}", stdout);
    Ok(())
}

fn fantoch_features_as_arg(features: &Vec<FantochFeature>) -> String {
    features
        .iter()
        .map(|feature| feature.name())
        .collect::<Vec<_>>()
        .join(",")
}

pub fn fantoch_bin_script(
    process_type: ProcessType,
    binary: &str,
    args: Vec<String>,
    run_mode: RunMode,
    max_log_level: &tracing::Level,
    err_file: impl ToString,
) -> String {
    // binary=info makes sure that we also capture any logs in there
    let env_vars = format!(
        "RUST_LOG={}={},fantoch={},fantoch_ps={}",
        binary, max_log_level, max_log_level, max_log_level,
    );
    let run_command = run_mode.run_command(process_type, &env_vars, binary);
    let args = args.join(" ");
    format!("{} {} > {} 2>&1", run_command, args, err_file.to_string())
}
