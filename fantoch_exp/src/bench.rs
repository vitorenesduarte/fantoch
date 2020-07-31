use crate::config::{
    self, ClientConfig, ExperimentConfig, ProcessType, ProtocolConfig,
    RegionIndex,
};
use crate::machine::{Machine, Machines};
use crate::{FantochFeature, Protocol, RunMode, SerializationFormat, Testbed};
use color_eyre::eyre::{self, WrapErr};
use color_eyre::Report;
use fantoch::client::Workload;
use fantoch::config::Config;
use fantoch::id::ProcessId;
use fantoch::planet::{Planet, Region};
use std::collections::HashMap;
use std::path::Path;

type Ips = HashMap<ProcessId, String>;

const LOG_FILE_EXT: &str = "log";
const DSTAT_FILE_EXT: &str = "dstat.csv";
const METRICS_FILE_EXT: &str = "metrics";
pub(crate) const FLAMEGRAPH_FILE_EXT: &str = "flamegraph.svg";

pub async fn bench_experiment(
    machines: Machines<'_>,
    run_mode: RunMode,
    features: Vec<FantochFeature>,
    testbed: Testbed,
    planet: Option<Planet>,
    configs: Vec<(Protocol, Config)>,
    tracer_show_interval: Option<usize>,
    clients_per_region: Vec<usize>,
    workloads: Vec<Workload>,
    skip: impl Fn(Protocol, Config, usize) -> bool,
    results_dir: impl AsRef<Path>,
) -> Result<(), Report> {
    if tracer_show_interval.is_some() {
        panic!("vitor: you should set the 'prof' feature for this to work!");
    }

    for workload in workloads {
        for &clients in &clients_per_region {
            for &(protocol, config) in &configs {
                // check that we have the correct number of server machines
                assert_eq!(
                    machines.server_count(),
                    config.n() * config.shards(),
                    "not enough server machines"
                );

                // check that we have the correct number of client machines
                assert_eq!(
                    machines.client_count(),
                    config.n(),
                    "not enough client machines"
                );

                // maybe skip configuration
                if skip(protocol, config, clients) {
                    continue;
                }
                run_experiment(
                    &machines,
                    run_mode,
                    features.clone(),
                    testbed,
                    &planet,
                    protocol,
                    config,
                    tracer_show_interval,
                    clients,
                    workload,
                    &results_dir,
                )
                .await?;
            }
        }
    }
    Ok(())
}

async fn run_experiment(
    machines: &Machines<'_>,
    run_mode: RunMode,
    features: Vec<FantochFeature>,
    testbed: Testbed,
    planet: &Option<Planet>,
    protocol: Protocol,
    config: Config,
    tracer_show_interval: Option<usize>,
    clients_per_region: usize,
    workload: Workload,
    results_dir: impl AsRef<Path>,
) -> Result<(), Report> {
    // holder of dstat processes to be launched in all machines
    let mut dstats = Vec::with_capacity(machines.vm_count());

    // start processes
    let (process_ips, processes) = start_processes(
        machines,
        run_mode,
        testbed,
        planet,
        protocol,
        config,
        tracer_show_interval,
        &mut dstats,
    )
    .await
    .wrap_err("start_processes")?;

    // run clients
    run_clients(
        clients_per_region,
        workload,
        machines,
        process_ips,
        &mut dstats,
    )
    .await
    .wrap_err("run_clients")?;

    // stop dstat
    stop_dstats(machines, dstats).await.wrap_err("stop_dstat")?;

    // create experiment config and pull metrics
    let exp_config = ExperimentConfig::new(
        machines.placement().clone(),
        planet.clone(),
        run_mode,
        features,
        testbed,
        protocol,
        config,
        clients_per_region,
        workload,
    );
    let exp_dir = pull_metrics(machines, exp_config, results_dir)
        .await
        .wrap_err("pull_metrics")?;

    // stop processes: should only be stopped after copying all the metrics to
    // avoid unnecessary noise in the logs
    stop_processes(machines, run_mode, protocol, exp_dir, processes)
        .await
        .wrap_err("stop_processes")?;

    Ok(())
}

async fn start_processes(
    machines: &Machines<'_>,
    run_mode: RunMode,
    testbed: Testbed,
    planet: &Option<Planet>,
    protocol: Protocol,
    config: Config,
    tracer_show_interval: Option<usize>,
    dstats: &mut Vec<tokio::process::Child>,
) -> Result<(Ips, HashMap<ProcessId, (Region, tokio::process::Child)>), Report>
{
    let ips: Ips = machines
        .servers()
        .map(|(process_id, vm)| (*process_id, vm.ip()))
        .collect();
    tracing::debug!("processes ips: {:?}", ips);

    let process_count = config.n() * config.shards();
    let mut processes = HashMap::with_capacity(process_count);
    let mut wait_processes = Vec::with_capacity(process_count);

    for ((from_region, shard_id), (process_id, region_index)) in
        machines.placement()
    {
        let vm = machines.server(process_id);

        // compute the set of sorted processes
        let sorted = machines.sorted_processes(
            config.shards(),
            config.n(),
            *process_id,
            *shard_id,
            *region_index,
        );

        // get ips to connect to (based on sorted)
        let ips = sorted
            .iter()
            .filter(|peer_id| *peer_id != process_id)
            .map(|peer_id| {
                // get process ip
                let ip = ips
                    .get(peer_id)
                    .expect("all processes should have an ip")
                    .clone();
                // compute delay to be injected (if theres's a `planet`)
                let to_region = machines.process_region(peer_id);
                let delay = maybe_inject_delay(from_region, to_region, planet);
                (*peer_id, ip, delay)
            })
            .collect();

        // set sorted only if on baremetal and no delay will be injected
        let set_sorted = testbed == Testbed::Baremetal && planet.is_none();
        let sorted = if set_sorted { Some(sorted) } else { None };

        // compute process type
        let process_type = ProcessType::Server(*process_id);

        // compute files to be generated during this run
        let log_file = config::run_file(process_type, LOG_FILE_EXT);
        let dstat_file = config::run_file(process_type, DSTAT_FILE_EXT);
        let metrics_file = config::run_file(process_type, METRICS_FILE_EXT);

        // start dstat and save it
        let dstat = start_dstat(dstat_file, vm).await?;
        dstats.push(dstat);

        // create protocol config and generate args
        let mut protocol_config = ProtocolConfig::new(
            protocol,
            *process_id,
            *shard_id,
            config,
            sorted,
            ips,
            metrics_file,
        );
        if let Some(interval) = tracer_show_interval {
            protocol_config.set_tracer_show_interval(interval);
        }
        let args = protocol_config.to_args();

        let command = crate::machine::fantoch_bin_script(
            process_type,
            protocol.binary(),
            args,
            run_mode,
            log_file,
        );
        let process = vm
            .prepare_exec(command)
            .spawn()
            .wrap_err("failed to start process")?;
        processes.insert(*process_id, (from_region.clone(), process));

        wait_processes.push(wait_process_started(process_id, &vm));
    }

    // wait all processse started
    for result in futures::future::join_all(wait_processes).await {
        let () = result?;
    }

    Ok((ips, processes))
}

fn maybe_inject_delay(
    from: &Region,
    to: &Region,
    planet: &Option<Planet>,
) -> Option<usize> {
    // inject delay if a planet was provided
    planet.as_ref().map(|planet| {
        // find ping latency
        let ping = planet
            .ping_latency(from, to)
            .expect("both regions should be part of the planet");
        // the delay should be half the ping latency
        (ping / 2) as usize
    })
}

async fn run_clients(
    clients_per_region: usize,
    workload: Workload,
    machines: &Machines<'_>,
    process_ips: Ips,
    dstats: &mut Vec<tokio::process::Child>,
) -> Result<(), Report> {
    let mut clients = HashMap::with_capacity(machines.client_count());
    let mut wait_clients = Vec::with_capacity(machines.client_count());

    for (region, vm) in machines.clients() {
        // find all processes in this region (we have more than one there's more
        // than one shard)
        let (processes_in_region, region_index) =
            machines.processes_in_region(region);

        // compute id start and id end:
        // - first compute the id end
        // - and then compute id start: subtract `clients_per_machine` and add 1
        let id_end = region_index as usize * clients_per_region;
        let id_start = id_end - clients_per_region + 1;

        // get ips of all processes in this region
        let ips = processes_in_region
            .iter()
            .map(|process_id| {
                let ip = process_ips
                    .get(process_id)
                    .expect("process should have ip")
                    .clone();
                (*process_id, ip)
            })
            .collect();

        // compute process type
        let process_type = ProcessType::Client(region_index);

        // compute files to be generated during this run
        let log_file = config::run_file(process_type, LOG_FILE_EXT);
        let dstat_file = config::run_file(process_type, DSTAT_FILE_EXT);
        let metrics_file = config::run_file(process_type, METRICS_FILE_EXT);

        // start dstat and save it
        let dstat = start_dstat(dstat_file, vm).await?;
        dstats.push(dstat);

        // create client config and generate args
        let client_config =
            ClientConfig::new(id_start, id_end, ips, workload, metrics_file);
        let args = client_config.to_args();

        let command = crate::machine::fantoch_bin_script(
            process_type,
            "client",
            args,
            // always run clients on release mode
            RunMode::Release,
            log_file,
        );
        let client = vm
            .prepare_exec(command)
            .spawn()
            .wrap_err("failed to start client")?;
        clients.insert(region_index, client);

        wait_clients.push(wait_client_ended(region_index, region.clone(), &vm));
    }

    // wait all clients ended
    for result in futures::future::join_all(wait_clients).await {
        let _ = result.wrap_err("wait_client_ended")?;
    }
    Ok(())
}

async fn stop_processes(
    machines: &Machines<'_>,
    run_mode: RunMode,
    protocol: Protocol,
    exp_dir: String,
    processes: HashMap<ProcessId, (Region, tokio::process::Child)>,
) -> Result<(), Report> {
    let mut wait_processes = Vec::with_capacity(machines.server_count());
    for (process_id, (region, mut pchild)) in processes {
        // find vm
        let vm = machines.server(&process_id);

        let heaptrack_pid = if let RunMode::Heaptrack = run_mode {
            // find heaptrack pid if in heaptrack mode
            let command = format!(
                "ps -aux | grep heaptrack | grep ' \\-\\-id {}' | grep -v 'bash -c'",
                process_id
            );
            let heaptrack_process =
                vm.exec(command).await.wrap_err("ps heaptrack")?;
            tracing::debug!("{}: {}", process_id, heaptrack_process);

            // check that there's a single heaptrack processs
            let lines: Vec<_> = heaptrack_process.split("\n").collect();
            assert_eq!(
                lines.len(),
                1,
                "there should be a single heaptrack process"
            );

            // compute heaptrack pid
            let parts: Vec<_> = lines[0].split_whitespace().collect();
            tracing::debug!("{}: parts {:?}", process_id, parts);
            let heaptrack_pid = parts[1]
                .parse::<u32>()
                .expect("heaptrack pid should be a number");
            Some(heaptrack_pid)
        } else {
            None
        };

        // kill ssh process
        if let Err(e) = pchild.kill() {
            tracing::warn!(
                "error trying to kill ssh process {} with pid {}: {:?}",
                process_id,
                pchild.id(),
                e
            );
        }

        // find process pid in remote vm
        // TODO: this should equivalent to `pkill PROTOCOL_BINARY`
        let command = format!(
            "lsof -i :{} -i :{} -sTCP:LISTEN | grep -v PID",
            config::port(process_id),
            config::client_port(process_id)
        );
        let output = vm.exec(command).await.wrap_err("lsof | grep")?;
        let mut pids: Vec<_> = output
            .lines()
            // take the second column (which contains the PID)
            .map(|line| line.split_whitespace().collect::<Vec<_>>()[1])
            .collect();
        pids.sort();
        pids.dedup();

        // there should be at most one pid
        match pids.len() {
            0 => {
                tracing::warn!(
                    "process {} already not running in region {:?}",
                    process_id,
                    region
                );
            }
            1 => {
                // kill all
                let command = format!("kill {}", pids.join(" "));
                let output = vm.exec(command).await.wrap_err("kill")?;
                tracing::debug!("{}", output);
            }
            n => panic!("there should be at most one pid and found {}", n),
        }

        wait_processes.push(wait_process_ended(
            protocol,
            heaptrack_pid,
            process_id,
            region,
            vm,
            run_mode,
            &exp_dir,
        ));
    }

    // wait all processse started
    for result in futures::future::join_all(wait_processes).await {
        let () = result?;
    }
    Ok(())
}

async fn wait_process_started(
    process_id: &ProcessId,
    vm: &Machine<'_>,
) -> Result<(), Report> {
    // small delay between calls
    let duration = tokio::time::Duration::from_secs(2);

    // compute process type and log file
    let process_type = ProcessType::Server(*process_id);
    let log_file = config::run_file(process_type, LOG_FILE_EXT);

    let mut count = 0;
    while count != 1 {
        tokio::time::delay_for(duration).await;
        let command =
            format!("grep -c 'process {} started' {}", process_id, log_file);
        let stdout = vm.exec(&command).await.wrap_err("grep -c")?;
        if stdout.is_empty() {
            tracing::warn!("empty output from: {}", command);
        } else {
            count = stdout.parse::<usize>().wrap_err("grep -c parse")?;
        }
    }
    Ok(())
}

async fn wait_process_ended(
    protocol: Protocol,
    heaptrack_pid: Option<u32>,
    process_id: ProcessId,
    region: Region,
    vm: &Machine<'_>,
    run_mode: RunMode,
    exp_dir: &str,
) -> Result<(), Report> {
    // small delay between calls
    let duration = tokio::time::Duration::from_secs(2);

    let mut count = 1;
    while count != 0 {
        tokio::time::delay_for(duration).await;
        let command = format!(
            "lsof -i :{} -i :{} | wc -l",
            config::port(process_id),
            config::client_port(process_id)
        );
        let stdout = vm.exec(&command).await.wrap_err("lsof | wc")?;
        if stdout.is_empty() {
            tracing::warn!("empty output from: {}", command);
        } else {
            count = stdout.parse::<usize>().wrap_err("lsof | wc parse")?;
        }
    }

    tracing::info!(
        "process {} in region {:?} terminated successfully",
        process_id,
        region
    );

    // compute process type
    let process_type = ProcessType::Server(process_id);

    // pull aditional files
    match run_mode {
        RunMode::Release => {
            // nothing to do in this case
        }
        RunMode::Flamegraph => {
            // wait for the flamegraph process to finish writing the flamegraph
            // file
            let mut count = 1;
            while count != 0 {
                tokio::time::delay_for(duration).await;
                let command =
                    "ps -aux | grep flamegraph | grep -v grep | wc -l"
                        .to_string();
                let stdout = vm.exec(&command).await.wrap_err("ps | wc")?;
                if stdout.is_empty() {
                    tracing::warn!("empty output from: {}", command);
                } else {
                    count =
                        stdout.parse::<usize>().wrap_err("lsof | wc parse")?;
                }
            }

            // once the flamegraph process is not running, we can grab the
            // flamegraph file
            pull_flamegraph_file(process_type, &region, vm, exp_dir)
                .await
                .wrap_err("pull_flamegraph_file")?;
        }
        RunMode::Heaptrack => {
            let heaptrack_pid =
                heaptrack_pid.expect("heaptrack pid should be set");
            pull_heaptrack_file(
                protocol,
                heaptrack_pid,
                process_type,
                &region,
                vm,
                exp_dir,
            )
            .await
            .wrap_err("pull_heaptrack_file")?;
        }
    }
    Ok(())
}

async fn wait_client_ended(
    region_index: RegionIndex,
    region: Region,
    vm: &Machine<'_>,
) -> Result<(), Report> {
    // small delay between calls
    let duration = tokio::time::Duration::from_secs(10);

    // compute process type and log file
    let process_type = ProcessType::Client(region_index);
    let log_file = config::run_file(process_type, LOG_FILE_EXT);

    let mut count = 0;
    while count != 1 {
        tokio::time::delay_for(duration).await;
        let command = format!("grep -c 'all clients ended' {}", log_file);
        let stdout = vm.exec(&command).await.wrap_err("grep -c")?;
        if stdout.is_empty() {
            tracing::warn!("empty output from: {}", command);
        } else {
            count = stdout.parse::<usize>().wrap_err("grep -c parse")?;
        }
    }

    tracing::info!(
        "client {} in region {:?} terminated successfully",
        region_index,
        region
    );

    Ok(())
}

async fn start_dstat(
    dstat_file: String,
    vm: &Machine<'_>,
) -> Result<tokio::process::Child, Report> {
    let command = format!(
        "dstat -t -T -cdnm --io --output {} 1 > /dev/null",
        dstat_file
    );
    vm.prepare_exec(command)
        .spawn()
        .wrap_err("failed to start dstat")
}

async fn stop_dstats(
    machines: &Machines<'_>,
    dstats: Vec<tokio::process::Child>,
) -> Result<(), Report> {
    for mut dstat in dstats {
        // kill ssh process
        dstat.kill().wrap_err("dstat kill")?;
        if let Err(e) = dstat.kill() {
            tracing::warn!(
                "error trying to kill ssh dstat {}: {:?}",
                dstat.id(),
                e
            );
        }
    }

    for vm in machines.vms() {
        // find dstat pid in remote vm
        let command = "ps -aux | grep dstat | grep -v grep";
        let output = vm.exec(command).await.wrap_err("ps")?;
        let mut pids: Vec<_> = output
            .lines()
            // take the second column (which contains the PID)
            .map(|line| line.split_whitespace().collect::<Vec<_>>()[1])
            .collect();
        pids.sort();
        pids.dedup();

        // there should be at most one pid
        match pids.len() {
            0 => {
                tracing::warn!("dstat already not running");
            }
            n => {
                if n > 2 {
                    // there should be `bash -c dstat` and a `python2
                    // /usr/bin/dstat`; if more than these two, then there's
                    // more than one dstat running
                    tracing::warn!(
                        "found more than one dstat. killing all of them"
                    );
                }
                // kill dstat
                let command = format!("kill {}", pids.join(" "));
                let output = vm.exec(command).await.wrap_err("kill")?;
                tracing::debug!("{}", output);
            }
        }
    }

    for vm in machines.vms() {
        check_no_dstat(vm).await.wrap_err("check_no_dstat")?;
    }
    Ok(())
}

async fn check_no_dstat(vm: &Machine<'_>) -> Result<(), Report> {
    let command = "ps -aux | grep dstat | grep -v grep | wc -l";
    loop {
        let stdout = vm.exec(&command).await?;
        if stdout.is_empty() {
            tracing::warn!("empty output from: {}", command);
            // check again
            continue;
        } else {
            let count = stdout.parse::<usize>().wrap_err("wc -c parse")?;
            if count != 0 {
                eyre::bail!("dstat shouldn't be running")
            } else {
                return Ok(());
            }
        }
    }
}

async fn pull_metrics(
    machines: &Machines<'_>,
    exp_config: ExperimentConfig,
    results_dir: impl AsRef<Path>,
) -> Result<String, Report> {
    // save experiment config, making sure experiment directory exists
    let exp_dir = save_exp_config(exp_config, results_dir)
        .await
        .wrap_err("save_exp_config")?;
    tracing::info!("experiment metrics will be saved in {}", exp_dir);

    let mut pulls = Vec::with_capacity(machines.vm_count());
    // prepare server metrics pull
    for (process_id, vm) in machines.servers() {
        // compute region and process type
        let region = machines.process_region(process_id);
        let process_type = ProcessType::Server(*process_id);
        pulls.push(pull_metrics_files(process_type, region, vm, &exp_dir));
    }
    // prepare client metrics pull
    for (region, vm) in machines.clients() {
        // compute region index and process type
        let region_index = machines.region_index(region);
        let process_type = ProcessType::Client(region_index);
        pulls.push(pull_metrics_files(process_type, region, vm, &exp_dir));
    }

    // pull all metrics in parallel
    for result in futures::future::join_all(pulls).await {
        let _ = result.wrap_err("pull_metrics")?;
    }

    Ok(exp_dir)
}

async fn save_exp_config(
    exp_config: ExperimentConfig,
    results_dir: impl AsRef<Path>,
) -> Result<String, Report> {
    let timestamp = exp_timestamp();
    let exp_dir = format!("{}/{}", results_dir.as_ref().display(), timestamp);
    tokio::fs::create_dir_all(&exp_dir)
        .await
        .wrap_err("create_dir_all")?;

    // save config file
    crate::serialize(
        exp_config,
        format!("{}/exp_config.json", exp_dir),
        SerializationFormat::Json,
    )?;
    Ok(exp_dir)
}

fn exp_timestamp() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("we're way past epoch")
        .as_micros()
}

async fn pull_metrics_files(
    process_type: ProcessType,
    region: &Region,
    vm: &Machine<'_>,
    exp_dir: &str,
) -> Result<(), Report> {
    // compute filename prefix
    let prefix = config::file_prefix(process_type, region);

    // compute files to be pulled
    let log_file = config::run_file(process_type, LOG_FILE_EXT);
    let dstat_file = config::run_file(process_type, DSTAT_FILE_EXT);
    let metrics_file = config::run_file(process_type, METRICS_FILE_EXT);

    // pull log file and remove it
    let local_path = format!("{}/{}.log", exp_dir, prefix);
    vm.copy_from(&log_file, local_path)
        .await
        .wrap_err("copy log")?;

    // pull dstat and remove it
    let local_path = format!("{}/{}_dstat.csv", exp_dir, prefix);
    vm.copy_from(&dstat_file, local_path)
        .await
        .wrap_err("copy dstat")?;

    // pull metrics file and remove it
    let local_path = format!("{}/{}_metrics.bincode.gz", exp_dir, prefix);
    vm.copy_from(&metrics_file, local_path)
        .await
        .wrap_err("copy metrics")?;

    // remove metric files:
    // - note that in the case of `Process::Server`, the metrics file is
    //   generated periodic, and thus, remove it makes little sense
    let to_remove = format!("rm {} {} {}", log_file, dstat_file, metrics_file);
    vm.exec(to_remove).await.wrap_err("remove files")?;

    match process_type {
        ProcessType::Server(process_id) => {
            tracing::info!(
                "all process {:?} metric files pulled in region {:?}",
                process_id,
                region
            );
        }
        ProcessType::Client(_) => {
            tracing::info!(
                "all client metric files pulled in region {:?}",
                region
            );
        }
    }

    Ok(())
}

async fn pull_flamegraph_file(
    process_type: ProcessType,
    region: &Region,
    vm: &Machine<'_>,
    exp_dir: &str,
) -> Result<(), Report> {
    // compute flamegraph file
    let flamegraph_file = config::run_file(process_type, FLAMEGRAPH_FILE_EXT);

    // compute filename prefix
    let prefix = config::file_prefix(process_type, region);
    let local_path = format!("{}/{}_flamegraph.svg", exp_dir, prefix);
    vm.copy_from(&flamegraph_file, local_path)
        .await
        .wrap_err("copy flamegraph")?;

    // remove flamegraph file
    let command = format!("rm {}", flamegraph_file);
    vm.exec(command).await.wrap_err("remove flamegraph ile")?;
    Ok(())
}

async fn pull_heaptrack_file(
    protocol: Protocol,
    heaptrack_pid: u32,
    process_type: ProcessType,
    region: &Region,
    vm: &Machine<'_>,
    exp_dir: &str,
) -> Result<(), Report> {
    // compute heaptrack filename: heaptrack.BINARY.PID.gz
    let heaptrack =
        format!("heaptrack.{}.{}.gz", protocol.binary(), heaptrack_pid);

    // compute filename prefix
    let prefix = config::file_prefix(process_type, region);
    let local_path = format!("{}/{}_heaptrack.gz", exp_dir, prefix);
    vm.copy_from(&heaptrack, local_path)
        .await
        .wrap_err("copy heaptrack")?;

    // remove heaptrack file
    let command = format!("rm {}", heaptrack);
    vm.exec(command).await.wrap_err("remove heaptrack file")?;
    Ok(())
}
