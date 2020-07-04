use crate::config::{
    ClientConfig, ExperimentConfig, ProtocolConfig, CLIENT_PORT, PORT,
};
use crate::exp::{self, Machines, Protocol, RunMode, Testbed};
use crate::util;
use color_eyre::Report;
use eyre::WrapErr;
use fantoch::config::Config;
use fantoch::id::ProcessId;
use fantoch::planet::{Planet, Region};
use std::collections::HashMap;
use std::path::Path;

type Ips = HashMap<Region, String>;

const LOG_FILE: &str = ".log";
const DSTAT_FILE: &str = "dstat.csv";
const METRICS_FILE: &str = ".metrics";

pub async fn bench_experiment(
    machines: Machines<'_>,
    run_mode: RunMode,
    testbed: Testbed,
    planet: Option<Planet>,
    configs: Vec<(Protocol, Config)>,
    tracer_show_interval: Option<usize>,
    clients_per_region: Vec<usize>,
    results_dir: impl AsRef<Path>,
) -> Result<(), Report> {
    if tracer_show_interval.is_some() {
        panic!("vitor: you should set the timing feature for this to work!");
    }

    for (protocol, config) in configs {
        // check that we have the correct number of regions
        assert_eq!(machines.region_count(), config.n());
        for &clients in &clients_per_region {
            run_experiment(
                &machines,
                run_mode,
                testbed,
                &planet,
                protocol,
                config,
                tracer_show_interval,
                clients,
                &results_dir,
            )
            .await?;
        }
    }
    Ok(())
}

async fn run_experiment(
    machines: &Machines<'_>,
    run_mode: RunMode,
    testbed: Testbed,
    planet: &Option<Planet>,
    protocol: Protocol,
    config: Config,
    tracer_show_interval: Option<usize>,
    clients_per_region: usize,
    results_dir: impl AsRef<Path>,
) -> Result<(), Report> {
    // start dstat in all machines
    let dstats = start_dstat(machines).await.wrap_err("start_dstat")?;

    // start processes
    let (process_ips, processes) = start_processes(
        machines,
        run_mode,
        testbed,
        planet,
        protocol,
        config,
        tracer_show_interval,
    )
    .await
    .wrap_err("start_processes")?;

    // run clients
    run_clients(clients_per_region, machines, process_ips)
        .await
        .wrap_err("run_clients")?;

    // stop dstat
    stop_dstat(machines, dstats).await.wrap_err("stop_dstat")?;

    // create experiment config and pull metrics
    let exp_config = ExperimentConfig::new(
        machines.regions().clone(),
        run_mode,
        testbed,
        protocol,
        config,
        clients_per_region,
    );
    let exp_dir = pull_metrics(machines, exp_config, results_dir)
        .await
        .wrap_err("pull_metrics")?;

    // stop processes: should only be stopped after copying all the metrics to
    // avoid unnecessary noise in the logs
    stop_processes(machines, run_mode, exp_dir, processes)
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
) -> Result<(Ips, HashMap<Region, tokio::process::Child>), Report> {
    let n = config.n();
    // check that we have the correct number of regions and vms
    assert_eq!(machines.region_count(), n);
    assert_eq!(machines.server_count(), n);

    let ips: Ips = machines
        .servers()
        .map(|(region, vm)| (region.clone(), vm.public_ip.clone()))
        .collect();
    tracing::debug!("processes ips: {:?}", ips);
    let all_but_self = |region: &Region| {
        // select all ips but self
        ips.iter()
            .filter(|(ip_region, _ip)| ip_region != &region)
            .collect::<Vec<_>>()
    };

    let mut processes = HashMap::with_capacity(n);
    let mut wait_processes = Vec::with_capacity(n);

    for (from_region, &process_id) in machines.regions() {
        let vm = machines.server(from_region);

        // get ips for this region
        let ips = all_but_self(from_region)
            .into_iter()
            .map(|(to_region, ip)| {
                let delay =
                    maybe_inject_delay(from_region, to_region, testbed, planet);
                (ip.clone(), delay)
            })
            .collect();

        // create protocol config and generate args
        let mut protocol_config =
            ProtocolConfig::new(protocol, process_id, config, ips);
        if let Some(interval) = tracer_show_interval {
            protocol_config.set_tracer_show_interval(interval);
        }
        let args = protocol_config.to_args();

        let command = exp::fantoch_bin_script(
            protocol.binary(),
            args,
            run_mode,
            LOG_FILE,
        );
        let process = util::vm_prepare_command(&vm, command)
            .spawn()
            .wrap_err("failed to start process")?;
        processes.insert(from_region.clone(), process);

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
    testbed: Testbed,
    planet: &Option<Planet>,
) -> Option<usize> {
    match testbed {
        Testbed::Aws => {
            // do not inject delay in AWS
            None
        }
        Testbed::Baremetal => {
            // in this case, there should be a planet
            let planet = planet.as_ref().expect("planet not found");
            // find ping latency
            let ping = planet
                .ping_latency(from, to)
                .expect("both regions should be part of the planet");
            // the delay should be half the ping latency
            let delay = (ping / 2) as usize;
            Some(delay)
        }
    }
}

async fn run_clients(
    clients_per_region: usize,
    machines: &Machines<'_>,
    process_ips: Ips,
) -> Result<(), Report> {
    let mut clients = HashMap::with_capacity(machines.client_count());
    let mut wait_clients = Vec::with_capacity(machines.client_count());

    for (region, vm) in machines.clients() {
        // find process id
        let process_id = machines.process_id(region);

        // compute id start and id end:
        // - first compute the id end
        // - and then compute id start: subtract `clients_per_machine` and add 1
        let id_end = process_id as usize * clients_per_region;
        let id_start = id_end - clients_per_region + 1;

        // get ip for this region
        let ip = process_ips.get(region).expect("get process ip").clone();

        // create client config and generate args
        let client_config =
            ClientConfig::new(id_start, id_end, ip, METRICS_FILE);
        let args = client_config.to_args();

        let command = exp::fantoch_bin_script(
            "client",
            args,
            // always run clients on release mode
            RunMode::Release,
            LOG_FILE,
        );
        let client = util::vm_prepare_command(&vm, command)
            .spawn()
            .wrap_err("failed to start client")?;
        clients.insert(process_id, client);

        wait_clients.push(wait_client_ended(process_id, region.clone(), &vm));
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
    exp_dir: String,
    processes: HashMap<Region, tokio::process::Child>,
) -> Result<(), Report> {
    let mut wait_processes = Vec::with_capacity(machines.server_count());
    for (region, mut pchild) in processes {
        // find process id and vm
        let process_id = machines.process_id(&region);
        let vm = machines.server(&region);

        // kill ssh process
        if let Err(e) = pchild.kill() {
            tracing::warn!(
                "error trying to kill ssh process {} in region {:?}: {:?}",
                pchild.id(),
                region,
                e
            );
        }

        // find process pid in remote vm
        // TODO: this should equivalent to `pkill PROTOCOL_BINARY`
        let command =
            format!("lsof -i :{} -i :{} | grep -v PID", PORT, CLIENT_PORT);
        let output =
            util::vm_exec(vm, command).await.wrap_err("lsof | grep")?;
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
                // kill it
                let pid = pids[0];
                let command = format!("kill {}", pid);
                let output =
                    util::vm_exec(vm, command).await.wrap_err("kill")?;
                tracing::debug!("{}", output);
            }
            n => panic!("there should be at most one pid and found {}", n),
        }

        wait_processes.push(wait_process_ended(
            process_id, region, vm, run_mode, &exp_dir,
        ));
    }

    // wait all processse started
    for result in futures::future::join_all(wait_processes).await {
        let () = result?;
    }
    Ok(())
}

async fn wait_process_started(
    process_id: ProcessId,
    vm: &tsunami::Machine<'_>,
) -> Result<(), Report> {
    // small delay between calls
    let duration = tokio::time::Duration::from_secs(2);

    let mut count = 0;
    while count != 1 {
        tokio::time::delay_for(duration).await;
        let command =
            format!("grep -c 'process {} started' {}", process_id, LOG_FILE);
        let stdout = util::vm_exec(vm, &command).await.wrap_err("grep -c")?;
        if stdout.is_empty() {
            tracing::warn!("empty output from: {}", command);
        } else {
            count = stdout.parse::<usize>().wrap_err("grep -c parse")?;
        }
    }
    Ok(())
}

async fn wait_process_ended(
    process_id: ProcessId,
    region: Region,
    vm: &tsunami::Machine<'_>,
    run_mode: RunMode,
    exp_dir: &String,
) -> Result<(), Report> {
    // small delay between calls
    let duration = tokio::time::Duration::from_secs(2);

    let mut count = 1;
    while count != 0 {
        tokio::time::delay_for(duration).await;
        let command = format!("lsof -i :{} -i :{} | wc -l", PORT, CLIENT_PORT);
        let stdout = util::vm_exec(vm, &command).await.wrap_err("lsof | wc")?;
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

    // pull flamegraph if in flamegraph mode
    if run_mode == RunMode::Flamegraph {
        // wait for the flamegraph process to finish writing the flamegraph file
        let mut count = 1;
        while count != 0 {
            tokio::time::delay_for(duration).await;
            let command =
                format!("ps -aux | grep flamegraph | grep -v grep | wc -l");
            let stdout =
                util::vm_exec(vm, &command).await.wrap_err("ps | wc")?;
            if stdout.is_empty() {
                tracing::warn!("empty output from: {}", command);
            } else {
                count = stdout.parse::<usize>().wrap_err("lsof | wc parse")?;
            }
        }

        // once the flamegraph process is not running, we can grab the
        // flamegraph file
        pull_flamegraph_file("server", &region, vm, exp_dir)
            .await
            .wrap_err("pull_flamegraph_file")?;
    }
    Ok(())
}

async fn wait_client_ended(
    process_id: ProcessId,
    region: Region,
    vm: &tsunami::Machine<'_>,
) -> Result<(), Report> {
    // small delay between calls
    let duration = tokio::time::Duration::from_secs(10);

    let mut count = 0;
    while count != 1 {
        tokio::time::delay_for(duration).await;
        let command = format!("grep -c 'all clients ended' {}", LOG_FILE);
        let stdout = util::vm_exec(vm, &command).await.wrap_err("grep -c")?;
        if stdout.is_empty() {
            tracing::warn!("empty output from: {}", command);
        } else {
            count = stdout.parse::<usize>().wrap_err("grep -c parse")?;
        }
    }

    tracing::info!(
        "client {} in region {:?} terminated successfully",
        process_id,
        region
    );

    Ok(())
}

async fn start_dstat(
    machines: &Machines<'_>,
) -> Result<Vec<tokio::process::Child>, Report> {
    let command = format!(
        "dstat -t -T -cdnm --io --output {} 1 > /dev/null",
        DSTAT_FILE
    );

    let mut dstats = Vec::with_capacity(machines.vm_count());
    // start dstat in both server and client machines
    for vm in machines.vms() {
        let dstat = util::vm_prepare_command(&vm, command.clone())
            .spawn()
            .wrap_err("failed to start dstat")?;
        dstats.push(dstat);
    }

    Ok(dstats)
}

async fn stop_dstat(
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
        let output = util::vm_exec(vm, command).await.wrap_err("ps")?;
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
                let output =
                    util::vm_exec(vm, command).await.wrap_err("kill")?;
                tracing::debug!("{}", output);
            }
        }
    }

    for vm in machines.vms() {
        check_no_dstat(vm).await.wrap_err("check_no_dstat")?;
    }
    Ok(())
}

async fn check_no_dstat(vm: &tsunami::Machine<'_>) -> Result<(), Report> {
    let command = "ps -aux | grep dstat | grep -v grep | wc -l";
    loop {
        let stdout = util::vm_exec(vm, &command).await?;
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
    for (region, vm) in machines.servers() {
        let pull_metrics = false;
        pulls.push(pull_metrics_files(
            "server",
            region,
            vm,
            &exp_dir,
            pull_metrics,
        ));
    }
    for (region, vm) in machines.clients() {
        let pull_metrics = true;
        pulls.push(pull_metrics_files(
            "client",
            region,
            vm,
            &exp_dir,
            pull_metrics,
        ));
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
    util::serialize(exp_config, format!("{}/exp_config.bincode", exp_dir))?;
    Ok(exp_dir)
}

fn exp_timestamp() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("we're way past epoch")
        .as_micros()
}

async fn pull_metrics_files(
    tag: &str,
    region: &Region,
    vm: &tsunami::Machine<'_>,
    exp_dir: &String,
    pull_metrics: bool,
) -> Result<(), Report> {
    // pull log file and remove it
    let local_path = format!("{}/{}_{:?}.log", exp_dir, tag, region);
    util::copy_from((LOG_FILE, vm), local_path)
        .await
        .wrap_err("copy log")?;

    // pull dstat and remove it
    let local_path = format!("{}/{}_{:?}_dstat.csv", exp_dir, tag, region);
    util::copy_from((DSTAT_FILE, vm), local_path)
        .await
        .wrap_err("copy dstat")?;

    // files to be removed
    let mut to_remove = format!("rm {} {}", LOG_FILE, DSTAT_FILE);

    if pull_metrics {
        // pull metrics file and remove it
        let local_path =
            format!("{}/{}_{:?}_metrics.bincode", exp_dir, tag, region);
        util::copy_from((METRICS_FILE, vm), local_path)
            .await
            .wrap_err("copy metrics")?;

        // also remove metrics file
        to_remove = format!("{} {}", to_remove, METRICS_FILE);
    }

    // remove files
    util::vm_exec(vm, to_remove)
        .await
        .wrap_err("remove files")?;

    Ok(())
}

async fn pull_flamegraph_file(
    tag: &str,
    region: &Region,
    vm: &tsunami::Machine<'_>,
    exp_dir: &String,
) -> Result<(), Report> {
    let local_path = format!("{}/{}_{:?}_flamegraph.svg", exp_dir, tag, region);
    util::copy_from(("flamegraph.svg", vm), local_path)
        .await
        .wrap_err("copy flamegraph")?;
    Ok(())
}
