use crate::config::{ClientConfig, ProcessConfig, CLIENT_PORT, PORT};
use crate::util::{self, RunMode};
use color_eyre::Report;
use eyre::WrapErr;
use fantoch::config::Config;
use fantoch::id::ProcessId;
use rusoto_core::Region;
use std::collections::HashMap;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tsunami::providers::aws;
use tsunami::{Machine, Tsunami};

type ClientMetrics = (String, String);

// run mode
const RUN_MODE: RunMode = RunMode::Release;

pub async fn bench_experiment(
    server_instance_type: String,
    client_instance_type: String,
    regions: Vec<Region>,
    max_spot_instance_request_wait_secs: u64,
    max_instance_duration_hours: usize,
    branch: String,
    protocol: String,
    configs: Vec<Config>,
    tracer_show_interval: Option<usize>,
    clients_per_region: Vec<usize>,
    output_log: tokio::fs::File,
) -> Result<(), Report> {
    let mut launcher: aws::Launcher<_> = Default::default();
    let res = do_bench_experiment(
        &mut launcher,
        server_instance_type,
        client_instance_type,
        regions,
        max_spot_instance_request_wait_secs,
        max_instance_duration_hours,
        protocol,
        branch,
        configs,
        tracer_show_interval,
        clients_per_region,
        output_log,
    )
    .await;
    if let Err(e) = &res {
        tracing::warn!("bench experiment error: {:?}", e);
    }
    tracing::info!("will wait 5 minutes before terminating spot instances");
    tokio::time::delay_for(tokio::time::Duration::from_secs(300)).await;
    launcher.terminate_all().await?;
    res
}

async fn do_bench_experiment(
    launcher: &mut aws::Launcher<rusoto_credential::DefaultCredentialsProvider>,
    server_instance_type: String,
    client_instance_type: String,
    regions: Vec<Region>,
    max_spot_instance_request_wait_secs: u64,
    max_instance_duration_hours: usize,
    branch: String,
    protocol: String,
    configs: Vec<Config>,
    tracer_show_interval: Option<usize>,
    clients_per_region: Vec<usize>,
    mut output_log: tokio::fs::File,
) -> Result<(), Report> {
    let (regions, server_vms, client_vms) = spawn(
        launcher,
        server_instance_type,
        client_instance_type,
        regions,
        max_spot_instance_request_wait_secs,
        max_instance_duration_hours,
        branch,
    )
    .await
    .wrap_err("spawn machines")?;

    for config in configs {
        let line = format!(">running {} n = {} | f = {} | tiny = {} | clock_bump_interval = {}ms | skip_fast_ack = {}", protocol, config.n(), config.f(), config.newt_tiny_quorums(), config.newt_clock_bump_interval().unwrap_or(0), config.skip_fast_ack());
        append_to_output_log(&mut output_log, line).await?;

        // check that we have the correct number of regions
        assert_eq!(regions.len(), config.n());
        for &clients in &clients_per_region {
            let run_id = format!(
                "n{}_f{}_tiny{}_interval{}_skipfast_{}clients{}",
                config.n(),
                config.f(),
                config.newt_tiny_quorums(),
                config.newt_clock_bump_interval().unwrap_or(0),
                config.skip_fast_ack(),
                clients,
            );
            let latencies = run_experiment(
                run_id,
                &regions,
                &server_vms,
                &client_vms,
                &protocol,
                config,
                tracer_show_interval,
                clients,
            )
            .await?;

            let metrics =
                vec!["min", "max", "avg", "p95", "p99", "p99.9", "p99.99"];
            let mut latencies_to_avg = HashMap::new();

            // region latency should be something like:
            // - "min=173   max=183   avg=178   p95=183   p99=183 p99.9=183
            //   p99.99=183"
            for (region, region_latency) in latencies {
                let region_latency = region_latency
                    .strip_prefix("latency: ")
                    .expect("client output should start with 'latency: '");
                let line =
                    format!("region = {:<14} | {}", region, region_latency);
                append_to_output_log(&mut output_log, line).await?;

                for entry in region_latency.split_whitespace() {
                    // entry should be something like:
                    // - "min=78"
                    let parts: Vec<_> = entry.split("=").collect();
                    assert_eq!(parts.len(), 2);
                    let metric = parts[0].to_string();
                    let latency = parts[1].parse::<usize>()?;
                    latencies_to_avg
                        .entry(metric)
                        .or_insert_with(Vec::new)
                        .push(latency);
                }
            }

            let mut line =
                format!("n = {} AND c = {:<9} |", config.n(), clients);
            for metric in metrics {
                let latencies = latencies_to_avg
                    .remove(metric)
                    .expect("metric should exist");
                // there should be as many latencies as regions
                assert_eq!(latencies.len(), regions.len());
                let avg = latencies.into_iter().sum::<usize>() / regions.len();
                line = format!("{} {}={:<6}", line, metric, avg);
            }
            append_to_output_log(&mut output_log, line).await?;
        }
    }
    Ok(())
}

async fn run_experiment(
    run_id: String,
    regions: &Vec<String>,
    server_vms: &HashMap<String, Machine<'_>>,
    client_vms: &HashMap<String, Machine<'_>>,
    protocol: &str,
    config: Config,
    tracer_show_interval: Option<usize>,
    clients_per_region: usize,
) -> Result<Vec<ClientMetrics>, Report> {
    // start processes
    let (process_ips, processes) = start_processes(
        regions,
        server_vms,
        protocol,
        config,
        tracer_show_interval,
    )
    .await
    .wrap_err("start_processes")?;

    // run clients
    let client_metrics =
        run_clients(clients_per_region, client_vms, process_ips)
            .await
            .wrap_err("run_clients")?;

    // stop processes
    stop_processes(run_id, server_vms, processes)
        .await
        .wrap_err("stop_processes")?;

    Ok(client_metrics)
}

async fn spawn(
    launcher: &mut aws::Launcher<rusoto_credential::DefaultCredentialsProvider>,
    server_instance_type: String,
    client_instance_type: String,
    regions: Vec<Region>,
    max_spot_instance_request_wait_secs: u64,
    max_instance_duration_hours: usize,
    branch: String,
) -> Result<
    (
        Vec<String>,
        HashMap<String, Machine<'_>>,
        HashMap<String, Machine<'_>>,
    ),
    Report,
> {
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
    )
    .await?;
    let server_vms = vms.remove(server_tag).expect("servers vms");
    let client_vms = vms.remove(client_tag).expect("client vms");
    Ok((regions, server_vms, client_vms))
}

async fn do_spawn(
    launcher: &mut aws::Launcher<rusoto_credential::DefaultCredentialsProvider>,
    tags: Vec<(String, String)>,
    regions: Vec<Region>,
    max_spot_instance_request_wait_secs: u64,
    max_instance_duration_hours: usize,
    branch: String,
) -> Result<(Vec<String>, HashMap<String, HashMap<String, Machine<'_>>>), Report>
{
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
            let setup = aws::Setup::default()
                .instance_type(instance_type.clone())
                .region_with_ubuntu_ami(region.clone())
                .await?
                .setup(util::fantoch_setup(branch.clone(), RUN_MODE));

            // save setup
            descriptors.push((name, setup))
        }
    }

    // spawn and connect
    launcher
        .set_max_instance_duration(max_instance_duration_hours)
        // make sure ports are open
        // TODO just open PORT and CLIENT_PORT?
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

async fn start_processes(
    regions: &Vec<String>,
    vms: &HashMap<String, Machine<'_>>,
    protocol: &str,
    config: Config,
    tracer_show_interval: Option<usize>,
) -> Result<
    (
        HashMap<String, String>,
        HashMap<String, (ProcessId, tokio::process::Child)>,
    ),
    Report,
> {
    let n = config.n();
    // check that we have the correct number of regions and vms
    assert_eq!(regions.len(), n);
    assert_eq!(vms.len(), n);

    let ips: HashMap<_, _> = vms
        .iter()
        .map(|(region, vm)| (region.clone(), vm.public_ip.clone()))
        .collect();
    tracing::debug!("processes ips: {:?}", ips);
    let all_but_self = |region| {
        // select all ips but self
        ips.iter()
            .filter_map(
                |(ip_region, ip)| {
                    if ip_region == region {
                        None
                    } else {
                        Some(ip)
                    }
                },
            )
            .collect::<Vec<_>>()
    };

    let mut processes = HashMap::with_capacity(n);
    let mut wait_processes = Vec::with_capacity(n);

    for (process_id, region) in
        fantoch::util::process_ids(n).zip(regions.iter())
    {
        let vm = vms.get(region).expect("process vm should exist");

        // get ips for this region
        let ips = all_but_self(region)
            .into_iter()
            .map(|ip| {
                let delay = None;
                (ip.clone(), delay)
            })
            .collect();

        // create process config and generate args
        let mut process_config = ProcessConfig::new(process_id, config, ips);
        if let Some(interval) = tracer_show_interval {
            process_config.set_tracer_show_interval(interval);
        }
        let args = process_config.to_args();

        let command = fantoch_bin_script(
            &protocol,
            args,
            RUN_MODE,
            process_file(process_id),
        );
        let process = util::prepare_command(&vm, command)
            .spawn()
            .wrap_err("failed to start process")?;
        processes.insert(region.clone(), (process_id, process));

        wait_processes.push(wait_process_started(process_id, &vm));
    }

    // wait all processse started
    for result in futures::future::join_all(wait_processes).await {
        let () = result?;
    }

    Ok((ips, processes))
}

async fn run_clients(
    clients_per_region: usize,
    vms: &HashMap<String, Machine<'_>>,
    process_ips: HashMap<String, String>,
) -> Result<Vec<ClientMetrics>, Report> {
    let n = vms.len();
    let mut clients = HashMap::with_capacity(n);
    let mut wait_clients = Vec::with_capacity(n);

    for (client_index, (region, vm)) in (1..=n).zip(vms.iter()) {
        // compute id start and id end:
        // - first compute the id end
        // - and then compute id start: subtract `clients_per_machine` and add 1
        let id_end = client_index * clients_per_region;
        let id_start = id_end - clients_per_region + 1;

        // get ip for this region
        let ip = process_ips.get(region).expect("get process ip").clone();

        // create client config and generate args
        let client_config = ClientConfig::new(id_start, id_end, ip);
        let args = client_config.to_args();

        // always run clients on release mode
        let command = fantoch_bin_script(
            "client",
            args,
            RunMode::Release,
            client_file(client_index),
        );
        let client = util::prepare_command(&vm, command)
            .spawn()
            .wrap_err("failed to start client")?;
        clients.insert(client_index, client);

        wait_clients.push(wait_client_ended(client_index, region.clone(), &vm));
    }

    // wait all processse started
    let mut latencies = Vec::with_capacity(n);
    for result in futures::future::join_all(wait_clients).await {
        let latency = result?;
        latencies.push(latency);
    }
    Ok(latencies)
}

async fn stop_processes(
    run_id: String,
    vms: &HashMap<String, Machine<'_>>,
    processes: HashMap<String, (ProcessId, tokio::process::Child)>,
) -> Result<(), Report> {
    // first copy the logs
    for (region, &(process_id, _)) in &processes {
        let vm = vms.get(region).expect("process vm should exist");
        util::copy_from(
            (&process_file(process_id), &vm),
            &format!(".log_{}_id{}_{}", run_id, process_id, region),
        )
        .await
        .wrap_err("copy log")?;
    }
    tracing::info!("copied all process logs");

    let mut wait_processes = Vec::with_capacity(vms.len());
    for (region, (process_id, mut pchild)) in processes {
        let vm = vms.get(&region).expect("process vm should exist");

        // kill ssh process
        if let Err(e) = pchild.kill() {
            tracing::warn!(
                "error trying to kill ssh process {} in region {}: {:?}",
                pchild.id(),
                region,
                e
            );
        }

        // find process pid in remote vm
        // TODO: this should equivalent to `pkill newt_atomic`
        let command =
            format!("lsof -i :{} -i :{} | grep -v PID", PORT, CLIENT_PORT);
        let output = util::exec(vm, command).await.wrap_err("lsof | grep")?;
        tracing::debug!("{}", output);
        let mut pids: Vec<_> = output
            .lines()
            .map(|line| line.split_whitespace().collect::<Vec<_>>()[1])
            .collect();
        pids.sort();
        pids.dedup();

        // there should be at most one pid
        match pids.len() {
            0 => {
                tracing::warn!(
                    "process {} already not running in region {}",
                    process_id,
                    region
                );
            }
            1 => {
                // kill it
                let pid = pids[0];
                let command = format!("kill {}", pid);
                let output = util::exec(vm, command).await.wrap_err("kill")?;
                tracing::debug!("{}", output);
            }
            n => panic!("there should be at most one pid and found {}", n),
        }

        wait_processes.push(wait_process_ended(
            run_id.clone(),
            process_id,
            region,
            vm,
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
    vm: &Machine<'_>,
) -> Result<(), Report> {
    let mut count = 0;
    while count != 1 {
        // small delay between calls
        let seconds = 2;
        tokio::time::delay_for(tokio::time::Duration::from_secs(seconds)).await;

        let command = format!(
            "grep -c 'process {} started' {}",
            process_id,
            process_file(process_id)
        );
        let stdout = util::exec(vm, command).await.wrap_err("grep -c")?;
        count = stdout.parse::<usize>().wrap_err("grep -c parse")?;
    }
    Ok(())
}

async fn wait_client_ended(
    client_index: usize,
    region: String,
    vm: &Machine<'_>,
) -> Result<ClientMetrics, Report> {
    // wait until all clients end
    let mut count = 0;
    while count != 1 {
        // small delay between calls
        let seconds = 5;
        tokio::time::delay_for(tokio::time::Duration::from_secs(seconds)).await;

        let command = format!(
            "grep -c 'all clients ended' {}",
            client_file(client_index)
        );
        let stdout = util::exec(vm, command).await.wrap_err("grep -c")?;
        count = stdout.parse::<usize>().wrap_err("grep -c parse")?;
    }

    // fetch client log
    let command = format!("grep latency {}", client_file(client_index));
    let latency = util::exec(vm, command).await.wrap_err("grep latency")?;
    Ok((region, latency))
}

async fn wait_process_ended(
    run_id: String,
    process_id: ProcessId,
    region: String,
    vm: &Machine<'_>,
) -> Result<(), Report> {
    // small delay between calls
    let delay = tokio::time::Duration::from_secs(2);

    let mut count = 1;
    while count != 0 {
        tokio::time::delay_for(delay).await;
        let command = format!("lsof -i :{} -i :{} | wc -l", PORT, CLIENT_PORT);
        let stdout = util::exec(vm, command).await.wrap_err("lsof | wc")?;
        count = stdout.parse::<usize>().wrap_err("lsof | wc parse")?;
    }
    tracing::info!(
        "process {} in region {} terminated successfully",
        process_id,
        region
    );

    // maybe copy flamegraph
    if RUN_MODE == RunMode::Flamegraph {
        // wait for the flamegraph process to finish writing `flamegraph.svg`
        let mut count = 1;
        while count != 0 {
            tokio::time::delay_for(delay).await;
            let command =
                format!("ps -aux | grep flamegraph | grep -v grep | wc -l");
            let stdout = util::exec(vm, command).await.wrap_err("ps | wc")?;
            count = stdout.parse::<usize>().wrap_err("lsof | wc parse")?;
        }

        // copy `flamegraph.svg`
        util::copy_from(
            ("flamegraph.svg", &vm),
            &format!(".flamegraph_{}_id{}_{}.svg", run_id, process_id, region),
        )
        .await
        .wrap_err("copy flamegraph")?;

        tracing::info!(
            "copied flamegraph log of process {} in region {}",
            process_id,
            region
        );
    }

    Ok(())
}

fn fantoch_bin_script(
    binary: &str,
    args: Vec<String>,
    run_mode: RunMode,
    output_file: String,
) -> String {
    let binary = run_mode.binary(binary);
    let args = args.join(" ");
    format!("{} {} > {} 2>&1", binary, args, output_file)
}

fn process_file(process_id: ProcessId) -> String {
    format!(".log_process_{}", process_id)
}

fn client_file(client_index: usize) -> String {
    format!(".log_client_{}", client_index)
}

async fn append_to_output_log(
    output_log: &mut tokio::fs::File,
    line: String,
) -> Result<(), Report> {
    tracing::info!("{}", line);
    output_log
        .write_all(format!("{}\n", line).as_bytes())
        .await
        .wrap_err("output log write")?;
    output_log.flush().await?;
    Ok(())
}
