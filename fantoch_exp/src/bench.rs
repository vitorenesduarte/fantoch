use crate::config::{ClientConfig, ProtocolConfig, CLIENT_PORT, PORT};
use crate::exp::{self, Machines, Protocol, RunMode, Testbed};
use crate::util;
use color_eyre::Report;
use eyre::WrapErr;
use fantoch::config::Config;
use fantoch::id::ProcessId;
use fantoch::planet::{Planet, Region};
use std::collections::HashMap;
use tokio::io::AsyncWriteExt;

type ClientMetrics = (Region, String);

pub async fn bench_experiment(
    machines: Machines<'_>,
    run_mode: RunMode,
    testbed: Testbed,
    planet: Option<Planet>,
    protocol: Protocol,
    configs: Vec<Config>,
    tracer_show_interval: Option<usize>,
    clients_per_region: Vec<usize>,
    mut output_log: tokio::fs::File,
) -> Result<(), Report> {
    if tracer_show_interval.is_some() {
        panic!("vitor: you should set the timing feature for this to work!");
    }

    for config in configs {
        let line = format!(">running {} n = {} | f = {} | tiny = {} | clock_bump_interval = {}ms | skip_fast_ack = {}", protocol.binary(), config.n(), config.f(), config.newt_tiny_quorums(), config.newt_clock_bump_interval().unwrap_or(0), config.skip_fast_ack());
        append_to_output_log(&mut output_log, line).await?;

        // check that we have the correct number of regions
        assert_eq!(machines.regions.len(), config.n());
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
                &machines,
                run_id,
                run_mode,
                testbed,
                &planet,
                protocol,
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
                let line = format!(
                    "region = {:<14} | {}",
                    region.name(),
                    region_latency
                );
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
                assert_eq!(latencies.len(), machines.regions.len());
                let avg = latencies.into_iter().sum::<usize>()
                    / machines.regions.len();
                line = format!("{} {}={:<6}", line, metric, avg);
            }
            append_to_output_log(&mut output_log, line).await?;
        }
    }
    Ok(())
}

async fn run_experiment(
    machines: &Machines<'_>,
    run_id: String,
    run_mode: RunMode,
    testbed: Testbed,
    planet: &Option<Planet>,
    protocol: Protocol,
    config: Config,
    tracer_show_interval: Option<usize>,
    clients_per_region: usize,
) -> Result<Vec<ClientMetrics>, Report> {
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
    let client_metrics = run_clients(clients_per_region, machines, process_ips)
        .await
        .wrap_err("run_clients")?;

    // stop processes
    stop_processes(run_id, run_mode, machines, processes)
        .await
        .wrap_err("stop_processes")?;

    Ok(client_metrics)
}

async fn start_processes(
    machines: &Machines<'_>,
    run_mode: RunMode,
    testbed: Testbed,
    planet: &Option<Planet>,
    protocol: Protocol,
    config: Config,
    tracer_show_interval: Option<usize>,
) -> Result<
    (
        HashMap<Region, String>,
        HashMap<Region, (ProcessId, tokio::process::Child)>,
    ),
    Report,
> {
    let n = config.n();
    // check that we have the correct number of regions and vms
    assert_eq!(machines.regions.len(), n);
    assert_eq!(machines.servers.len(), n);

    let ips: HashMap<_, _> = machines
        .servers
        .iter()
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

    for (process_id, from_region) in
        fantoch::util::process_ids(n).zip(machines.regions.iter())
    {
        let vm = machines
            .servers
            .get(from_region)
            .expect("process vm should exist");

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
            process_file(process_id),
        );
        let process = util::vm_prepare_command(&vm, command)
            .spawn()
            .wrap_err("failed to start process")?;
        processes.insert(from_region.clone(), (process_id, process));

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
    process_ips: HashMap<Region, String>,
) -> Result<Vec<ClientMetrics>, Report> {
    let regions_with_clients = machines.clients.len();
    let mut clients = HashMap::with_capacity(regions_with_clients);
    let mut wait_clients = Vec::with_capacity(regions_with_clients);

    for (client_index, (region, vm)) in
        (1..=regions_with_clients).zip(machines.clients.iter())
    {
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

        let command = exp::fantoch_bin_script(
            "client",
            args,
            // always run clients on release mode
            RunMode::Release,
            client_file(client_index),
        );
        let client = util::vm_prepare_command(&vm, command)
            .spawn()
            .wrap_err("failed to start client")?;
        clients.insert(client_index, client);

        wait_clients.push(wait_client_ended(client_index, region.clone(), &vm));
    }

    // wait all processse started
    let mut latencies = Vec::with_capacity(regions_with_clients);
    for result in futures::future::join_all(wait_clients).await {
        let latency = result?;
        latencies.push(latency);
    }
    Ok(latencies)
}

async fn stop_processes(
    run_id: String,
    run_mode: RunMode,
    machines: &Machines<'_>,
    processes: HashMap<Region, (ProcessId, tokio::process::Child)>,
) -> Result<(), Report> {
    // first copy the logs
    for (region, &(process_id, _)) in &processes {
        let vm = machines
            .servers
            .get(region)
            .expect("process vm should exist");
        util::copy_from(
            (&process_file(process_id), &vm),
            &format!(".log_{}_id{}_{:?}", run_id, process_id, region),
        )
        .await
        .wrap_err("copy log")?;
    }
    tracing::info!("copied all process logs");

    let mut wait_processes = Vec::with_capacity(machines.servers.len());
    for (region, (process_id, mut pchild)) in processes {
        let vm = machines
            .servers
            .get(&region)
            .expect("process vm should exist");

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
        // TODO: this should equivalent to `pkill newt_atomic`
        let command =
            format!("lsof -i :{} -i :{} | grep -v PID", PORT, CLIENT_PORT);
        let output =
            util::vm_exec(vm, command).await.wrap_err("lsof | grep")?;
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
            run_id.clone(),
            run_mode,
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
    vm: &tsunami::Machine<'_>,
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
        let stdout = util::vm_exec(vm, command).await.wrap_err("grep -c")?;
        count = stdout.parse::<usize>().wrap_err("grep -c parse")?;
    }
    Ok(())
}

async fn wait_client_ended(
    client_index: usize,
    region: Region,
    vm: &tsunami::Machine<'_>,
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
        let stdout = util::vm_exec(vm, command).await.wrap_err("grep -c")?;
        count = stdout.parse::<usize>().wrap_err("grep -c parse")?;
    }

    // fetch client log
    let command = format!("grep latency {}", client_file(client_index));
    let latency = util::vm_exec(vm, command).await.wrap_err("grep latency")?;
    Ok((region, latency))
}

async fn wait_process_ended(
    run_id: String,
    run_mode: RunMode,
    process_id: ProcessId,
    region: Region,
    vm: &tsunami::Machine<'_>,
) -> Result<(), Report> {
    // small delay between calls
    let delay = tokio::time::Duration::from_secs(2);

    let mut count = 1;
    while count != 0 {
        tokio::time::delay_for(delay).await;
        let command = format!("lsof -i :{} -i :{} | wc -l", PORT, CLIENT_PORT);
        let stdout = util::vm_exec(vm, command).await.wrap_err("lsof | wc")?;
        count = stdout.parse::<usize>().wrap_err("lsof | wc parse")?;
    }
    tracing::info!(
        "process {} in region {:?} terminated successfully",
        process_id,
        region
    );

    // maybe copy flamegraph
    if run_mode == RunMode::Flamegraph {
        // wait for the flamegraph process to finish writing `flamegraph.svg`
        let mut count = 1;
        while count != 0 {
            tokio::time::delay_for(delay).await;
            let command =
                format!("ps -aux | grep flamegraph | grep -v grep | wc -l");
            let stdout =
                util::vm_exec(vm, command).await.wrap_err("ps | wc")?;
            count = stdout.parse::<usize>().wrap_err("lsof | wc parse")?;
        }

        // copy `flamegraph.svg`
        util::copy_from(
            ("flamegraph.svg", &vm),
            &format!(
                ".flamegraph_{}_id{}_{:?}.svg",
                run_id, process_id, region
            ),
        )
        .await
        .wrap_err("copy flamegraph")?;

        tracing::info!(
            "copied flamegraph log of process {} in region {:?}",
            process_id,
            region
        );
    }

    Ok(())
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
