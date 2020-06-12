use crate::{args, util};
use color_eyre::Report;
use eyre::WrapErr;
use rusoto_core::Region;
use std::collections::HashMap;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tsunami::providers::aws;
use tsunami::{Machine, Tsunami};

type ProcessId = usize;
type ClientMetrics = (String, String);

// processes config
const PORT: usize = 3000;
const CLIENT_PORT: usize = 4000;
const PROTOCOL: &str = "newt_atomic";
const FAULTS: usize = 1;
const TRANSITIVE_CONFLICTS: bool = true;
const EXECUTE_AT_COMMIT: bool = false;
const EXECUTION_LOG: Option<&str> = None;
const LEADER: Option<ProcessId> = None;
const GC_INTERVAL: usize = 500; // every 500ms
const TRACER_SHOW_INTERVAL: Option<usize> = None;

// parallelism config
const WORKERS: usize = 8;
const EXECUTORS: usize = 8;
const MULTIPLEXING: usize = 2;

// clients config
const CONFLICT_RATE: usize = 0;
const COMMANDS_PER_CLIENT: usize = 1000;
const PAYLOAD_SIZE: usize = 0;

// process tcp config
const PROCESS_TCP_NODELAY: bool = true;
// by default, each socket stream is buffered (with a buffer of size 8KBs),
// which should greatly reduce the number of syscalls for small-sized messages
const PROCESS_TCP_BUFFER_SIZE: usize = 0 * 1024;
const PROCESS_TCP_FLUSH_INTERVAL: Option<usize> = None;

// client tcp config
const CLIENT_TCP_NODELAY: bool = true;

// if this value is 100, the run doesn't finish, which probably means there's a
// deadlock somewhere with 1000 we can see that channels fill up sometimes with
// 10000 that doesn't seem to happen
const CHANNEL_BUFFER_SIZE: usize = 10000;

pub async fn bench_experiment(
    server_instance_type: String,
    client_instance_type: String,
    regions: Vec<Region>,
    max_spot_instance_request_wait_secs: u64,
    max_instance_duration_hours: usize,
    branch: String,
    ns: Vec<usize>,
    clients_per_region: Vec<usize>,
    newt_configs: Vec<(bool, bool, usize)>,
    mut output_log: tokio::fs::File,
) -> Result<(), Report> {
    let mut launcher: aws::Launcher<_> = Default::default();
    let (regions, server_vms, client_vms) = spawn(
        &mut launcher,
        server_instance_type,
        client_instance_type,
        regions,
        max_spot_instance_request_wait_secs,
        max_instance_duration_hours,
        branch,
    )
    .await
    .wrap_err("spawn machines")?;

    for n in ns {
        for newt_config in newt_configs.clone() {
            let (newt_tiny_quorums, newt_real_time, newt_clock_bump_interval) =
                newt_config;
            if n == 3 && newt_tiny_quorums {
                tracing::warn!("skipping newt config n = 3 tiny = true");
                continue;
            }

            let line = if newt_real_time {
                format!(">running {} n = {} | f = {} | tiny = {} | clock_bump_interval = {}ms", PROTOCOL, n, FAULTS, newt_tiny_quorums, newt_clock_bump_interval)
            } else {
                format!(">running {} n = {} | f = {} | tiny = {} | real_time = false", PROTOCOL, n, FAULTS, newt_tiny_quorums)
            };
            append_to_output_log(&mut output_log, line).await?;

            // select the first `n` regions
            let regions: Vec<_> = regions.iter().cloned().take(n).collect();
            for &clients in &clients_per_region {
                let latencies = run_experiment(
                    &regions,
                    &server_vms,
                    &client_vms,
                    newt_tiny_quorums,
                    newt_real_time,
                    newt_clock_bump_interval,
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
                    let line =
                        format!("region = {} | {}", region, region_latency);
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

                let mut line = format!("n = {} AND c = {} |", n, clients);
                for metric in metrics {
                    let latencies = latencies_to_avg
                        .remove(metric)
                        .expect("metric should exist");
                    // there should be as many latencies as regions
                    assert_eq!(latencies.len(), regions.len());
                    let avg =
                        latencies.into_iter().sum::<usize>() / regions.len();
                    line = format!("{}   {}={}", line, metric, avg);
                }
                append_to_output_log(&mut output_log, line).await?;
            }
        }
    }
    launcher.terminate_all().await?;
    Ok(())
}

async fn run_experiment(
    regions: &Vec<String>,
    server_vms: &HashMap<String, Machine<'_>>,
    client_vms: &HashMap<String, Machine<'_>>,
    newt_tiny_quorums: bool,
    newt_real_time: bool,
    newt_clock_bump_interval: usize,
    clients_per_region: usize,
) -> Result<Vec<ClientMetrics>, Report> {
    // start processes
    let (process_ips, processes) = start_processes(
        regions,
        server_vms,
        newt_tiny_quorums,
        newt_real_time,
        newt_clock_bump_interval,
    )
    .await
    .wrap_err("start_processes")?;

    // run clients
    let client_metrics =
        run_clients(clients_per_region, client_vms, process_ips)
            .await
            .wrap_err("run_clients")?;

    // stop processes
    stop_processes(server_vms, processes)
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
                .setup(util::fantoch_setup(branch.clone()));

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
    newt_tiny_quorums: bool,
    newt_real_time: bool,
    newt_clock_bump_interval: usize,
) -> Result<
    (
        HashMap<String, String>,
        HashMap<String, tokio::process::Child>,
    ),
    Report,
> {
    let ips: HashMap<_, _> = vms
        .iter()
        .map(|(region, vm)| (region.clone(), vm.public_ip.clone()))
        .collect();
    tracing::debug!("processes ips: {:?}", ips);
    let addresses = |region| {
        // select all ips but self
        ips.iter()
            .filter_map(|(ip_region, ip)| {
                if ip_region == region {
                    None
                } else {
                    Some(address(ip, PORT))
                }
            })
            .collect::<Vec<_>>()
            .join(",")
    };

    let n = vms.len();
    let mut processes = HashMap::with_capacity(n);
    let mut wait_processes = Vec::with_capacity(n);

    for (process_id, region) in (1..=n).zip(regions.iter()) {
        let vm = vms.get(region).expect("process vm should exist");
        let addresses = addresses(region);
        let mut args = args![
            "--id",
            process_id,
            // bind to 0.0.0.0
            "--ip",
            "0.0.0.0",
            "--port",
            PORT,
            "--addresses",
            addresses,
            "--client_port",
            CLIENT_PORT,
            "--processes",
            n,
            "--faults",
            FAULTS,
            "--transitive_conflicts",
            TRANSITIVE_CONFLICTS,
            "--execute_at_commit",
            EXECUTE_AT_COMMIT,
            "--tcp_nodelay",
            PROCESS_TCP_NODELAY,
            "--tcp_buffer_size",
            PROCESS_TCP_BUFFER_SIZE,
            "--gc_interval",
            GC_INTERVAL,
            "--channel_buffer_size",
            CHANNEL_BUFFER_SIZE,
            "--workers",
            WORKERS,
            "--executors",
            EXECUTORS,
            "--multiplexing",
            MULTIPLEXING,
            "--newt_tiny_quorums",
            newt_tiny_quorums,
            "--newt_real_time",
            newt_real_time,
            "--newt_clock_bump_interval",
            newt_clock_bump_interval,
        ];
        if let Some(interval) = PROCESS_TCP_FLUSH_INTERVAL {
            args.extend(args!["--tcp_flush_interval", interval]);
        }
        if let Some(log) = EXECUTION_LOG {
            args.extend(args!["--execution_log", log]);
        }
        if let Some(interval) = TRACER_SHOW_INTERVAL {
            args.extend(args!["-tracer_show_interval", interval]);
        }
        if let Some(leader) = LEADER {
            args.extend(args!["--leader", leader]);
        }

        let command =
            fantoch_bin_script(PROTOCOL, args, process_file(process_id));
        let process = util::prepare_command(&vm, command)
            .spawn()
            .wrap_err("failed to start process")?;
        processes.insert(region.clone(), process);

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
        // get ip for this region
        let ip = process_ips.get(region).expect("get process ip");
        let address = address(ip, CLIENT_PORT);

        // compute id start and id end:
        // - first compute the id end
        // - and then compute id start: subtract `clients_per_machine` and add 1
        let id_end = client_index * clients_per_region;
        let id_start = id_end - clients_per_region + 1;

        let args = args![
            "--ids",
            format!("{}-{}", id_start, id_end),
            "--address",
            address,
            "--conflict_rate",
            CONFLICT_RATE,
            "--commands_per_client",
            COMMANDS_PER_CLIENT,
            "--payload_size",
            PAYLOAD_SIZE,
            "--tcp_nodelay",
            CLIENT_TCP_NODELAY,
            "--channel_buffer_size",
            CHANNEL_BUFFER_SIZE,
        ];

        let command =
            fantoch_bin_script("client", args, client_file(client_index));
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
    vms: &HashMap<String, Machine<'_>>,
    processes: HashMap<String, tokio::process::Child>,
) -> Result<(), Report> {
    let mut wait_processes = Vec::with_capacity(vms.len());
    for (region, mut pchild) in processes {
        if let Err(e) = pchild.kill() {
            tracing::warn!(
                "error trying to kill ssh process {} in region {}: {:?}",
                pchild.id(),
                region,
                e
            );
        }

        // find process pid in remote vm
        let vm = vms.get(&region).expect("process vm should exist");
        let command = format!(
            "lsof -i :{} -i :{} | grep -v PID | sort -u",
            PORT, CLIENT_PORT
        );
        let output = util::exec(vm, command)
            .await
            .wrap_err("lsof | grep | sort")?;
        tracing::debug!("{}", output);
        let pids: Vec<_> = output
            .lines()
            .map(|line| line.split_whitespace().collect::<Vec<_>>()[1])
            .collect();
        // there should be a single pid
        assert_eq!(pids.len(), 1, "there should be a single process pid");

        // kill it
        let pid = pids[0];
        let command = format!("kill -SIGKILL {}", pid);
        let output = util::exec(vm, command).await.wrap_err("kill")?;
        tracing::debug!("{}", output);

        wait_processes.push(wait_process_ended(region, vm));
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
    region: String,
    vm: &Machine<'_>,
) -> Result<(), Report> {
    let mut count = 1;
    while count != 0 {
        // small delay between calls
        let seconds = 2;
        tokio::time::delay_for(tokio::time::Duration::from_secs(seconds)).await;

        let command = format!("lsof -i :{} -i :{} | wc -l", PORT, CLIENT_PORT);
        let stdout = util::exec(vm, command).await.wrap_err("lsof | wc")?;
        count = stdout.parse::<usize>().wrap_err("lsof | wc parse")?;
    }
    tracing::info!("process in region {} terminated successfully", region);
    Ok(())
}

fn fantoch_bin_script(
    binary: &str,
    args: Vec<String>,
    output_file: String,
) -> String {
    let prefix = "source ~/.cargo/env && cd fantoch";
    let args = args.join(" ");
    format!(
        "{} && ./target/release/{} {} > {} 2>&1",
        prefix, binary, args, output_file
    )
}

fn address(ip: impl ToString, port: usize) -> String {
    format!("{}:{}", ip.to_string(), port)
}

fn process_file(process_id: ProcessId) -> String {
    format!("~/.log_process_{}", process_id)
}

fn client_file(client_index: usize) -> String {
    format!("~/.log_client_{}", client_index)
}

async fn append_to_output_log(
    output_log: &mut tokio::fs::File,
    line: String,
) -> Result<(), Report> {
    tracing::info!("{}", line);
    output_log
        .write_all(line.as_bytes())
        .await
        .wrap_err("output log write")
}
