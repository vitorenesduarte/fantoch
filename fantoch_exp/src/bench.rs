use crate::{args, util};
use color_eyre::Report;
use eyre::WrapErr;
use rusoto_core::Region;
use std::collections::HashMap;
use std::time::Duration;
use tsunami::providers::aws;
use tsunami::{Machine, Tsunami};

type ProcessId = usize;
const DEBUG_SECS: u64 = 30 * 60; // 30 minutes

// processes config
const PORT: usize = 3000;
const CLIENT_PORT: usize = 4000;
const PROTOCOL: &str = "newt_atomic";
const FAULTS: usize = 1;
const TRANSITIVE_CONFLICTS: bool = true;
const EXECUTE_AT_COMMIT: bool = false;
const EXECUTION_LOG: Option<&str> = None;
const LEADER: Option<ProcessId> = None;
const GC_INTERVAL: usize = 5; // every 5ms
const TRACER_SHOW_INTERVAL: Option<usize> = None;

// parallelism config
const WORKERS: usize = 8;
const EXECUTORS: usize = 8;
const MULTIPLEXING: usize = 2;

// clients config
const CONFLICT_RATE: usize = 0;
const COMMANDS_PER_CLIENT: usize = 50000;
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
) -> Result<(), Report> {
    let mut launcher: aws::Launcher<_> = Default::default();
    let res = do_bench_experiment(
        &mut launcher,
        server_instance_type,
        client_instance_type,
        regions,
        max_spot_instance_request_wait_secs,
        max_instance_duration_hours,
        branch,
    )
    .await;
    tracing::warn!("experiment res: {:?}", res);
    tokio::time::delay_for(tokio::time::Duration::from_secs(DEBUG_SECS)).await;
    // make sure we always terminate
    launcher.terminate_all().await?;
    Ok(())
}

async fn do_bench_experiment(
    launcher: &mut aws::Launcher<rusoto_credential::DefaultCredentialsProvider>,
    server_instance_type: String,
    client_instance_type: String,
    regions: Vec<Region>,
    max_spot_instance_request_wait_secs: u64,
    max_instance_duration_hours: usize,
    branch: String,
) -> Result<(), Report> {
    let server_tag = "server";
    // let client_tag = "client";
    let tags = vec![
        (server_tag.to_string(), server_instance_type),
        // (client_tag.to_string(), client_instance_type),
    ];
    let mut vms = spawn(
        launcher,
        tags,
        regions,
        max_spot_instance_request_wait_secs,
        max_instance_duration_hours,
        branch,
    )
    .await?;

    let servers = vms.remove(server_tag).expect("servers vms");
    // let clients = vms.remove(client_tag).expect("client vms");
    let processes = start_processes(servers).await?;
    // run_clients(clients).await?;
    Ok(())
}

async fn spawn<'l>(
    launcher: &'l mut aws::Launcher<
        rusoto_credential::DefaultCredentialsProvider,
    >,
    tags: Vec<(String, String)>,
    regions: Vec<Region>,
    max_spot_instance_request_wait_secs: u64,
    max_instance_duration_hours: usize,
    branch: String,
) -> Result<HashMap<String, HashMap<String, Machine<'l>>>, Report> {
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
    Ok(results)
}

async fn start_processes(
    vms: HashMap<String, Machine<'_>>,
) -> Result<HashMap<ProcessId, tokio::process::Child>, Report> {
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
                    // create address as "ip:port"
                    let address = format!("{}:{}", ip, PORT);
                    Some(address)
                }
            })
            .collect::<Vec<_>>()
            .join(",")
    };

    let n = vms.len();
    let mut processes = HashMap::with_capacity(n);
    let mut wait_processes = Vec::with_capacity(n);
    for (process_id, (region, vm)) in (1..=n).zip(vms.iter()) {
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
            MULTIPLEXING
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
        processes.insert(process_id, process);

        wait_processes.push(wait_process_started(process_id, &vm));
    }

    // wait all processse started
    for result in futures::future::join_all(wait_processes).await {
        let () = result?;
    }

    Ok(processes)
}

async fn wait_process_started(
    process_id: ProcessId,
    vm: &Machine<'_>,
) -> Result<(), Report> {
    let mut count = 0;
    while count != 1 {
        tokio::time::delay_for(tokio::time::Duration::from_secs(1)).await;
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

async fn run_clients(vms: HashMap<String, Machine<'_>>) -> Result<(), Report> {
    todo!()
}

fn fantoch_bin_script(
    binary: &str,
    args: Vec<String>,
    output_file: String,
) -> String {
    let prefix = "source ~/.cargo/env && cd fantoch";
    let args = args.join(" ");
    format!(
        "{} && ./target/release/{} {} 2>&1 > {}",
        prefix, binary, args, output_file
    )
}

fn process_file(process_id: ProcessId) -> String {
    format!("~/.log_process_{}", process_id)
}
