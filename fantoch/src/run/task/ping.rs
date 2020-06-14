use super::chan::ChannelSender;
use crate::id::ProcessId;
use crate::log;
use crate::metrics::Histogram;
use crate::run::prelude::*;
use std::collections::HashMap;
use std::net::IpAddr;
use tokio::time::{self, Duration};

const PING_SHOW_INTERVAL: u64 = 5000; // millis
const ITERATIONS_PER_PING: u64 = 5;

pub async fn ping_task(
    ping_interval: Option<usize>,
    process_id: ProcessId,
    ips: HashMap<ProcessId, IpAddr>,
    mut parent: SortedProcessesReceiver,
) {
    // if no interval, do not ping
    if ping_interval.is_none() {
        return;
    }
    let ping_interval = ping_interval.unwrap();

    // create tokio interval
    let millis = Duration::from_millis(ping_interval as u64);
    log!("[ping_task] interval {:?}", millis);
    let mut ping_interval = time::interval(millis);

    // create another tokio interval
    let millis = Duration::from_millis(PING_SHOW_INTERVAL);
    log!("[ping_task] show interval {:?}", millis);
    let mut ping_show_interval = time::interval(millis);

    //  create ping stats
    let mut ping_stats = ips
        .into_iter()
        .map(|(process_id, ip)| (process_id, (ip, Histogram::new())))
        .collect();

    // make sure we do at least one round of pinging before receiving any
    // message from parent
    ping_task_ping(&mut ping_stats).await;

    loop {
        tokio::select! {
            _ = ping_interval.tick() => {
                ping_task_ping(&mut ping_stats).await;
            }
            _ = ping_show_interval.tick() => {
                ping_task_show(&ping_stats);
            }
            sort_request = parent.recv() => {
                ping_task_sort(process_id, &ping_stats, sort_request).await;
            }
        }
    }
}

async fn ping_task_ping(
    ping_stats: &mut HashMap<ProcessId, (IpAddr, Histogram)>,
) {
    for (ip, histogram) in ping_stats.values_mut() {
        for _ in 0..ITERATIONS_PER_PING {
            let command =
                format!("ping -c 1 -q {} | tail -n 1 | cut -d/ -f5", ip);
            let out = tokio::process::Command::new("sh")
                .arg("-c")
                .arg(command)
                .output()
                .await
                .expect("ping command should work");
            let stdout = String::from_utf8(out.stdout)
                .expect("ping output should be utf8")
                .trim()
                .to_string();
            let latency = stdout
                .parse::<f64>()
                .expect("ping output should be a float");
            let rounded_latency = latency as u64;
            histogram.increment(rounded_latency);
        }
    }
}

fn ping_task_show(ping_stats: &HashMap<ProcessId, (IpAddr, Histogram)>) {
    for (process_id, (_, histogram)) in ping_stats {
        println!("{}: {:?}", process_id, histogram);
    }
}

async fn ping_task_sort(
    process_id: ProcessId,
    ping_stats: &HashMap<ProcessId, (IpAddr, Histogram)>,
    sort_request: Option<ChannelSender<Vec<ProcessId>>>,
) {
    match sort_request {
        Some(mut sort_request) => {
            let sorted_processes = sort_by_distance(process_id, &ping_stats);
            if let Err(e) = sort_request.send(sorted_processes).await {
                println!(
                    "[ping_task] error sending message to parent: {:?}",
                    e
                );
            }
        }
        None => {
            println!("[ping_task] error receiving message from parent");
        }
    }
}

/// This function makes sure that self is always the first process in the
/// returned list.
fn sort_by_distance(
    process_id: ProcessId,
    ping_stats: &HashMap<ProcessId, (IpAddr, Histogram)>,
) -> Vec<ProcessId> {
    // sort processes by ping time
    let mut pings = ping_stats
        .iter()
        .map(|(id, (_, histogram))| (histogram.mean().round(), id))
        .collect::<Vec<_>>();
    pings.sort();
    // make sure we're the first process
    std::iter::once(process_id)
        .chain(pings.into_iter().map(|(_latency, &process_id)| process_id))
        .collect()
}
