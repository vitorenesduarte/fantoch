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
    parent: Option<SortedProcessesReceiver>,
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

    if let Some(mut parent) = parent {
        // do one round of pinging and then process the parent message
        ping_task_ping(&mut ping_stats).await;
        let sort_request = parent.recv().await;
        ping_task_sort(process_id, &ping_stats, sort_request).await;
    }

    loop {
        tokio::select! {
            _ = ping_interval.tick() => {
                ping_task_ping(&mut ping_stats).await;
            }
            _ = ping_show_interval.tick() => {
                ping_task_show(&ping_stats);
            }
        }
    }
}

async fn ping_task_ping(
    ping_stats: &mut HashMap<ProcessId, (IpAddr, Histogram)>,
) {
    for (ip, histogram) in ping_stats.values_mut() {
        for _ in 0..ITERATIONS_PER_PING {
            let latency = loop {
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

                if stdout.is_empty() {
                    println!(
                        "[ping_task] ping output was empty; trying again..."
                    )
                } else {
                    break stdout;
                }
            };

            let latency = latency
                .parse::<f64>()
                .expect("ping output should be a float");
            let rounded_latency = latency as u64;
            histogram.increment(rounded_latency);
        }
    }
}

fn ping_task_show(ping_stats: &HashMap<ProcessId, (IpAddr, Histogram)>) {
    for (process_id, (_, histogram)) in ping_stats {
        println!("[ping_task] {}: {:?}", process_id, histogram);
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
        .map(|(id, (_, histogram))| (u64::from(histogram.mean()), id))
        .collect::<Vec<_>>();
    pings.sort();
    // make sure we're the first process
    std::iter::once(process_id)
        .chain(pings.into_iter().map(|(_latency, &process_id)| process_id))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn sort_by_distance_test() {
        let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

        let mut ping_stats = HashMap::new();
        assert_eq!(sort_by_distance(1, &ping_stats), vec![1]);

        ping_stats.insert(2, (ip, Histogram::from(vec![10, 20, 30])));
        assert_eq!(sort_by_distance(1, &ping_stats), vec![1, 2]);

        ping_stats.insert(3, (ip, Histogram::from(vec![5, 5, 5])));
        assert_eq!(sort_by_distance(1, &ping_stats), vec![1, 3, 2]);

        let (_, histogram_2) = ping_stats.get_mut(&2).unwrap();
        for _ in 1..100 {
            histogram_2.increment(1);
        }
        assert_eq!(sort_by_distance(1, &ping_stats), vec![1, 2, 3]);
    }
}
