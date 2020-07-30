use crate::executor::ExecutorMetrics;
use crate::log;
use crate::protocol::ProtocolMetrics;
use crate::run::prelude::*;
use crate::HashMap;
use serde::{Deserialize, Serialize};
use tokio::time::{self, Duration};

pub const METRICS_INTERVAL: Duration = Duration::from_secs(5); // notify/flush every 5 seconds

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ProcessMetrics {
    workers: HashMap<usize, ProtocolMetrics>,
    executors: HashMap<usize, ExecutorMetrics>,
}

impl ProcessMetrics {
    fn new() -> Self {
        Self {
            workers: HashMap::new(),
            executors: HashMap::new(),
        }
    }
}

pub async fn metrics_logger_task(
    metrics_file: String,
    mut from_workers: ProtocolMetricsReceiver,
    mut from_executors: ExecutorMetricsReceiver,
) {
    println!("[metrics_logger] started with log {}", metrics_file);

    // create metrics
    let mut global_metrics = ProcessMetrics::new();

    // create interval
    let mut interval = time::interval(METRICS_INTERVAL);

    loop {
        tokio::select! {
            metrics = from_workers.recv() => {
                log!("[metrics_logger] from protocol worker: {:?}", metrics);
                if let Some((index, protocol_metrics)) = metrics  {
                    // update metrics for this worker
                    global_metrics.workers.insert(index, protocol_metrics);
                } else {
                    println!("[metrics_logger] error while receiving metrics from protocol worker");
                }
            }
            metrics = from_executors.recv() => {
                log!("[metrics_logger] from executor: {:?}", metrics);
                if let Some((index, executor_metrics)) = metrics  {
                    // update metrics for this executor
                    global_metrics.executors.insert(index, executor_metrics);
                } else {
                    println!("[metrics_logger] error while receiving metrics from executor");
                }
            }
            _ = interval.tick()  => {
                // First serialize to a temporary file, and then rename it. This makes it more
                // likely we won't end up with a corrupted file if we're shutdown in the middle
                // of this.
                let tmp = format!("{}_tmp", metrics_file);
                if let Err(e) = crate::run::serialize_and_compress(&global_metrics, &tmp) {
                    panic!("[metrics_logger] couldn't serialize metrics: {:?}", e);
                } else {
                    // if there was no error, rename file
                    std::fs::rename(&tmp, &metrics_file).expect("couldn't rename temporary metrics file");
                }
            }
        }
    }
}
