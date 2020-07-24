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
                serialize_process_metrics(&global_metrics, &metrics_file);
            }
        }
    }
}

/// First serialize to a temporary file, and then rename it. This makes it more
/// likely we won't end up with a corrupted file if we're shutdown in the middle
/// of this.
// TODO make this async
fn serialize_process_metrics(data: &ProcessMetrics, path: &str) {
    // if the file does not exist it will be created, otherwise truncated
    let tmp = format!("{}_tmp", path);
    let file = std::fs::File::create(&tmp)
        .expect("couldn't create temporary metrics file");
    // create a buf writer
    let writer = std::io::BufWriter::new(file);
    // and try to serialize
    bincode::serialize_into(writer, &data)
        .expect("error serializing process metrics");

    // finally, rename temporary file
    std::fs::rename(tmp, path).expect("couldn't rename temporary metrics file");
}
