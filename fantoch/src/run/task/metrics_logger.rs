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

// TODO make this async
fn serialize_process_metrics(data: &ProcessMetrics, file: &String) {
    // if the file does not exist it will be created, otherwise truncated
    std::fs::File::create(file)
        .ok()
        // create a buf writer
        .map(std::io::BufWriter::new)
        // and try to serialize
        .map(|writer| {
            bincode::serialize_into(writer, &data)
                .expect("error serializing process metrics")
        })
        .unwrap_or_else(|| panic!("couldn't save process metrics"));
}
