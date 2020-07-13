use crate::executor::ExecutorMetrics;
use crate::log;
use crate::protocol::ProtocolMetrics;
use crate::run::prelude::*;
use crate::run::rw::Rw;
use crate::HashMap;
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::time::{self, Duration};

pub const METRICS_INTERVAL: Duration = Duration::from_secs(5); // notify/flush every 5 seconds
const METRICS_LOGGER_BUFFER_SIZE: usize = 8 * 1024; // 8KB

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
                // create metrics log file (truncating it if already exists)
                let file = File::create(&metrics_file)
                    .await
                    .expect("it should be possible to create metrics log file");

                // create file logger
                let mut logger =
                    Rw::from(METRICS_LOGGER_BUFFER_SIZE, METRICS_LOGGER_BUFFER_SIZE, file);

                logger.write(&global_metrics).await;
                logger.flush().await
            }
        }
    }
}
