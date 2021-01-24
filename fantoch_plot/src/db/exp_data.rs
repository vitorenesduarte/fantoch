use crate::db::{Dstat, DstatCompress, MicrosHistogramCompress};
use fantoch::client::ClientData;
use fantoch::executor::ExecutorMetrics;
use fantoch::id::ProcessId;
use fantoch::planet::Region;
use fantoch::protocol::ProtocolMetrics;
use fantoch::run::task::server::metrics_logger::ProcessMetrics;
use fantoch_prof::metrics::Histogram;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExperimentData {
    pub process_metrics: HashMap<ProcessId, (Region, ProcessMetrics)>,
    pub global_protocol_metrics: ProtocolMetrics,
    pub global_executor_metrics: ExecutorMetrics,
    pub process_dstats: HashMap<ProcessId, DstatCompress>,
    pub global_process_dstats: DstatCompress,
    pub global_client_dstats: DstatCompress,
    pub client_latency: HashMap<Region, MicrosHistogramCompress>,
    pub global_client_latency: MicrosHistogramCompress,
    pub client_throughput: HashMap<Region, f64>,
    pub global_client_throughput: f64,
}

impl ExperimentData {
    pub fn new(
        process_metrics: HashMap<ProcessId, (Region, ProcessMetrics)>,
        process_dstats: HashMap<ProcessId, Dstat>,
        client_metrics: HashMap<Region, ClientData>,
        client_dstats: HashMap<Region, Dstat>,
        global_client_metrics: ClientData,
    ) -> Self {
        // create global protocol and executor metrics
        let mut global_protocol_metrics = ProtocolMetrics::new();
        let mut global_executor_metrics = ExecutorMetrics::new();
        for (_, (_, process_metrics)) in process_metrics.iter() {
            global_protocol_metrics.merge(&process_metrics.protocol_metrics());
            global_executor_metrics.merge(&process_metrics.executor_metrics());
        }

        // compress process dstats and create global process dstat
        let mut global_process_dstats = Dstat::new();
        let process_dstats = process_dstats
            .into_iter()
            .map(|(process_id, process_dstat)| {
                // merge with global process dstat
                global_process_dstats.merge(&process_dstat);
                // compress process dstat
                let process_dstat = DstatCompress::from(&process_dstat);
                (process_id, process_dstat)
            })
            .collect();
        // compress global process dstat
        let global_process_dstats = DstatCompress::from(&global_process_dstats);

        // merge all client dstats
        let mut global_client_dstats = Dstat::new();
        for (_, client_dstat) in client_dstats {
            global_client_dstats.merge(&client_dstat);
        }
        // compress global client dstat
        let global_client_dstats = DstatCompress::from(&global_client_dstats);

        // create latency histogram per region (and also compute throughput)
        let mut client_throughput =
            HashMap::with_capacity(client_metrics.len());
        let client_latency = client_metrics
            .into_iter()
            .map(|(region, client_data)| {
                // compute throughput
                let throughput = client_data.throughput();
                client_throughput.insert(region.clone(), throughput);

                // create latency histogram
                let latency = Self::extract_micros(client_data.latency_data());
                let histogram = Histogram::from(latency);
                // compress client histogram
                let histogram = MicrosHistogramCompress::from(&histogram);
                (region, histogram)
            })
            .collect();

        // create global latency histogram (and also compute throughput)
        let global_client_throughput = global_client_metrics.throughput();
        let latency =
            Self::extract_micros(global_client_metrics.latency_data());
        let global_client_latency = Histogram::from(latency);
        // compress global client histogram
        let global_client_latency =
            MicrosHistogramCompress::from(&global_client_latency);

        Self {
            process_metrics,
            global_protocol_metrics,
            global_executor_metrics,
            process_dstats,
            global_process_dstats,
            client_latency,
            global_client_dstats,
            global_client_latency,
            client_throughput,
            global_client_throughput,
        }
    }

    fn extract_micros(
        latency_data: impl Iterator<Item = Duration>,
    ) -> impl Iterator<Item = u64> {
        latency_data.map(move |duration| duration.as_micros() as u64)
    }
}
