use crate::db::{Dstat, DstatCompress, HistogramCompress};
use fantoch::client::ClientData;
use fantoch::id::ProcessId;
use fantoch::metrics::Histogram;
use fantoch::planet::{Planet, Region};
use fantoch::run::task::metrics_logger::ProcessMetrics;
use fantoch_exp::Testbed;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExperimentData {
    pub process_metrics: HashMap<ProcessId, (Region, ProcessMetrics)>,
    pub global_process_dstats: DstatCompress,
    pub client_latency: HashMap<Region, HistogramCompress>,
    pub global_client_latency: HistogramCompress,
}

impl ExperimentData {
    pub fn new(
        planet: &Option<Planet>,
        testbed: Testbed,
        process_metrics: HashMap<ProcessId, (Region, ProcessMetrics)>,
        process_dstats: HashMap<ProcessId, Dstat>,
        client_metrics: HashMap<Region, ClientData>,
        global_client_metrics: ClientData,
    ) -> Self {
        // merge all process dstats
        let mut global_process_dstats = Dstat::new();
        for (_, process_dstat) in process_dstats {
            global_process_dstats.merge(&process_dstat);
        }
        // compress global dstat
        let global_process_dstats = DstatCompress::from(&global_process_dstats);

        // we should use milliseconds if: AWS or (baremetal + injected latency)
        let precision = match testbed {
            Testbed::Aws => {
                // assert that no latency was injected
                assert!(planet.is_none());
                LatencyPrecision::Millis
            }
            Testbed::Baremetal => {
                // use ms if latency was injected, otherwise micros
                if planet.is_some() {
                    LatencyPrecision::Millis
                } else {
                    LatencyPrecision::Micros
                }
            }
        };

        // create latency histogram per region
        let client_latency = client_metrics
            .into_iter()
            .map(|(region, client_data)| {
                // create latency histogram (for the given precision)
                let latency = Self::extract_latency(
                    precision,
                    client_data.latency_data(),
                );
                let histogram = Histogram::from(latency);
                // compress client histogram
                let histogram = HistogramCompress::from(&histogram);
                (region, histogram)
            })
            .collect();

        // create global latency histogram
        let latency = Self::extract_latency(
            precision,
            global_client_metrics.latency_data(),
        );
        let global_client_latency = Histogram::from(latency);
        // compress global client histogram
        let global_client_latency =
            HistogramCompress::from(&global_client_latency);

        Self {
            process_metrics,
            global_process_dstats,
            client_latency,
            global_client_latency,
        }
    }

    fn extract_latency(
        precision: LatencyPrecision,
        latency_data: impl Iterator<Item = Duration>,
    ) -> impl Iterator<Item = u64> {
        latency_data.map(move |duration| {
            let latency = match precision {
                LatencyPrecision::Micros => duration.as_micros(),
                LatencyPrecision::Millis => duration.as_millis(),
            };
            latency as u64
        })
    }
}

#[derive(Clone, Copy)]
enum LatencyPrecision {
    Micros,
    Millis,
}
