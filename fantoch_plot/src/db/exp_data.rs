use crate::db::Dstat;
use fantoch::client::ClientData;
use fantoch::metrics::Histogram;
use fantoch::planet::{Planet, Region};
use fantoch::run::task::metrics_logger::ProcessMetrics;
use fantoch_exp::Testbed;
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct ExperimentData {
    pub process_metrics: HashMap<Region, ProcessMetrics>,
    pub global_process_dstats: Dstat,
    pub client_latency: HashMap<Region, Histogram>,
    pub global_client_latency: Histogram,
}

impl ExperimentData {
    pub fn new(
        planet: &Option<Planet>,
        testbed: Testbed,
        process_metrics: HashMap<Region, ProcessMetrics>,
        process_dstats: HashMap<Region, Dstat>,
        client_metrics: HashMap<Region, ClientData>,
        global_client_metrics: ClientData,
    ) -> Self {
        // merge all process dstats
        let mut global_process_dstats = Dstat::new();
        for (_, process_dstat) in process_dstats {
            global_process_dstats.merge(&process_dstat);
        }

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
                (region, histogram)
            })
            .collect();

        // create global latency histogram
        let latency = Self::extract_latency(
            precision,
            global_client_metrics.latency_data(),
        );
        let global_client_latency = Histogram::from(latency);

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
