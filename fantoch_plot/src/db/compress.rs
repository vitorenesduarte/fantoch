use crate::db::Dstat;
use fantoch_prof::metrics::Histogram;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

#[derive(Clone, Copy)]
pub enum LatencyPrecision {
    Micros,
    Millis,
}

impl LatencyPrecision {
    pub fn name(&self) -> String {
        match self {
            Self::Micros => String::from("us"),
            Self::Millis => String::from("ms"),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct MicrosHistogramCompress {
    hist: HistogramCompress,
}

impl MicrosHistogramCompress {
    pub fn from(histogram: &Histogram) -> Self {
        Self {
            hist: HistogramCompress::from(histogram),
        }
    }

    pub fn min(&self, precision: LatencyPrecision) -> f64 {
        Self::convert(self.hist.min(), precision)
    }

    pub fn max(&self, precision: LatencyPrecision) -> f64 {
        Self::convert(self.hist.max(), precision)
    }

    pub fn mean(&self, precision: LatencyPrecision) -> f64 {
        Self::convert(self.hist.mean(), precision)
    }

    pub fn stddev(&self, precision: LatencyPrecision) -> f64 {
        Self::convert(self.hist.stddev(), precision)
    }

    pub fn percentile(
        &self,
        percentile: f64,
        precision: LatencyPrecision,
    ) -> f64 {
        Self::convert(self.hist.percentile(percentile), precision)
    }

    fn convert(micros: f64, latency_precision: LatencyPrecision) -> f64 {
        match latency_precision {
            LatencyPrecision::Micros => micros,
            LatencyPrecision::Millis => micros / 1000.0,
        }
    }
}

// same as `Histogram`'s
impl fmt::Debug for MicrosHistogramCompress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let latency_precision = LatencyPrecision::Micros;
        write!(
            f,
            "min={:<6} max={:<6} avg={:<6} p5={:<6} p95={:<6} p99={:<6} p99.9={:<6} p99.99={:<6}",
            self.min(latency_precision).round(),
            self.max(latency_precision).round(),
            self.mean(latency_precision).round(),
            self.percentile(0.05, latency_precision).round(),
            self.percentile(0.95, latency_precision).round(),
            self.percentile(0.99, latency_precision).round(),
            self.percentile(0.999, latency_precision).round(),
            self.percentile(0.9999, latency_precision).round()
        )
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct DstatCompress {
    pub cpu_usr: HistogramCompress,
    pub cpu_sys: HistogramCompress,
    pub cpu_wait: HistogramCompress,
    pub net_recv: HistogramCompress,
    pub net_send: HistogramCompress,
    pub mem_used: HistogramCompress,
}

impl DstatCompress {
    pub fn from(dstat: &Dstat) -> Self {
        Self {
            cpu_usr: HistogramCompress::from(&dstat.cpu_usr),
            cpu_sys: HistogramCompress::from(&dstat.cpu_sys),
            cpu_wait: HistogramCompress::from(&dstat.cpu_wait),
            net_recv: HistogramCompress::from(&dstat.net_recv),
            net_send: HistogramCompress::from(&dstat.net_send),
            mem_used: HistogramCompress::from(&dstat.mem_used),
        }
    }

    pub fn cpu_usr_mad(&self) -> (u64, u64) {
        Self::mad(&self.cpu_usr, None)
    }

    pub fn cpu_sys_mad(&self) -> (u64, u64) {
        Self::mad(&self.cpu_sys, None)
    }

    pub fn cpu_wait_mad(&self) -> (u64, u64) {
        Self::mad(&self.cpu_wait, None)
    }

    pub fn net_recv_mad(&self) -> (u64, u64) {
        Self::mad(&self.net_recv, Some(1_000_000f64))
    }

    pub fn net_send_mad(&self) -> (u64, u64) {
        Self::mad(&self.net_send, Some(1_000_000f64))
    }

    pub fn mem_used_mad(&self) -> (u64, u64) {
        Self::mad(&self.mem_used, Some(1_000_000f64))
    }

    // mad: mean and standard-deviation.
    fn mad(hist: &HistogramCompress, norm: Option<f64>) -> (u64, u64) {
        let mut mean = hist.mean();
        let mut stddev = hist.stddev();
        if let Some(norm) = norm {
            mean /= norm;
            stddev /= norm;
        }
        (mean.round() as u64, stddev.round() as u64)
    }
}

impl fmt::Debug for DstatCompress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let usr = self.cpu_usr_mad();
        let sys = self.cpu_sys_mad();
        let wait = self.cpu_wait_mad();
        let recv = self.net_recv_mad();
        let send = self.net_send_mad();
        let used = self.mem_used_mad();
        writeln!(f, "cpu:")?;
        writeln!(f, "  usr              {:>4}   stddev={}", usr.0, usr.1)?;
        writeln!(f, "  sys              {:>4}   stddev={}", sys.0, sys.1)?;
        writeln!(f, "  wait             {:>4}   stddev={}", wait.0, wait.1)?;
        writeln!(f, "net:")?;
        writeln!(f, "  (MB/s) receive   {:>4}   stddev={}", recv.0, recv.1)?;
        writeln!(f, "  (MB/s) send      {:>4}   stddev={}", send.0, send.1)?;
        writeln!(f, "mem:")?;
        writeln!(f, "  (MB) used        {:>4}   stddev={}", used.0, used.1)?;
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct HistogramCompress {
    min: f64,
    max: f64,
    mean: f64,
    stddev: f64,
    percentiles: HashMap<String, f64>,
}

impl HistogramCompress {
    fn from(histogram: &Histogram) -> Self {
        let min = histogram.min().value();
        let max = histogram.max().value();
        let mean = histogram.mean().value();
        let stddev = histogram.stddev().value();
        // all percentiles from 0.01 to 1.0 (step by 0.01) + 0.951 + 0.999 (step
        // by 0.001) + 0.9999 + 0.99999
        let percentiles = (0..100)
            .map(|percentile| percentile as f64 / 100f64)
            .chain(
                (950..=998)
                    .step_by(2)
                    .map(|percentile| percentile as f64 / 1000f64),
            )
            .chain(vec![0.999, 0.9999, 0.99999])
            .map(|percentile| {
                (
                    percentile.to_string(),
                    histogram.percentile(percentile).value(),
                )
            })
            .collect();
        Self {
            min,
            max,
            mean,
            stddev,
            percentiles,
        }
    }

    pub fn min(&self) -> f64 {
        self.min
    }

    pub fn max(&self) -> f64 {
        self.max
    }

    pub fn mean(&self) -> f64 {
        self.mean
    }

    pub fn stddev(&self) -> f64 {
        self.stddev
    }

    pub fn percentile(&self, percentile: f64) -> f64 {
        if let Some(value) = self.percentiles.get(&percentile.to_string()) {
            *value
        } else {
            panic!(
                "percentile {:?} should exist in compressed histogram",
                percentile
            )
        }
    }
}

// same as `Histogram`'s
impl fmt::Debug for HistogramCompress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "min={:<6} max={:<6} avg={:<6} p5={:<6} p95={:<6} p99={:<6} p99.9={:<6} p99.99={:<6}",
            self.min().round(),
            self.max().round(),
            self.mean().round(),
            self.percentile(0.05).round(),
            self.percentile(0.95).round(),
            self.percentile(0.99).round(),
            self.percentile(0.999).round(),
            self.percentile(0.9999).round()
        )
    }
}
