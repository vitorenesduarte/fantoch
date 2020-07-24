use color_eyre::eyre::WrapErr;
use color_eyre::Report;
use csv::ReaderBuilder;
use fantoch::metrics::Histogram;
use serde::{Deserialize, Deserializer};
use std::fs::File;
use std::io::{BufRead, BufReader};

#[derive(Clone)]
pub struct Dstat {
    pub cpu_usr: Histogram,
    pub cpu_sys: Histogram,
    pub cpu_wait: Histogram,
    pub net_recv: Histogram,
    pub net_send: Histogram,
    pub mem_used: Histogram,
}

impl Dstat {
    pub fn new() -> Self {
        Self {
            cpu_usr: Histogram::new(),
            cpu_sys: Histogram::new(),
            cpu_wait: Histogram::new(),
            net_recv: Histogram::new(),
            net_send: Histogram::new(),
            mem_used: Histogram::new(),
        }
    }

    pub fn merge(&mut self, other: &Self) {
        self.cpu_usr.merge(&other.cpu_usr);
        self.cpu_sys.merge(&other.cpu_sys);
        self.cpu_wait.merge(&other.cpu_wait);
        self.net_recv.merge(&other.net_recv);
        self.net_send.merge(&other.net_send);
        self.mem_used.merge(&other.mem_used);
    }

    pub fn from(start: u64, end: u64, path: &String) -> Result<Self, Report> {
        // create all histograms
        let mut cpu_usr = Histogram::new();
        let mut cpu_sys = Histogram::new();
        let mut cpu_wait = Histogram::new();
        let mut net_recv = Histogram::new();
        let mut net_send = Histogram::new();
        let mut mem_used = Histogram::new();

        // open csv file
        let file = File::open(path)?;
        let mut buf = BufReader::new(file);

        // skip first 5 lines (non-header lines)
        for _ in 0..5 {
            let mut s = String::new();
            buf.read_line(&mut s)?;
        }

        // create csv reader:
        // - `flexible(true)` makes `reader.records()` not throw a error in case
        //   there's a row with not enough fields
        let mut reader = ReaderBuilder::new().flexible(true).from_reader(buf);

        // get dstat headers
        let headers = reader.headers().wrap_err("csv headers")?.clone();

        for row in reader.records() {
            // fetch row
            let row = row.wrap_err("csv record")?;

            // skip row if doesn't have enough fields/columns
            if row.len() < headers.len() {
                continue;
            }

            // parse csv row
            let row: DstatRow = row.deserialize(Some(&headers))?;

            // only consider the record if within bounds
            if row.epoch >= start && row.epoch <= end {
                cpu_usr.increment(row.cpu_usr);
                cpu_sys.increment(row.cpu_sys);
                cpu_wait.increment(row.cpu_wait);
                net_recv.increment(row.net_recv);
                net_send.increment(row.net_send);
                mem_used.increment(row.mem_used);
            }
        }

        // create self
        let dstat = Self {
            cpu_usr,
            cpu_sys,
            cpu_wait,
            net_recv,
            net_send,
            mem_used,
        };
        Ok(dstat)
    }
}

// Fields we're generating:
// "time","epoch","usr","sys","idl","wai","stl","read","writ","recv","send"
// ,"used","free","buff","cach","read","writ"
#[derive(Debug, Deserialize)]
struct DstatRow {
    #[serde(deserialize_with = "parse_epoch")]
    epoch: u64,

    // cpu metrics
    #[serde(rename = "usr")]
    #[serde(deserialize_with = "f64_to_u64")]
    cpu_usr: u64,
    #[serde(rename = "sys")]
    #[serde(deserialize_with = "f64_to_u64")]
    cpu_sys: u64,
    #[serde(rename = "wai")]
    #[serde(deserialize_with = "f64_to_u64")]
    cpu_wait: u64,

    // net metrics
    #[serde(rename = "recv")]
    #[serde(deserialize_with = "f64_to_u64")]
    net_recv: u64,
    #[serde(rename = "send")]
    #[serde(deserialize_with = "f64_to_u64")]
    net_send: u64,

    // memory metrics
    #[serde(rename = "used")]
    #[serde(deserialize_with = "f64_to_u64")]
    mem_used: u64,
}

fn parse_epoch<'de, D>(de: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let epoch = String::deserialize(de)?;
    let epoch = epoch.parse::<f64>().expect("epoch should be a float");
    // convert epoch to milliseconds
    let epoch = epoch * 1000f64;
    let epoch = epoch.round() as u64;
    Ok(epoch)
}

fn f64_to_u64<'de, D>(de: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let n = String::deserialize(de)?;
    let n = n.parse::<f64>().expect("dstat value should be a float");
    let n = n.round() as u64;
    Ok(n)
}
