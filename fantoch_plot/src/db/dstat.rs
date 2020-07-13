use color_eyre::eyre::WrapErr;
use color_eyre::Report;
use csv::Reader;
use fantoch::metrics::Histogram;
use serde::{Deserialize, Deserializer};
use std::fmt;

#[derive(Clone)]
pub struct Dstat {
    pub cpu_usr: Histogram,
    pub cpu_sys: Histogram,
    pub cpu_wait: Histogram,
    pub net_receive: Histogram,
    pub net_send: Histogram,
    pub memory_used: Histogram,
}

impl fmt::Debug for Dstat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "cpu:")?;
        writeln!(
            f,
            "  usr: {} stddev={}",
            self.cpu_usr.mean().value().round() as u64,
            self.cpu_usr.stddev().value().round() as u64,
        )?;
        writeln!(
            f,
            "  sys: {} stddev={}",
            self.cpu_sys.mean().value().round() as u64,
            self.cpu_sys.stddev().value().round() as u64,
        )?;
        writeln!(
            f,
            "  wait: {} stddev={}",
            self.cpu_wait.mean().value().round() as u64,
            self.cpu_wait.stddev().value().round() as u64,
        )?;

        writeln!(f, "net:")?;
        writeln!(
            f,
            "  receive: {}MB stddev={}",
            (self.net_receive.mean().value() / 1_000_000f64).round() as u64,
            self.net_receive.stddev().value().round() as u64,
        )?;
        writeln!(
            f,
            "  send: {}MB stddev={}",
            (self.net_receive.mean().value() / 1_000_000f64).round() as u64,
            self.net_send.stddev().value().round() as u64,
        )?;

        writeln!(
            f,
            "mem: {}MB stddev={}",
            (self.memory_used.mean().value() / 1_000_000f64).round() as u64,
            self.memory_used.stddev().value().round() as u64,
        )?;
        Ok(())
    }
}

impl Dstat {
    pub fn new(start: u64, end: u64, path: String) -> Result<Self, Report> {
        // create all histograms
        let mut cpu_usr = Histogram::new();
        let mut cpu_sys = Histogram::new();
        let mut cpu_wait = Histogram::new();
        let mut net_receive = Histogram::new();
        let mut net_send = Histogram::new();
        let mut memory_used = Histogram::new();

        // parse csv
        let mut reader =
            Reader::from_path(path).wrap_err("creating csv reader")?;
        for record in reader.deserialize() {
            // parse csv row
            let record: DstatRow = record?;
            // only consider the record if within bounds
            if record.epoch >= start && record.epoch <= end {
                cpu_usr.increment(record.cpu_usr);
                cpu_sys.increment(record.cpu_sys);
                cpu_wait.increment(record.cpu_wait);
                net_receive.increment(record.net_receive);
                net_send.increment(record.net_send);
                memory_used.increment(record.memory_used);
            }
        }

        // create self
        let dstat = Self {
            cpu_usr,
            cpu_sys,
            cpu_wait,
            net_receive,
            net_send,
            memory_used,
        };
        Ok(dstat)
    }
}

// All fields:
// "time","epoch","usr","sys","idl","wai","stl","read","writ","recv","send"
// ,"used","free","buff","cach","read","writ"
#[derive(Deserialize)]
struct DstatRow {
    #[serde(deserialize_with = "parse_epoch")]
    epoch: u64,

    // cpu metrics
    #[serde(rename = "usr")]
    cpu_usr: u64,
    #[serde(rename = "sys")]
    cpu_sys: u64,
    /*
    #[serde(rename = "idl")]
    cpu_idle: u64,
    */
    #[serde(rename = "wai")]
    cpu_wait: u64,

    /*
        // disk metrics
        #[serde(rename = "read")]
        disk_read: u64,
        #[serde(rename = "writ")]
        disk_write: u64,
    */
    // net metrics
    #[serde(rename = "recv")]
    net_receive: u64,
    #[serde(rename = "send")]
    net_send: u64,

    // memory metrics
    #[serde(rename = "used")]
    memory_used: u64,
}

fn parse_epoch<'de, D>(de: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let epoch = String::deserialize(de)?;
    let epoch = epoch.parse::<f64>().expect("dstat epoch should be a float");
    let epoch = epoch.round() as u64;
    Ok(epoch)
}
