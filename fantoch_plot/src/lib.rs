mod plot;
mod results_db;

use color_eyre::eyre::{self, WrapErr};
use color_eyre::Report;
use fantoch_exp::Protocol;
use plot::Matplotlib;
use pyo3::prelude::*;
use results_db::ResultsDB;
use std::collections::HashSet;

macro_rules! pytry {
    ($e:expr) => {{
        match $e {
            Ok(v) => v,
            Err(e) => eyre::bail!("{:?}", e),
        }
    }};
}

pub fn latency_plot(
    n: usize,
    clients_per_region: usize,
    conflict_rate: usize,
    payload_size: usize,
    output_file: &str,
    results_dir: &str,
) -> Result<(), Report> {
    let db = ResultsDB::load(results_dir).wrap_err("load results")?;

    let protocols = vec![
        Protocol::NewtAtomic,
        Protocol::AtlasLocked,
        Protocol::FPaxos,
    ];
    let max_f = if n == 3 { 1 } else { 2 };

    // compute all protocol combinations
    let mut combinations = Vec::new();
    for protocol in protocols {
        for f in 1..=max_f {
            combinations.push((protocol, n, f));
        }
    }

    // compute x: we have as sites
    let x: Vec<_> = (0..n).map(|i| i * 10).collect();

    // start plot
    let gil = Python::acquire_gil();
    let py = gil.python();
    let plt = pytry!(Matplotlib::new(py));
    let (fig, ax) = pytry!(plt.subplots());

    // keep track of all regions
    let mut all_regions = HashSet::new();

    for (protocol, n, f) in combinations {
        let mut exp_data = db
            .search()
            .n(n)
            .f(f)
            .protocol(protocol)
            .clients_per_region(clients_per_region)
            .conflict_rate(conflict_rate)
            .payload_size(payload_size)
            .load()?;
        assert_eq!(exp_data.len(), 1);
        let exp_data = exp_data.pop().unwrap();

        // compute y: avg latencies sorted by region name
        let mut latencies: Vec<_> = exp_data
            .client_latency
            .into_iter()
            .map(|(region, histogram)| {
                let region_name = region.name().to_string();
                let avg_latency = histogram.mean().value().round() as usize;
                (region_name, avg_latency)
            })
            .collect();
        latencies.sort();
        let (regions, y): (HashSet<_>, Vec<_>) = latencies.into_iter().unzip();

        // update set of all regions
        for region in regions {
            all_regions.insert(region);
        }

        // compute label
        let label = format!("{} f = {}", protocol, f);
        let kwargs = &[("label", label)];
        pytry!(ax.bar(x.clone(), y, Some(kwargs), py));
    }

    // set labels
    pytry!(ax.set_ylabel("latency (ms)"));

    // set xticks
    pytry!(ax.set_xticks(x));

    // set x labels:
    // - check the number of regions is correct
    assert_eq!(all_regions.len(), n);
    let labels: Vec<_> = all_regions.into_iter().collect();
    pytry!(ax.set_xticklabels(labels));

    // add legend
    pytry!(ax.legend());

    let kwargs = &[("format", "pdf")];
    pytry!(plt.savefig(output_file, Some(kwargs), py));
    Ok(())
}

pub fn single_plot() -> PyResult<()> {
    let gil = Python::acquire_gil();
    let py = gil.python();
    let plt = Matplotlib::new(py)?;

    let x = vec!["us-east-1", "ca-central-1", "eu-west-2"];
    let y = vec![10, 20, 30];
    plt.plot(x, y, "o-")?;
    plt.title("latency per region")?;
    plt.xlabel("regions")?;
    plt.ylabel("latency (ms)")?;

    let kwargs = &[("format", "pdf")];
    plt.savefig("plot.pdf", Some(kwargs), py)?;
    Ok(())
}
