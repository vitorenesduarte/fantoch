mod plot;
mod results_db;

use color_eyre::eyre::WrapErr;
use color_eyre::Report;
use fantoch_exp::Protocol;
use plot::Matplotlib;
use pyo3::prelude::*;
use results_db::ResultsDB;
use std::collections::HashSet;

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

    // compute x:
    let full_region_width = 10f64;
    let x: Vec<_> = (0..n).map(|i| i as f64 * full_region_width).collect();

    // compute bar width: only occupy 80% of the given width
    let bar_width = (full_region_width * 0.8) / combinations.len() as f64;

    // we need to shift all to the left by half of the number of combinations
    let shift_left = combinations.len() as f64 / 2f64;
    // we also need to shift half bar to the right
    let shift_right = 0.5;
    let combinations =
        combinations
            .into_iter()
            .enumerate()
            .map(|(index, combination)| {
                // compute index according to shifts
                let index = index as f64 - shift_left + shift_right;
                // compute combination's shift
                let shift = index * bar_width;
                (shift, combination)
            });

    // start plot
    let gil = Python::acquire_gil();
    let py = gil.python();
    let plt = pytry!(py, Matplotlib::new(py));
    let (fig, ax) = pytry!(py, plt.subplots());

    // keep track of all regions
    let mut all_regions = HashSet::new();

    for (shift, (protocol, n, f)) in combinations {
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
        let kwargs =
            pytry!(py, pydict!(py, ("label", label), ("width", bar_width)));

        // compute x: shift all values by `shift`
        let x: Vec<_> = x.iter().map(|&x| x + shift).collect();
        pytry!(py, ax.bar(x, y, Some(kwargs)));
    }

    // set labels
    pytry!(py, ax.set_ylabel("latency (ms)"));

    // set xticks
    pytry!(py, ax.set_xticks(x));

    // set x labels:
    // - check the number of regions is correct
    assert_eq!(all_regions.len(), n);
    let labels: Vec<_> = all_regions.into_iter().collect();
    pytry!(py, ax.set_xticklabels(labels));

    // add legend
    pytry!(py, ax.legend());

    let kwargs = pytry!(py, pydict!(py, ("format", "pdf")));
    pytry!(py, plt.savefig(output_file, Some(kwargs)));
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

    let kwargs = pydict![py, ("format", "pdf")]?;
    plt.savefig("plot.pdf", Some(kwargs))?;
    Ok(())
}
