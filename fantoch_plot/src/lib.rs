mod fmt;
mod plot;
mod results_db;

use color_eyre::eyre::WrapErr;
use color_eyre::Report;
use fantoch_exp::Protocol;
use fmt::PlotFmt;
use plot::Matplotlib;
use pyo3::prelude::*;
use results_db::ResultsDB;
use std::collections::HashSet;

pub enum ErrorBar {
    With(f64),
    Without,
}

pub fn latency_plot(
    n: usize,
    clients_per_region: usize,
    conflict_rate: usize,
    payload_size: usize,
    error_bar: ErrorBar,
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
            combinations.push((protocol, f));
        }
    }
    let combination_count = combinations.len() as f64;

    // compute x:
    let full_region_width = 10f64;
    let x: Vec<_> = (0..n).map(|i| i as f64 * full_region_width).collect();

    // compute bar width: only occupy 80% of the given width
    let bar_width = (full_region_width * 0.8) / combination_count;

    // we need to shift all to the left by half of the number of combinations
    let shift_left = combination_count / 2f64;
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

    for (shift, (protocol, f)) in combinations {
        println!("{} f = {}", PlotFmt::protocol_name(protocol), f);
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
        let mut err = Vec::new();
        let mut latencies: Vec<_> = exp_data
            .client_latency
            .into_iter()
            .map(|(region, histogram)| {
                let avg = histogram.mean().value().round() as usize;
                if let ErrorBar::With(percentile) = error_bar {
                    let p99_9 = histogram.percentile(percentile).value().round()
                        as usize;
                    let error_bar = (0, p99_9 - avg);
                    err.push(error_bar);
                }
                (region, avg)
            })
            .collect();
        latencies.sort();
        let (regions, y): (HashSet<_>, Vec<_>) = latencies.into_iter().unzip();
        let (from_err, to_err): (Vec<_>, Vec<_>) = err.into_iter().unzip();

        // update set of all regions
        for region in regions {
            all_regions.insert(region);
        }

        // compute label
        let label = format!("{} f = {}", PlotFmt::protocol_name(protocol), f);
        let kwargs = pytry!(
            py,
            pydict!(
                py,
                // set style
                ("label", label),
                ("width", bar_width),
                ("edgecolor", "black"),
                ("linewidth", 1),
                ("color", PlotFmt::color(protocol, f)),
                ("hatch", PlotFmt::hatch(protocol, f)),
            )
        );
        // maybe set error bars
        if let ErrorBar::With(_) = error_bar {
            pytry!(py, kwargs.set_item("yerr", (from_err, to_err)));
        }

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
    let mut regions: Vec<_> = all_regions.into_iter().collect();
    regions.sort();
    // map regions to their pretty name
    let labels: Vec<_> = regions
        .into_iter()
        .map(|region| PlotFmt::region_name(region))
        .collect();
    pytry!(py, ax.set_xticklabels(labels));

    // add legend
    let kwargs = pytry!(
        py,
        pydict!(
            py,
            ("loc", "upper center"),
            ("bbox_to_anchor", (0.5, 1.15)),
            // remove box around legend:
            ("edgecolor", "white"),
            // 2 lines:
            ("ncol", combination_count as u64 / 2),
        )
    );
    pytry!(py, ax.legend(Some(kwargs)));

    // save figure
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
