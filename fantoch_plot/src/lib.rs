mod fmt;
mod plot;
mod results_db;

// Re-exports.
pub use fmt::PlotFmt;
pub use results_db::ResultsDB;

use color_eyre::Report;
use fantoch::metrics::Histogram;
use fantoch_exp::Protocol;
use plot::Matplotlib;
use pyo3::prelude::*;
use std::collections::{BTreeMap, HashSet};

pub enum ErrorBar {
    With(f64),
    Without,
}

const FULL_REGION_WIDTH: f64 = 10f64;
const MAX_COMBINATIONS: usize = 6;
// 80% of `FULL_REGION_WIDTH` when `MAX_COMBINATIONS` is reached
const BAR_WIDTH: f64 = FULL_REGION_WIDTH * 0.8 / MAX_COMBINATIONS as f64;
const LEGEND_NCOL: usize = 3;

pub fn latency_plot(
    n: usize,
    clients_per_region: usize,
    conflict_rate: usize,
    payload_size: usize,
    error_bar: ErrorBar,
    output_file: &str,
    db: &mut ResultsDB,
) -> Result<BTreeMap<(Protocol, usize), Histogram>, Report> {
    let combinations = combinations(n);
    assert!(combinations.len() <= MAX_COMBINATIONS);
    let combination_count = combinations.len() as f64;

    // compute x:
    let x: Vec<_> = (0..n).map(|i| i as f64 * FULL_REGION_WIDTH).collect();

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
                let shift = index * BAR_WIDTH;
                (shift, combination)
            });

    // start plot
    let gil = Python::acquire_gil();
    let py = gil.python();
    let plt = pytry!(py, Matplotlib::new(py));
    let (fig, ax) = pytry!(py, plt.subplots());

    // keep track of all regions
    let mut all_regions = HashSet::new();

    // return global client metrics for each combination
    let mut global_metrics = BTreeMap::new();

    for (shift, (protocol, f)) in combinations {
        let mut exp_data = db
            .search()
            .n(n)
            .f(f)
            .protocol(protocol)
            .clients_per_region(clients_per_region)
            .conflict_rate(conflict_rate)
            .payload_size(payload_size)
            .load()?;
        match exp_data.len() {
            0 => {
                eprintln!("missing data for {} f = {}", PlotFmt::protocol_name(protocol), f);
                continue;
            },
            1 => (),
            _ => panic!("found more than 1 matching experiment for this search criteria"),
        };
        let exp_data = exp_data.pop().unwrap();

        // save global client metrics
        global_metrics.insert((protocol, f), exp_data.global_client_latency);

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
                ("width", BAR_WIDTH),
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
            ("ncol", LEGEND_NCOL),
        )
    );
    pytry!(py, ax.legend(Some(kwargs)));

    // save figure
    let kwargs = pytry!(py, pydict!(py, ("format", "pdf")));
    pytry!(py, plt.savefig(output_file, Some(kwargs)));

    // close the figure
    pytry!(py, plt.close(fig));
    Ok(global_metrics)
}

pub fn cdf_plot(
    n: usize,
    clients_per_region: usize,
    conflict_rate: usize,
    payload_size: usize,
    output_file: &str,
    db: &mut ResultsDB,
) -> Result<(), Report> {
    Ok(())
}

fn combinations(n: usize) -> Vec<(Protocol, usize)> {
    let protocols = vec![
        Protocol::NewtAtomic,
        Protocol::AtlasLocked,
        Protocol::FPaxos,
    ];
    let max_f = match n {
        3 => 1,
        5 => 2,
        _ => panic!("combinations: unsupported n = {}", n),
    };

    // compute all protocol combinations
    let mut combinations = Vec::new();
    for protocol in protocols {
        for f in 1..=max_f {
            combinations.push((protocol, f));
        }
    }

    combinations
}
