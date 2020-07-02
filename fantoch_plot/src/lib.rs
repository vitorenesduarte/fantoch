mod fmt;
mod plot;
mod results_db;

// Re-exports.
pub use fmt::PlotFmt;
pub use results_db::ResultsDB;

use color_eyre::Report;
use fantoch::metrics::Histogram;
use fantoch_exp::Protocol;
use plot::axes::Axes;
use plot::figure::Figure;
use plot::ticker::Ticker;
use plot::PyPlot;
use pyo3::prelude::*;
use std::collections::{BTreeMap, HashSet};

// defaults: [6.4, 4.8]
// copied from: https://github.com/jonhoo/thesis/blob/master/graphs/common.py
const GOLDEN_RATIO: f64 = 1.61803f64;
const FIGWIDTH: f64 = 8.5 / GOLDEN_RATIO;
const FIGSIZE: (f64, f64) = (FIGWIDTH, FIGWIDTH / GOLDEN_RATIO);

// margins
const ADJUST_TOP: f64 = 0.85;
const ADJUST_BOTTOM: f64 = 0.15;

const LEGEND_NCOL: usize = 3;

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
    db: &mut ResultsDB,
) -> Result<BTreeMap<(Protocol, usize), Histogram>, Report> {
    const FULL_REGION_WIDTH: f64 = 10f64;
    const MAX_COMBINATIONS: usize = 6;
    // 80% of `FULL_REGION_WIDTH` when `MAX_COMBINATIONS` is reached
    const BAR_WIDTH: f64 = FULL_REGION_WIDTH * 0.8 / MAX_COMBINATIONS as f64;

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

    // keep track of all regions
    let mut all_regions = HashSet::new();

    // return global client metrics for each combination
    let mut global_metrics = BTreeMap::new();

    // start python
    let gil = Python::acquire_gil();
    let py = gil.python();
    let plt = pytry!(py, PyPlot::new(py));

    // start plot
    let (fig, ax) = start_plot(py, &plt)?;

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

    // end plot
    end_plot(output_file, n, py, &plt, fig, ax)?;

    Ok(global_metrics)
}

// based on: https://github.com/jonhoo/thesis/blob/master/graphs/vote-memlimit-cdf.py
pub fn cdf_plot(
    n: usize,
    clients_per_region: usize,
    conflict_rate: usize,
    payload_size: usize,
    output_file: &str,
    db: &mut ResultsDB,
) -> Result<(), Report> {
    // start python
    let gil = Python::acquire_gil();
    let py = gil.python();
    let plt = pytry!(py, PyPlot::new(py));
    let ticker = pytry!(py, Ticker::new(py));

    // start plot
    let (fig, ax) = start_plot(py, &plt)?;

    // percentiles of interest:
    let percentiles: Vec<_> = (10..=60)
        .step_by(10)
        .chain((65..=95).step_by(5))
        .map(|percentile| percentile as f64 / 100f64)
        .chain(vec![0.97, 0.99, 0.999])
        .collect();

    for (protocol, f) in combinations(n) {
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

        // compute x: all values in the global histogram
        let x: Vec<_> = percentiles
            .iter()
            .map(|percentile| {
                exp_data
                    .global_client_latency
                    .percentile(*percentile)
                    .value()
                    .round() as u64
            })
            .collect();

        // compute label
        let label = format!("{} f = {}", PlotFmt::protocol_name(protocol), f);
        let kwargs = pytry!(
            py,
            pydict!(
                py,
                // set style
                ("label", label),
                ("color", PlotFmt::color(protocol, f)),
                ("marker", PlotFmt::marker(protocol, f)),
                ("linestyle", PlotFmt::linestyle(protocol, f)),
                ("linewidth", PlotFmt::linewidth(f)),
            )
        );

        pytry!(py, ax.plot(x, percentiles.clone(), None, Some(kwargs)));
    }

    // set y limits
    let kwargs = pytry!(py, pydict!(py, ("ymin", 0), ("ymax", 1)));
    pytry!(py, ax.set_ylim(Some(kwargs)));

    // set log scale on x axis
    pytry!(py, ax.set_xscale("log"));

    // the following two lines are needed before setting the ticklabel format to
    // plain (as suggested here: https://stackoverflow.com/questions/49750107/how-to-remove-scientific-notation-on-a-matplotlib-log-log-plot)
    let formatter = pytry!(py, ticker.scalar_formatter());
    pytry!(py, ax.xaxis.set_major_formatter(formatter));
    let formatter = pytry!(py, ticker.scalar_formatter());
    pytry!(py, ax.xaxis.set_minor_formatter(formatter));

    // prevent scientific notation on x axis
    // - this could be avoided by using the `FormatStrFormatter`
    let kwargs = pytry!(py, pydict!(py, ("axis", "x"), ("style", "plain")));
    pytry!(py, ax.ticklabel_format(Some(kwargs)));

    // set labels
    pytry!(py, ax.set_xlabel("latency (ms)"));
    pytry!(py, ax.set_ylabel("CDF"));

    // end plot
    end_plot(output_file, n, py, &plt, fig, ax)?;

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

fn start_plot<'a>(
    py: Python<'a>,
    plt: &'a PyPlot<'a>,
) -> Result<(Figure<'a>, Axes<'a>), Report> {
    // adjust fig size
    let kwargs = pytry!(py, pydict!(py, ("figsize", FIGSIZE)));
    let (fig, ax) = pytry!(py, plt.subplots(Some(kwargs)));

    // adjust fig margins
    let kwargs = pytry!(
        py,
        pydict!(py, ("top", ADJUST_TOP), ("bottom", ADJUST_BOTTOM))
    );
    pytry!(py, fig.subplots_adjust(Some(kwargs)));

    Ok((fig, ax))
}

fn end_plot(
    output_file: &str,
    n: usize,
    py: Python,
    plt: &PyPlot,
    fig: Figure,
    ax: Axes,
) -> Result<(), Report> {
    // pull legend up
    let y_bbox_to_anchor = match n {
        3 => 1.17,
        5 => 1.24,
        _ => panic!("cdf_plot: unsupported n: {}", n),
    };

    // add legend
    let kwargs = pytry!(
        py,
        pydict!(
            py,
            ("loc", "upper center"),
            ("bbox_to_anchor", (0.5, y_bbox_to_anchor)),
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

    Ok(())
}
