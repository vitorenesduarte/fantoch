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
use plot::pyplot::PyPlot;
use plot::ticker::Ticker;
use plot::Matplotlib;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::{BTreeMap, HashSet};

// defaults: [6.4, 4.8]
// copied from: https://github.com/jonhoo/thesis/blob/master/graphs/common.py
const GOLDEN_RATIO: f64 = 1.61803f64;
const FIGWIDTH: f64 = 8.5 / GOLDEN_RATIO;
const FIGSIZE: (f64, f64) = (FIGWIDTH, FIGWIDTH / GOLDEN_RATIO);

// margins
const ADJUST_TOP: f64 = 0.85;
const ADJUST_BOTTOM: f64 = 0.15;

pub enum ErrorBar {
    With(f64),
    Without,
}

enum AxisToScale {
    X,
    Y,
}

pub fn set_global_style() -> Result<(), Report> {
    // start python
    let gil = Python::acquire_gil();
    let py = gil.python();

    let lib = pytry!(py, Matplotlib::new(py));
    // need to load `PyPlot` for the following to work (which is just weird)
    let _ = pytry!(py, PyPlot::new(py));

    // adjust fig size
    let kwargs = pytry!(py, pydict!(py, ("figsize", FIGSIZE)));
    pytry!(py, lib.rc("figure", Some(kwargs)));

    // adjust font size
    let kwargs = pytry!(py, pydict!(py, ("size", 9)));
    pytry!(py, lib.rc("font", Some(kwargs)));
    let kwargs = pytry!(py, pydict!(py, ("fontsize", 10)));
    pytry!(py, lib.rc("legend", Some(kwargs)));

    // adjust axes linewidth
    let kwargs = pytry!(py, pydict!(py, ("linewidth", 1)));
    pytry!(py, lib.rc("axes", Some(kwargs)));

    Ok(())
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
    let mut combination_count = combinations.len();

    // compute x:
    let x: Vec<_> = (0..n).map(|i| i as f64 * FULL_REGION_WIDTH).collect();

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
    let (fig, ax) = start_plot(py, &plt, None)?;

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
                // reduce the number of combinations
                combination_count -= 1;
                continue;
            },
            1 => (),
            _ => panic!("found more than 1 matching experiment for this search criteria"),
        };
        let exp_data = exp_data.pop().unwrap();

        // compute y: avg latencies sorted by region name
        let mut err = Vec::new();
        let mut latencies: Vec<_> = exp_data
            .client_latency
            .into_iter()
            .map(|(region, histogram)| {
                // compute average latency
                let avg = histogram.mean().value().round() as usize;

                // maybe create error bar
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

        // compute x: shift all values by `shift`
        let x: Vec<_> = x.iter().map(|&x| x + shift).collect();

        // plot it:
        // - maybe set error bars
        let kwargs = bar_style(py, protocol, f, BAR_WIDTH)?;
        if let ErrorBar::With(_) = error_bar {
            pytry!(py, kwargs.set_item("yerr", (from_err, to_err)));
        }

        pytry!(py, ax.bar(x, y, Some(kwargs)));

        // save global client metrics
        global_metrics.insert((protocol, f), exp_data.global_client_latency);

        // update set of all regions
        for region in regions {
            all_regions.insert(region);
        }
    }

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

    // set labels
    pytry!(py, ax.set_ylabel("latency (ms)"));

    // legend
    add_legend(combination_count as usize, n, py, &ax)?;

    // end plot
    end_plot(output_file, py, &plt, fig)?;

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

    // start plot
    let (fig, ax) = start_plot(py, &plt, None)?;

    let combinations = combinations(n);
    let mut combination_count = combinations.len();
    for (protocol, f) in combinations {
        inner_cdf_plot(
            py,
            &ax,
            n,
            f,
            protocol,
            clients_per_region,
            conflict_rate,
            payload_size,
            &mut combination_count,
            db,
        )?;
    }

    // set cdf plot style
    inner_cdf_plot_style(py, &ax)?;

    // legend
    add_legend(combination_count, n, py, &ax)?;

    // end plot
    end_plot(output_file, py, &plt, fig)?;

    Ok(())
}

pub fn cdf_plots(
    n: usize,
    clients_per_region: usize,
    conflict_rate: usize,
    payload_size: usize,
    output_file: &str,
    db: &mut ResultsDB,
) -> Result<(), Report> {
    let (mut protocols, mut fs): (Vec<_>, Vec<_>) =
        combinations(n).into_iter().unzip();
    protocols.sort_by_key(|&protocol| PlotFmt::protocol_name(protocol));
    protocols.dedup();
    fs.sort();
    fs.dedup();
    match fs.as_slice() {
        [1, 2] => (),
        _ => panic!(
            "cdf_plots: unsupported f values: {:?}; use cdf_plot instead",
            fs
        ),
    };

    // start python
    let gil = Python::acquire_gil();
    let py = gil.python();
    let plt = pytry!(py, PyPlot::new(py));

    // start plot
    let height_between_subplots = Some(0.5);
    let (fig, _) = start_plot(py, &plt, height_between_subplots)?;

    let mut previous_axis: Option<Axes> = None;

    for f in vec![2, 1] {
        let mut combination_count = protocols.len();
        let mut hide_xticklabels = false;

        // create subplot (shared axis with the previous subplot (if any))
        let kwargs = match previous_axis {
            None => None,
            Some(previous_axis) => {
                // share the axis with f = 2so that the plots have the same
                // scale; also, hide the labels for f = 1
                hide_xticklabels = true;
                Some(pytry!(py, pydict!(py, ("sharex", previous_axis.ax()))))
            }
        };
        let ax = pytry!(py, plt.subplot(2, 1, f, kwargs));

        for &protocol in protocols.iter() {
            inner_cdf_plot(
                py,
                &ax,
                n,
                f,
                protocol,
                clients_per_region,
                conflict_rate,
                payload_size,
                &mut combination_count,
                db,
            )?;
        }

        // set cdf plot style
        inner_cdf_plot_style(py, &ax)?;

        // additional style: maybe hide x-axis labels
        if hide_xticklabels {
            pytry!(py, ax.xaxis.set_visible(false));
        }

        // legend
        add_subplot_legend(combination_count, n, py, &ax)?;

        // save axis
        previous_axis = Some(ax);
    }

    // end plot
    end_plot(output_file, py, &plt, fig)?;

    Ok(())
}

fn inner_cdf_plot_style(py: Python, ax: &Axes) -> Result<(), Report> {
    // set y limits
    let kwargs = pytry!(py, pydict!(py, ("ymin", 0), ("ymax", 1)));
    pytry!(py, ax.set_ylim(Some(kwargs)));

    // set log scale on x axis
    set_log_scale(py, &ax, AxisToScale::X)?;

    // set labels
    pytry!(py, ax.set_xlabel("latency (ms) [log-scale]"));
    pytry!(py, ax.set_ylabel("CDF"));

    Ok(())
}

fn inner_cdf_plot(
    py: Python,
    ax: &Axes,
    n: usize,
    f: usize,
    protocol: Protocol,
    clients_per_region: usize,
    conflict_rate: usize,
    payload_size: usize,
    combination_count: &mut usize,
    db: &mut ResultsDB,
) -> Result<(), Report> {
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
            eprintln!(
                "missing data for {} f = {}",
                PlotFmt::protocol_name(protocol),
                f
            );
            // reduce the number of combinations
            *combination_count -= 1;
            return Ok(());
        }
        1 => (),
        _ => panic!(
            "found more than 1 matching experiment for this search criteria"
        ),
    };
    let exp_data = exp_data.pop().unwrap();

    // compute x: all values in the global histogram
    let x: Vec<_> = percentiles()
        .map(|percentile| {
            exp_data
                .global_client_latency
                .percentile(percentile)
                .value()
                .round() as u64
        })
        .collect();

    // compute y: percentiles!
    let y: Vec<_> = percentiles().collect();

    // plot it!
    let kwargs = line_style(py, protocol, f)?;
    pytry!(py, ax.plot(x, y, None, Some(kwargs)));

    Ok(())
}

pub fn throughput_latency_plot(
    n: usize,
    clients_per_region: Vec<usize>,
    conflict_rate: usize,
    payload_size: usize,
    output_file: &str,
    db: &mut ResultsDB,
) -> Result<(), Report> {
    // start python
    let gil = Python::acquire_gil();
    let py = gil.python();
    let plt = pytry!(py, PyPlot::new(py));

    // start plot
    let (fig, ax) = start_plot(py, &plt, None)?;

    let combinations = combinations(n);
    let mut combination_count = combinations.len();

    for (protocol, f) in combinations {
        // compute y: latency values for each number of clients
        let mut y = Vec::with_capacity(clients_per_region.len());
        for &clients in clients_per_region.iter() {
            let mut exp_data = db
                .search()
                .n(n)
                .f(f)
                .protocol(protocol)
                .clients_per_region(clients)
                .conflict_rate(conflict_rate)
                .payload_size(payload_size)
                .load()?;
            match exp_data.len() {
                0 => {
                    eprintln!("missing data for {} f = {}", PlotFmt::protocol_name(protocol), f);
                    y.push(0f64);
                    // reduce the number of combinations
                    combination_count -= 1;
                    continue;
                },
                1 => (),
                _ => panic!("found more than 1 matching experiment for this search criteria"),
            };
            let exp_data = exp_data.pop().unwrap();

            // get average latency
            let latency = exp_data.global_client_latency.mean().value();
            y.push(latency);
        }

        // compute x: compute throughput given average latency and number of
        // clients
        let (x, y): (Vec<_>, Vec<_>) = y
            .iter()
            .zip(clients_per_region.iter())
            .filter_map(|(&latency, &clients)| {
                if latency == 0f64 {
                    None
                } else {
                    let per_second = 1000f64 / latency;
                    let per_site = clients as f64 * per_second;
                    let throughput = n as f64 * per_site;
                    // compute K ops
                    let x = throughput / 1000f64;
                    // round y
                    let y = latency.round() as usize;
                    Some((x, y))
                }
            })
            .unzip();

        // plot it!
        let kwargs = line_style(py, protocol, f)?;
        pytry!(py, ax.plot(x, y, None, Some(kwargs)));
    }

    // set log scale on y axis
    set_log_scale(py, &ax, AxisToScale::Y)?;

    // set labels
    pytry!(py, ax.set_xlabel("throughput (K ops/s)"));
    pytry!(py, ax.set_ylabel("latency (ms) [log-scale]"));

    // legend
    add_legend(combination_count, n, py, &ax)?;

    // end plot
    end_plot(output_file, py, &plt, fig)?;

    Ok(())
}

fn combinations(n: usize) -> Vec<(Protocol, usize)> {
    let mut protocols = vec![
        Protocol::NewtAtomic,
        Protocol::AtlasLocked,
        Protocol::FPaxos,
    ];
    protocols.sort_by_key(|&protocol| PlotFmt::protocol_name(protocol));
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

// percentiles of interest
fn percentiles() -> impl Iterator<Item = f64> {
    (10..=60)
        .step_by(10)
        .chain((65..=95).step_by(5))
        .map(|percentile| percentile as f64 / 100f64)
        .chain(vec![0.97, 0.99, 0.999])
}

fn start_plot<'a>(
    py: Python<'a>,
    plt: &'a PyPlot<'a>,
    height_between_subplots: Option<f64>,
) -> Result<(Figure<'a>, Axes<'a>), Report> {
    let (fig, ax) = pytry!(py, plt.subplots(None));

    // adjust fig margins
    let kwargs = pytry!(
        py,
        pydict!(py, ("top", ADJUST_TOP), ("bottom", ADJUST_BOTTOM))
    );
    // maybe also set `hspace`
    if let Some(hspace) = height_between_subplots {
        pytry!(py, kwargs.set_item("hspace", hspace));
    }
    pytry!(py, fig.subplots_adjust(Some(kwargs)));

    Ok((fig, ax))
}

fn end_plot(
    output_file: &str,
    py: Python,
    plt: &PyPlot,
    fig: Figure,
) -> Result<(), Report> {
    // save figure
    let kwargs = pytry!(py, pydict!(py, ("format", "pdf")));
    pytry!(py, plt.savefig(output_file, Some(kwargs)));

    // close the figure
    pytry!(py, plt.close(fig));

    Ok(())
}

fn add_legend(
    combination_count: usize,
    n: usize,
    py: Python,
    ax: &Axes,
) -> Result<(), Report> {
    // pull legend up
    let y_bbox_to_anchor = match n {
        3 => 1.17,
        5 => 1.24,
        _ => panic!("add_legend: unsupported n: {}", n),
    };

    do_add_legend(combination_count, y_bbox_to_anchor, py, ax)
}

fn add_subplot_legend(
    combination_count: usize,
    n: usize,
    py: Python,
    ax: &Axes,
) -> Result<(), Report> {
    // pull legend up
    let y_bbox_to_anchor = if n != 3 {
        1.37
    } else {
        panic!("add_subplot_legend: unsupported n: {}", n)
    };

    do_add_legend(combination_count, y_bbox_to_anchor, py, ax)
}

fn do_add_legend(
    combination_count: usize,
    y_bbox_to_anchor: f64,
    py: Python,
    ax: &Axes,
) -> Result<(), Report> {
    let legend_ncol = match combination_count {
        1 => 1,
        2 => 2,
        3 => 3,
        4 => 2,
        5 => 3,
        6 => 3,
        _ => panic!("do_add_legend: unsupported number of combinations"),
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
            ("ncol", legend_ncol),
        )
    );
    pytry!(py, ax.legend(Some(kwargs)));

    Ok(())
}

fn set_log_scale(
    py: Python,
    ax: &Axes,
    axis_to_scale: AxisToScale,
) -> Result<(), Report> {
    let ticker = pytry!(py, Ticker::new(py));

    // set log scale on axis
    match axis_to_scale {
        AxisToScale::X => {
            pytry!(py, ax.set_xscale("log"));
        }
        AxisToScale::Y => {
            pytry!(py, ax.set_yscale("log"));
        }
    }

    // the following lines are needed before setting the ticklabel format to
    // plain (as suggested here: https://stackoverflow.com/questions/49750107/how-to-remove-scientific-notation-on-a-matplotlib-log-log-plot)
    let major_formatter = pytry!(py, ticker.scalar_formatter());
    let minor_formatter = pytry!(py, ticker.scalar_formatter());
    match axis_to_scale {
        AxisToScale::X => {
            pytry!(py, ax.xaxis.set_major_formatter(major_formatter));
            pytry!(py, ax.xaxis.set_minor_formatter(minor_formatter));
        }
        AxisToScale::Y => {
            pytry!(py, ax.yaxis.set_major_formatter(major_formatter));
            pytry!(py, ax.yaxis.set_minor_formatter(minor_formatter));
        }
    }

    // prevent scientific notation on the axis
    // - this could be avoided by using the `FormatStrFormatter` instead of the
    //   `ScalarFormatter`
    let axis = match axis_to_scale {
        AxisToScale::X => "x",
        AxisToScale::Y => "y",
    };
    let kwargs = pytry!(py, pydict!(py, ("axis", axis), ("style", "plain")));
    pytry!(py, ax.ticklabel_format(Some(kwargs)));

    // prune minor ticks
    prune_minor_ticks(py, ax, axis_to_scale)?;

    Ok(())
}

fn prune_minor_ticks(
    py: Python,
    ax: &Axes,
    axis_to_scale: AxisToScale,
) -> Result<(), Report> {
    Ok(())
}

fn bar_style(
    py: Python,
    protocol: Protocol,
    f: usize,
    bar_width: f64,
) -> Result<&PyDict, Report> {
    let kwargs = pytry!(
        py,
        pydict!(
            py,
            ("label", PlotFmt::label(protocol, f)),
            ("width", bar_width),
            ("edgecolor", "black"),
            ("linewidth", 1),
            ("color", PlotFmt::color(protocol, f)),
            ("hatch", PlotFmt::hatch(protocol, f)),
        )
    );
    Ok(kwargs)
}

fn line_style(
    py: Python,
    protocol: Protocol,
    f: usize,
) -> Result<&PyDict, Report> {
    let kwargs = pytry!(
        py,
        pydict!(
            py,
            ("label", PlotFmt::label(protocol, f)),
            ("color", PlotFmt::color(protocol, f)),
            ("marker", PlotFmt::marker(protocol, f)),
            ("linestyle", PlotFmt::linestyle(protocol, f)),
            ("linewidth", PlotFmt::linewidth(f)),
        )
    );
    Ok(kwargs)
}
