#![deny(rust_2018_idioms)]

mod fmt;
mod plot;
mod results_db;

// Re-exports.
pub use fmt::PlotFmt;
pub use results_db::ResultsDB;

use color_eyre::Report;
use fantoch::client::KeyGen;
use fantoch::metrics::Histogram;
use fantoch_exp::Protocol;
use plot::axes::Axes;
use plot::figure::Figure;
use plot::pyplot::PyPlot;
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

pub enum Latency {
    Average,
    Percentile(f64),
}

enum AxisToScale {
    X,
    Y,
}

pub fn set_global_style() -> Result<(), Report> {
    // start python
    let gil = Python::acquire_gil();
    let py = gil.python();

    let lib = Matplotlib::new(py)?;
    // need to load `PyPlot` for the following to work (which is just weird)
    let _ = PyPlot::new(py)?;

    // adjust fig size
    let kwargs = pydict!(py, ("figsize", FIGSIZE));
    lib.rc("figure", Some(kwargs))?;

    // adjust font size
    let kwargs = pydict!(py, ("size", 9));
    lib.rc("font", Some(kwargs))?;
    let kwargs = pydict!(py, ("fontsize", 10));
    lib.rc("legend", Some(kwargs))?;

    // adjust axes linewidth
    let kwargs = pydict!(py, ("linewidth", 1));
    lib.rc("axes", Some(kwargs))?;

    Ok(())
}

pub fn latency_plot(
    n: usize,
    clients_per_region: usize,
    key_gen: KeyGen,
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
    let plt = PyPlot::new(py)?;

    // start plot
    let (fig, ax) = start_plot(py, &plt, None)?;

    // keep track of the number of plotted instances
    let mut plotted = 0;

    for (shift, (protocol, f)) in combinations {
        // start search
        let mut search = db.search();
        let mut exp_data = search
            .n(n)
            .f(f)
            .protocol(protocol)
            .clients_per_region(clients_per_region)
            .key_gen(key_gen)
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

        // compute y: avg latencies sorted by region name
        let mut err = Vec::new();
        let mut latencies: Vec<_> = exp_data
            .client_latency
            .iter()
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

                (region.clone(), avg)
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

        ax.bar(x, y, Some(kwargs))?;
        plotted += 1;

        // save global client metrics
        global_metrics
            .insert((protocol, f), exp_data.global_client_latency.clone());

        // update set of all regions
        for region in regions {
            all_regions.insert(region);
        }
    }

    // set xticks
    ax.set_xticks(x, None)?;

    // set x labels:
    // - check the number of regions is correct
    assert_eq!(all_regions.len(), n);
    let mut regions: Vec<_> = all_regions.into_iter().collect();
    regions.sort();
    // map regions to their pretty name
    let labels: Vec<_> =
        regions.into_iter().map(PlotFmt::region_name).collect();
    ax.set_xticklabels(labels, None)?;

    // set labels
    ax.set_ylabel("latency (ms)")?;

    // legend
    add_legend(plotted, n, py, &ax)?;

    // end plot
    end_plot(output_file, py, &plt, fig)?;

    Ok(global_metrics)
}

// based on: https://github.com/jonhoo/thesis/blob/master/graphs/vote-memlimit-cdf.py
pub fn cdf_plot(
    n: usize,
    clients_per_region: usize,
    key_gen: KeyGen,
    payload_size: usize,
    output_file: &str,
    db: &mut ResultsDB,
) -> Result<(), Report> {
    // start python
    let gil = Python::acquire_gil();
    let py = gil.python();
    let plt = PyPlot::new(py)?;

    // start plot
    let (fig, ax) = start_plot(py, &plt, None)?;

    // keep track of the number of plotted instances
    let mut plotted = 0;

    for (protocol, f) in combinations(n) {
        inner_cdf_plot(
            py,
            &ax,
            n,
            f,
            protocol,
            clients_per_region,
            key_gen,
            payload_size,
            &mut plotted,
            db,
        )?;
    }

    // set cdf plot style
    inner_cdf_plot_style(py, &ax)?;

    // legend
    add_legend(plotted, n, py, &ax)?;

    // end plot
    end_plot(output_file, py, &plt, fig)?;

    Ok(())
}

pub fn cdf_plots(
    n: usize,
    clients_per_region: usize,
    key_gen: KeyGen,
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
    let plt = PyPlot::new(py)?;

    // start plot
    let height_between_subplots = Some(0.5);
    let (fig, _) = start_plot(py, &plt, height_between_subplots)?;

    let mut previous_axis: Option<Axes<'_>> = None;

    for f in vec![2, 1] {
        let mut hide_xticklabels = false;

        // create subplot (shared axis with the previous subplot (if any))
        let kwargs = match previous_axis {
            None => None,
            Some(previous_axis) => {
                // share the axis with f = 2so that the plots have the same
                // scale; also, hide the labels for f = 1
                hide_xticklabels = true;
                Some(pydict!(py, ("sharex", previous_axis.ax())))
            }
        };
        let ax = plt.subplot(2, 1, f, kwargs)?;

        // keep track of the number of plotted instances
        let mut plotted = 0;

        for &protocol in protocols.iter() {
            inner_cdf_plot(
                py,
                &ax,
                n,
                f,
                protocol,
                clients_per_region,
                key_gen,
                payload_size,
                &mut plotted,
                db,
            )?;
        }

        // set cdf plot style
        inner_cdf_plot_style(py, &ax)?;

        // additional style: maybe hide x-axis labels
        if hide_xticklabels {
            ax.xaxis.set_visible(false)?;
        }

        // legend
        add_subplot_legend(plotted, n, py, &ax)?;

        // save axis
        previous_axis = Some(ax);
    }

    // end plot
    end_plot(output_file, py, &plt, fig)?;

    Ok(())
}

fn inner_cdf_plot_style(py: Python<'_>, ax: &Axes<'_>) -> Result<(), Report> {
    // set y limits
    let kwargs = pydict!(py, ("ymin", 0), ("ymax", 1));
    ax.set_ylim(Some(kwargs))?;

    // set log scale on x axis
    set_log_scale(py, ax, AxisToScale::X)?;

    // set labels
    ax.set_xlabel("latency (ms) [log-scale]")?;
    ax.set_ylabel("CDF")?;

    Ok(())
}

fn inner_cdf_plot(
    py: Python<'_>,
    ax: &Axes<'_>,
    n: usize,
    f: usize,
    protocol: Protocol,
    clients_per_region: usize,
    key_gen: KeyGen,
    payload_size: usize,
    plotted: &mut usize,
    db: &mut ResultsDB,
) -> Result<(), Report> {
    // start search
    let mut search = db.search();
    let mut exp_data = search
        .n(n)
        .f(f)
        .protocol(protocol)
        .clients_per_region(clients_per_region)
        .key_gen(key_gen)
        .payload_size(payload_size)
        .load()?;
    match exp_data.len() {
        0 => {
            eprintln!(
                "missing data for {} f = {}",
                PlotFmt::protocol_name(protocol),
                f
            );
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
    ax.plot(x, y, None, Some(kwargs))?;
    *plotted += 1;

    Ok(())
}

pub fn throughput_latency_plot(
    n: usize,
    clients_per_region: Vec<usize>,
    key_gen: KeyGen,
    payload_size: usize,
    latency: Latency,
    output_file: &str,
    db: &mut ResultsDB,
) -> Result<(), Report> {
    // start python
    let gil = Python::acquire_gil();
    let py = gil.python();
    let plt = PyPlot::new(py)?;

    // start plot
    let (fig, ax) = start_plot(py, &plt, None)?;

    // keep track of the number of plotted instances
    let mut plotted = 0;

    for (protocol, f) in combinations(n) {
        // keep track of average latency that will be used to compute throughput
        let mut avg_latency = Vec::with_capacity(clients_per_region.len());

        // compute y: latency values for each number of clients
        let mut y = Vec::with_capacity(clients_per_region.len());
        for &clients in clients_per_region.iter() {
            // start search
            let mut search = db.search();
            let mut exp_data = search
                .n(n)
                .f(f)
                .protocol(protocol)
                .clients_per_region(clients)
                .key_gen(key_gen)
                .payload_size(payload_size)
                .load()?;
            match exp_data.len() {
                0 => {
                    eprintln!("missing data for {} f = {}", PlotFmt::protocol_name(protocol), f);
                    avg_latency.push(0f64);
                    y.push(0f64);
                    continue;
                },
                1 => (),
                _ => panic!("found more than 1 matching experiment for this search criteria"),
            };
            let exp_data = exp_data.pop().unwrap();

            // get average latency
            let avg = exp_data.global_client_latency.mean().value();
            avg_latency.push(avg);

            // compute latency to be plotted
            let latency = match latency {
                Latency::Average => avg,
                Latency::Percentile(percentile) => exp_data
                    .global_client_latency
                    .percentile(percentile)
                    .value(),
            };
            y.push(latency);
        }

        // compute x: compute throughput given average latency and number of
        // clients
        let (x, y): (Vec<_>, Vec<_>) = avg_latency
            .into_iter()
            .zip(y.iter())
            .zip(clients_per_region.iter())
            .filter_map(|((avg_latency, &latency), &clients)| {
                if latency == 0f64 {
                    None
                } else {
                    // compute throughput using the average latency
                    let per_second = 1000f64 / avg_latency;
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
        ax.plot(x, y, None, Some(kwargs))?;
        plotted += 1;
    }

    // set log scale on y axis
    set_log_scale(py, &ax, AxisToScale::Y)?;

    // set labels
    ax.set_xlabel("throughput (K ops/s)")?;
    ax.set_ylabel("latency (ms) [log-scale]")?;

    // legend
    add_legend(plotted, n, py, &ax)?;

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
    let (fig, ax) = plt.subplots(None)?;

    let top = ("top", ADJUST_TOP);
    let bottom = ("bottom", ADJUST_BOTTOM);

    // adjust fig margins
    let kwargs = if let Some(hspace) = height_between_subplots {
        // also set `hspace`
        let hspace = ("hspace", hspace);
        pydict!(py, top, bottom, hspace)
    } else {
        pydict!(py, top, bottom)
    };
    fig.subplots_adjust(Some(kwargs))?;

    Ok((fig, ax))
}

fn end_plot(
    output_file: &str,
    py: Python<'_>,
    plt: &PyPlot<'_>,
    fig: Figure<'_>,
) -> Result<(), Report> {
    // save figure
    let kwargs = pydict!(py, ("format", "pdf"));
    plt.savefig(output_file, Some(kwargs))?;

    // close the figure
    plt.close(fig)?;

    Ok(())
}

fn add_legend(
    plotted: usize,
    n: usize,
    py: Python<'_>,
    ax: &Axes<'_>,
) -> Result<(), Report> {
    // pull legend up
    let y_bbox_to_anchor = match n {
        3 => 1.17,
        5 => 1.24,
        _ => panic!("add_legend: unsupported n: {}", n),
    };

    do_add_legend(plotted, y_bbox_to_anchor, py, ax)
}

fn add_subplot_legend(
    plotted: usize,
    n: usize,
    py: Python<'_>,
    ax: &Axes<'_>,
) -> Result<(), Report> {
    // pull legend up
    let y_bbox_to_anchor = if n != 3 {
        1.37
    } else {
        panic!("add_subplot_legend: unsupported n: {}", n)
    };

    do_add_legend(plotted, y_bbox_to_anchor, py, ax)
}

fn do_add_legend(
    plotted: usize,
    y_bbox_to_anchor: f64,
    py: Python<'_>,
    ax: &Axes<'_>,
) -> Result<(), Report> {
    let legend_ncol = match plotted {
        0 => 0,
        1 => 1,
        2 => 2,
        3 => 3,
        4 => 2,
        5 => 3,
        6 => 3,
        _ => panic!(
            "do_add_legend: unsupported number of plotted instances: {}",
            plotted
        ),
    };
    // add legend
    let kwargs = pydict!(
        py,
        ("loc", "upper center"),
        ("bbox_to_anchor", (0.5, y_bbox_to_anchor)),
        // remove box around legend:
        ("edgecolor", "white"),
        ("ncol", legend_ncol),
    );
    ax.legend(Some(kwargs))?;

    Ok(())
}

fn set_log_scale(
    py: Python<'_>,
    ax: &Axes<'_>,
    axis_to_scale: AxisToScale,
) -> Result<(), Report> {
    // set log scale on axis
    match axis_to_scale {
        AxisToScale::X => ax.set_xscale("log")?,
        AxisToScale::Y => ax.set_yscale("log")?,
    }

    // control which labels get plotted (matplotlib doesn't do a very good job
    // picking labels with a log scale)
    const LABEL_COUNT: usize = 7;

    // compute ticks given the limits
    let (start, end) = match axis_to_scale {
        AxisToScale::X => ax.get_xlim()?,
        AxisToScale::Y => ax.get_ylim()?,
    };

    // compute `shift` when `start` and `end` are in `ln`-values. this ensures
    // that we'll get roughly evenly-spaced ticks (after mapping them back to
    // their original value with `exp`)
    let start_log = start.ln();
    let end_log = end.ln();
    let shift = (end_log - start_log) / (LABEL_COUNT - 1) as f64;

    let mut ticks: Vec<_> = (0..LABEL_COUNT)
        .map(|i| {
            let log = start_log + i as f64 * shift;
            match log.exp().round() as u64 {
                n if n <= 400 => (n as f64 / 10f64).round() as u64 * 10,
                n if n <= 1000 => (n as f64 / 50f64).round() as u64 * 50,
                n if n <= 10000 => (n as f64 / 100f64).round() as u64 * 100,
                n if n <= 100000 => (n as f64 / 1000f64).round() as u64 * 1000,
                n => panic!("set_log_scale: unsupportted axis value: {}", n),
            }
        })
        .collect();

    // set major ticks with the `ticks` computed above; also remove minor ticks
    let major = pydict!(py, ("minor", false));
    let minor = pydict!(py, ("minor", true));

    match axis_to_scale {
        AxisToScale::X => {
            ax.set_xticks(ticks.clone(), Some(major))?;
            ax.set_xticklabels(ticks.clone(), Some(major))?;

            ticks.clear();
            ax.set_xticks(ticks, Some(minor))?;
        }
        AxisToScale::Y => {
            // set major ticks
            ax.set_yticks(ticks.clone(), Some(major))?;
            ax.set_yticklabels(ticks.clone(), Some(major))?;

            // remove minor ticks
            ticks.clear();
            ax.set_yticks(ticks, Some(minor))?;
        }
    }
    Ok(())
}

fn bar_style(
    py: Python<'_>,
    protocol: Protocol,
    f: usize,
    bar_width: f64,
) -> Result<&PyDict, Report> {
    let kwargs = pydict!(
        py,
        ("label", PlotFmt::label(protocol, f)),
        ("width", bar_width),
        ("edgecolor", "black"),
        ("linewidth", 1),
        ("color", PlotFmt::color(protocol, f)),
        ("hatch", PlotFmt::hatch(protocol, f)),
    );
    Ok(kwargs)
}

fn line_style(
    py: Python<'_>,
    protocol: Protocol,
    f: usize,
) -> Result<&PyDict, Report> {
    let kwargs = pydict!(
        py,
        ("label", PlotFmt::label(protocol, f)),
        ("color", PlotFmt::color(protocol, f)),
        ("marker", PlotFmt::marker(protocol, f)),
        ("linestyle", PlotFmt::linestyle(protocol, f)),
        ("linewidth", PlotFmt::linewidth(f)),
    );
    Ok(kwargs)
}
