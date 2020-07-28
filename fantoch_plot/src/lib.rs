#![deny(rust_2018_idioms)]

mod db;
mod fmt;
mod plot;

// Re-exports.
pub use db::{ExperimentData, ResultsDB, Search};
pub use fmt::PlotFmt;

use color_eyre::eyre::WrapErr;
use color_eyre::Report;
use fantoch::id::ProcessId;
use plot::axes::Axes;
use plot::figure::Figure;
use plot::pyplot::PyPlot;
use plot::Matplotlib;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::{BTreeSet, HashMap, HashSet};

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

impl ErrorBar {
    pub fn to_file_suffix(&self) -> String {
        match self {
            Self::Without => String::new(),
            Self::With(percentile) => format!("_p{}", percentile * 100f64),
        }
    }
}

pub enum LatencyMetric {
    Average,
    Percentile(f64),
}

impl LatencyMetric {
    pub fn to_file_suffix(&self) -> String {
        match self {
            Self::Average => String::new(),
            Self::Percentile(percentile) => {
                format!("_p{}", percentile * 100f64)
            }
        }
    }
}

#[derive(PartialEq, Eq, Hash)]
pub enum Style {
    Label,
    Color,
    Hatch,
    Marker,
    LineStyle,
    LineWidth,
}

pub enum DstatType {
    Process(ProcessId),
    ProcessGlobal,
    ClientGlobal,
}

impl DstatType {
    pub fn name(&self) -> String {
        match self {
            Self::Process(process_id) => format!("process_{}", process_id),
            Self::ProcessGlobal => String::from("process_global"),
            Self::ClientGlobal => String::from("client_global"),
        }
    }
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

pub fn latency_plot<R>(
    searches: Vec<Search>,
    style_fun: Option<Box<dyn Fn(&Search) -> HashMap<Style, String>>>,
    n: usize,
    error_bar: ErrorBar,
    output_dir: Option<&str>,
    output_file: &str,
    db: &ResultsDB,
    f: impl Fn(&ExperimentData) -> R,
) -> Result<Vec<(Search, R)>, Report> {
    const FULL_REGION_WIDTH: f64 = 10f64;
    const MAX_COMBINATIONS: usize = 6;
    // 80% of `FULL_REGION_WIDTH` when `MAX_COMBINATIONS` is reached
    const BAR_WIDTH: f64 = FULL_REGION_WIDTH * 0.8 / MAX_COMBINATIONS as f64;

    // let combinations = combinations(n);
    assert!(
        searches.len() <= MAX_COMBINATIONS,
        "latency_plot: expected less searches than the max number of combinations"
    );

    // compute x:
    let x: Vec<_> = (0..n).map(|i| i as f64 * FULL_REGION_WIDTH).collect();

    // we need to shift all to the left by half of the number of combinations
    let shift_left = searches.len() as f64 / 2f64;
    // we also need to shift half bar to the right
    let shift_right = 0.5;
    let searches = searches.into_iter().enumerate().map(|(index, search)| {
        // compute index according to shifts
        let index = index as f64 - shift_left + shift_right;
        // compute combination's shift
        let shift = index * BAR_WIDTH;
        (shift, search)
    });

    // keep track of all regions
    let mut all_regions = HashSet::new();

    // aggregate the output of `f` for each search
    let mut results = Vec::new();

    // start python
    let gil = Python::acquire_gil();
    let py = gil.python();
    let plt = PyPlot::new(py)?;

    // start plot
    let (fig, ax) = start_plot(py, &plt, None)?;

    // keep track of the number of plotted instances
    let mut plotted = 0;

    for (shift, search) in searches {
        // check `n`
        assert_eq!(
            search.n, n,
            "latency_plot: value of n in search doesn't match the provided"
        );
        let mut exp_data = db.find(search)?;
        match exp_data.len() {
            0 => {
                eprintln!("missing data for {} f = {}", PlotFmt::protocol_name(search.protocol), search.f);
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
        let kwargs = bar_style(py, search, &style_fun, BAR_WIDTH)?;
        if let ErrorBar::With(_) = error_bar {
            pytry!(py, kwargs.set_item("yerr", (from_err, to_err)));
        }

        ax.bar(x, y, Some(kwargs))?;
        plotted += 1;

        // save new result
        results.push((search, f(exp_data)));

        // update set of all regions
        for region in regions {
            all_regions.insert(region);
        }
    }

    // set xticks
    ax.set_xticks(x, None)?;

    // only do the following check if we had at least one search matching
    if plotted > 0 {
        // set x labels:
        // - check the number of regions is correct
        assert_eq!(
            all_regions.len(),
            n,
            "latency_plot: the number of regions doesn't match the n provided"
        );
    }

    let mut regions: Vec<_> = all_regions.into_iter().collect();
    regions.sort();
    // map regions to their pretty name
    let labels: Vec<_> =
        regions.into_iter().map(PlotFmt::region_name).collect();
    ax.set_xticklabels(labels, None)?;

    // set labels
    ax.set_ylabel("latency (ms)")?;

    // legend
    add_legend(plotted, None, py, &ax)?;

    // end plot
    end_plot(output_dir, output_file, py, &plt, Some(fig))?;
    Ok(results)
}

// based on: https://github.com/jonhoo/thesis/blob/master/graphs/vote-memlimit-cdf.py
pub fn cdf_plot(
    searches: Vec<Search>,
    style_fun: Option<Box<dyn Fn(&Search) -> HashMap<Style, String>>>,
    output_dir: Option<&str>,
    output_file: &str,
    db: &ResultsDB,
) -> Result<(), Report> {
    // start python
    let gil = Python::acquire_gil();
    let py = gil.python();
    let plt = PyPlot::new(py)?;

    // start plot
    let (fig, ax) = start_plot(py, &plt, None)?;

    // keep track of the number of plotted instances
    let mut plotted = 0;

    for search in searches {
        inner_cdf_plot(py, &ax, search, &style_fun, &mut plotted, db)?;
    }

    // set cdf plot style
    inner_cdf_plot_style(py, &ax)?;

    // legend
    add_legend(plotted, None, py, &ax)?;

    // end plot
    end_plot(output_dir, output_file, py, &plt, Some(fig))?;

    Ok(())
}

pub fn cdf_plot_per_f(
    searches: Vec<Search>,
    style_fun: Option<Box<dyn Fn(&Search) -> HashMap<Style, String>>>,
    output_dir: Option<&str>,
    output_file: &str,
    db: &ResultsDB,
) -> Result<(), Report> {
    let fs: BTreeSet<_> = searches.iter().map(|search| search.f).collect();
    let fs: Vec<_> = fs.into_iter().collect();
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

        // plot all searches that match this `f`
        for search in searches.iter().filter(|search| search.f == f) {
            inner_cdf_plot(py, &ax, *search, &style_fun, &mut plotted, db)?;
        }

        // set cdf plot style
        inner_cdf_plot_style(py, &ax)?;

        // additional style: maybe hide x-axis labels
        if hide_xticklabels {
            ax.xaxis.set_visible(false)?;
        }

        // specific pull-up for this kind of plot
        let y_bbox_to_anchor = Some(1.41);
        // legend
        add_legend(plotted, y_bbox_to_anchor, py, &ax)?;

        // save axis
        previous_axis = Some(ax);
    }

    // end plot
    end_plot(output_dir, output_file, py, &plt, Some(fig))?;

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
    search: Search,
    style_fun: &Option<Box<dyn Fn(&Search) -> HashMap<Style, String>>>,
    plotted: &mut usize,
    db: &ResultsDB,
) -> Result<(), Report> {
    let mut exp_data = db.find(search)?;
    match exp_data.len() {
        0 => {
            eprintln!(
                "missing data for {} f = {}",
                PlotFmt::protocol_name(search.protocol),
                search.f
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
    let kwargs = line_style(py, search, style_fun)?;
    ax.plot(x, y, None, Some(kwargs))?;
    *plotted += 1;

    Ok(())
}

pub fn throughput_latency_plot(
    searches: Vec<Search>,
    style_fun: Option<Box<dyn Fn(&Search) -> HashMap<Style, String>>>,
    n: usize,
    clients_per_region: Vec<usize>,
    latency: LatencyMetric,
    output_dir: Option<&str>,
    output_file: &str,
    db: &ResultsDB,
) -> Result<(), Report> {
    // start python
    let gil = Python::acquire_gil();
    let py = gil.python();
    let plt = PyPlot::new(py)?;

    // start plot
    let (fig, ax) = start_plot(py, &plt, None)?;

    // keep track of the number of plotted instances
    let mut plotted = 0;

    for mut search in searches {
        // check `n`
        assert_eq!(search.n, n, "throughput_latency_plot: value of n in search doesn't match the provided");

        // keep track of average latency that will be used to compute throughput
        let mut avg_latency = Vec::with_capacity(clients_per_region.len());

        // compute y: latency values for each number of clients
        let mut y = Vec::with_capacity(clients_per_region.len());
        for &clients in clients_per_region.iter() {
            // refine search
            search.clients_per_region(clients);

            // execute search
            let mut exp_data = db.find(search)?;
            match exp_data.len() {
                0 => {
                    eprintln!("missing data for {} f = {}", PlotFmt::protocol_name(search.protocol), search.f);
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
                LatencyMetric::Average => avg,
                LatencyMetric::Percentile(percentile) => exp_data
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
        let kwargs = line_style(py, search, &style_fun)?;
        ax.plot(x, y, None, Some(kwargs))?;
        plotted += 1;
    }

    // set log scale on y axis
    set_log_scale(py, &ax, AxisToScale::Y)?;

    // set labels
    ax.set_xlabel("throughput (K ops/s)")?;
    ax.set_ylabel("latency (ms) [log-scale]")?;

    // legend
    add_legend(plotted, None, py, &ax)?;

    // end plot
    end_plot(output_dir, output_file, py, &plt, Some(fig))?;

    Ok(())
}

pub fn dstat_table(
    searches: Vec<Search>,
    dstat_type: DstatType,
    output_dir: Option<&str>,
    output_file: &str,
    db: &ResultsDB,
) -> Result<(), Report> {
    let col_labels = vec![
        "cpu_usr",
        "cpu_sys",
        "cpu_wait",
        "net_recv (MB/s)",
        "net_send (MB/s)",
        "mem_used (MB)",
    ];
    let col_widths = vec![0.13, 0.13, 0.13, 0.20, 0.20, 0.20];

    // protocol labels
    let mut row_labels = Vec::with_capacity(searches.len());

    // actual data
    let mut cells = Vec::with_capacity(searches.len());

    let mut has_data = false;
    for search in searches {
        let mut exp_data = db.find(search)?;
        match exp_data.len() {
            0 => {
                eprintln!("missing data for {} f = {}", PlotFmt::protocol_name(search.protocol), search.f);
                continue;
            },
            1 => (),
            _ => panic!("found more than 1 matching experiment for this search criteria"),
        };
        let exp_data = exp_data.pop().unwrap();

        // create row label
        let row_label = format!(
            "{} f = {}",
            PlotFmt::protocol_name(search.protocol),
            search.f
        );
        row_labels.push(row_label);

        // select the correct dstats depending on the `DstatType` chosen
        let dstats = match dstat_type {
            DstatType::Process(process_id) => {
                match exp_data.process_dstats.get(&process_id) {
                    Some(dstats) => dstats,
                    None => {
                        panic!("didn't found dstat for process {}", process_id)
                    }
                }
            }
            DstatType::ProcessGlobal => &exp_data.global_process_dstats,
            DstatType::ClientGlobal => &exp_data.global_client_dstats,
        };
        // fetch all cell data
        let cpu_usr = dstats.cpu_usr_mad();
        let cpu_sys = dstats.cpu_sys_mad();
        let cpu_wait = dstats.cpu_wait_mad();
        let net_recv = dstats.net_recv_mad();
        let net_send = dstats.net_send_mad();
        let mem_used = dstats.mem_used_mad();
        // create cell
        let cell =
            vec![cpu_usr, cpu_sys, cpu_wait, net_recv, net_send, mem_used];
        // format cell
        let fmt_cell_data = |mad: (_, _)| format!("{} ± {}", mad.0, mad.1);
        let cell: Vec<_> = cell.into_iter().map(fmt_cell_data).collect();

        // save cell
        cells.push(cell);

        // mark that there's data to be plotted
        has_data = true
    }

    // only try to plot if there's any data
    if has_data {
        // start python
        let gil = Python::acquire_gil();
        let py = gil.python();
        let plt = PyPlot::new(py)?;

        // create table arguments
        let kwargs = pydict!(
            py,
            ("colLabels", col_labels),
            ("colWidths", col_widths),
            ("rowLabels", row_labels),
            ("cellText", cells),
            ("colLoc", "center"),
            ("cellLoc", "center"),
            ("rowLoc", "right"),
            ("loc", "center"),
        );

        let table = plt.table(Some(kwargs))?;
        plt.axis("off")?;

        // create font size font
        table.auto_set_font_size(false)?;
        table.set_fontsize(6.5)?;

        // row labels are too wide without this
        plt.tight_layout()?;

        // end plot
        end_plot(output_dir, output_file, py, &plt, None)?;
    }
    Ok(())
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
    output_dir: Option<&str>,
    output_file: &str,
    py: Python<'_>,
    plt: &PyPlot<'_>,
    fig: Option<Figure<'_>>,
) -> Result<(), Report> {
    // maybe save `output_file` in `output_dir` (if one was set)
    let output_file = if let Some(output_dir) = output_dir {
        // make sure `output_dir` exists
        std::fs::create_dir_all(&output_dir).wrap_err("create plot dir")?;
        format!("{}/{}", output_dir, output_file)
    } else {
        output_file.to_string()
    };

    // save figure
    let kwargs = pydict!(py, ("format", "pdf"));
    plt.savefig(&output_file, Some(kwargs))?;

    let kwargs = if let Some(fig) = fig {
        // close the figure passed as argument
        Some(pydict!(py, ("fig", fig.fig())))
    } else {
        // close the current figure
        None
    };
    plt.close(kwargs)?;
    Ok(())
}

fn add_legend(
    plotted: usize,
    y_bbox_to_anchor: Option<f64>,
    py: Python<'_>,
    ax: &Axes<'_>,
) -> Result<(), Report> {
    // default values for `y_bbox_to_anchor`
    let one_row = 1.17;
    let two_rows = 1.255;

    let (legend_ncol, y_bbox_to_anchor_default) = match plotted {
        0 => (0, 0.0),
        1 => (1, one_row),
        2 => (2, one_row),
        3 => (3, one_row),
        4 => (2, two_rows),
        5 => (3, two_rows),
        6 => (3, two_rows),
        _ => panic!(
            "add_legend: unsupported number of plotted instances: {}",
            plotted
        ),
    };

    // use the default value if not set
    let y_bbox_to_anchor = y_bbox_to_anchor.unwrap_or(y_bbox_to_anchor_default);

    // add legend
    let kwargs = pydict!(
        py,
        ("loc", "upper center"),
        // pull legend up
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

fn bar_style<'a>(
    py: Python<'a>,
    search: Search,
    style_fun: &Option<Box<dyn Fn(&Search) -> HashMap<Style, String>>>,
    bar_width: f64,
) -> Result<&'a PyDict, Report> {
    let protocol = search.protocol;
    let f = search.f;

    // compute styles
    let mut styles = style_fun
        .as_ref()
        .map(|style_fun| style_fun(&search))
        .unwrap_or_default();

    // compute label, color and hatch
    let label = styles
        .remove(&Style::Label)
        .unwrap_or_else(|| PlotFmt::label(protocol, f));
    let color = styles
        .remove(&Style::Color)
        .unwrap_or_else(|| PlotFmt::color(protocol, f));
    let hatch = styles
        .remove(&Style::Hatch)
        .unwrap_or_else(|| PlotFmt::hatch(protocol, f));

    let kwargs = pydict!(
        py,
        ("label", label),
        ("width", bar_width),
        ("edgecolor", "black"),
        ("linewidth", 1),
        ("color", color),
        ("hatch", hatch),
    );
    Ok(kwargs)
}

fn line_style<'a>(
    py: Python<'a>,
    search: Search,
    style_fun: &Option<Box<dyn Fn(&Search) -> HashMap<Style, String>>>,
) -> Result<&'a PyDict, Report> {
    let protocol = search.protocol;
    let f = search.f;

    // compute styles
    let mut styles = style_fun
        .as_ref()
        .map(|style_fun| style_fun(&search))
        .unwrap_or_default();

    // compute label, color, marker, linestyle and linewidth
    let label = styles
        .remove(&Style::Label)
        .unwrap_or_else(|| PlotFmt::label(protocol, f));
    let color = styles
        .remove(&Style::Color)
        .unwrap_or_else(|| PlotFmt::color(protocol, f));
    let marker = styles
        .remove(&Style::Marker)
        .unwrap_or_else(|| PlotFmt::marker(protocol, f));
    let linestyle = styles
        .remove(&Style::LineStyle)
        .unwrap_or_else(|| PlotFmt::linestyle(protocol, f));
    let linewidth = styles
        .remove(&Style::LineWidth)
        .unwrap_or_else(|| PlotFmt::linewidth(f));

    let kwargs = pydict!(
        py,
        ("label", label),
        ("color", color),
        ("marker", marker),
        ("linestyle", linestyle),
        ("linewidth", linewidth),
    );
    Ok(kwargs)
}
