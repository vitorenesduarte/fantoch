#![deny(rust_2018_idioms)]

mod db;
mod fmt;
pub mod plot;

// Re-exports.
pub use db::{ExperimentData, LatencyPrecision, ResultsDB, Search};
pub use fmt::PlotFmt;

use color_eyre::eyre::WrapErr;
use color_eyre::Report;
use fantoch::client::KeyGen;
use fantoch::executor::ExecutorMetricsKind;
use fantoch::id::ProcessId;
use fantoch::protocol::ProtocolMetricsKind;
use fantoch_exp::Protocol;
use plot::axes::Axes;
use plot::figure::Figure;
use plot::pyplot::PyPlot;
use plot::Matplotlib;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

// defaults: [6.4, 4.8]
// copied from: https://github.com/jonhoo/thesis/blob/master/graphs/common.py
const GOLDEN_RATIO: f64 = 1.61803f64;
const FIGWIDTH: f64 = 8.5 / GOLDEN_RATIO;
// no longer golden ratio
const FIGSIZE: (f64, f64) = (FIGWIDTH, (FIGWIDTH / GOLDEN_RATIO) - 0.6);

// adjust are percentages:
// - setting top to 0.80, means we leave the top 20% free
// - setting bottom to 0.20, means we leave the bottom 20% free
const ADJUST_TOP: f64 = 0.83;
const ADJUST_BOTTOM: f64 = 0.15;

pub enum ErrorBar {
    With(f64),
    Without,
}

impl ErrorBar {
    pub fn name(&self) -> String {
        match self {
            Self::Without => String::new(),
            Self::With(percentile) => format!("_p{}", percentile * 100f64),
        }
    }
}

#[derive(Clone, Copy)]
pub enum LatencyMetric {
    Average,
    Percentile(f64),
}

impl LatencyMetric {
    pub fn name(&self) -> String {
        match self {
            Self::Average => String::new(),
            Self::Percentile(percentile) => {
                format!("_p{}", percentile * 100f64)
            }
        }
    }
}

#[derive(Clone, Copy)]
pub enum HeatmapMetric {
    CPU,
    NetRecv,
    NetSend,
}

impl HeatmapMetric {
    pub fn name(&self) -> String {
        match self {
            Self::CPU => String::from("cpu"),
            Self::NetRecv => String::from("net_in"),
            Self::NetSend => String::from("net_out"),
        }
    }

    pub fn utilization(&self, value: f64) -> usize {
        let max = match self {
            Self::CPU => 100f64,
            Self::NetSend | Self::NetRecv => {
                // 10GBit to B
                10_000_000_000f64 / 8f64
            }
        };
        (value * 100f64 / max) as usize
    }
}

#[derive(Clone, Copy)]
pub enum ThroughputYAxis {
    Latency(LatencyMetric),
    CPU,
}

impl ThroughputYAxis {
    pub fn name(&self) -> String {
        match self {
            Self::Latency(latency) => format!("latency{}", latency.name()),
            Self::CPU => String::from("cpu"),
        }
    }

    fn y_label(&self, latency_precision: LatencyPrecision) -> String {
        match self {
            Self::Latency(_) => {
                format!("latency ({})", latency_precision.name())
            }
            Self::CPU => String::from("CPU utilization (%)"),
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

pub enum MetricsType {
    Process(ProcessId),
    ProcessGlobal,
    ClientGlobal,
}

impl MetricsType {
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
    // need to load `PyPlot` for the following to work
    let _ = PyPlot::new(py)?;

    // adjust fig size
    let kwargs = pydict!(py, ("figsize", FIGSIZE));
    lib.rc("figure", Some(kwargs))?;

    // adjust font size
    let kwargs = pydict!(py, ("size", 8.5));
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
    legend_order: Option<Vec<usize>>,
    style_fun: Option<Box<dyn Fn(&Search) -> HashMap<Style, String>>>,
    latency_precision: LatencyPrecision,
    n: usize,
    error_bar: ErrorBar,
    output_dir: Option<&str>,
    output_file: &str,
    db: &ResultsDB,
    f: impl Fn(&ExperimentData) -> R,
) -> Result<Vec<(Search, R)>, Report> {
    const FULL_REGION_WIDTH: f64 = 10f64;
    const MAX_COMBINATIONS: usize = 7;
    // 80% of `FULL_REGION_WIDTH` when `MAX_COMBINATIONS` is reached
    const BAR_WIDTH: f64 = FULL_REGION_WIDTH * 0.8 / MAX_COMBINATIONS as f64;

    assert!(
        searches.len() <= MAX_COMBINATIONS,
        "latency_plot: expected less searches than the max number of combinations"
    );

    // compute x: one per region
    // - the +1 is for the 'average' group
    let x: Vec<_> = (0..n + 1).map(|i| i as f64 * FULL_REGION_WIDTH).collect();

    // we need to shift all to the left by half of the number of combinations
    let search_count = searches.len();
    let shift_left = search_count as f64 / 2f64;
    // we also need to shift half bar to the right
    let shift_right = 0.5;
    let searches = searches.into_iter().enumerate().map(|(index, search)| {
        // compute index according to shifts
        let mut base = index as f64 - shift_left + shift_right;

        // HACK to separate move `f = 1` (i.e. the first 3 searches) a bit to
        // the left and `f = 2` (i.e. the remaining 4 searches) a bit to the
        // right
        if search_count == 7 {
            if search.f == 1 && index < 3 {
                base += 0.25;
            }
            if search.f == 2 && index >= 3 {
                base += 0.75;
            }
        }
        // compute combination's shift
        let shift = base * BAR_WIDTH;
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

    // compute legend order: if not defined, then it's the order given by
    // `searches`
    let legend_order =
        legend_order.unwrap_or_else(|| (0..searches.len()).collect::<Vec<_>>());
    assert_eq!(
        legend_order.len(),
        searches.len(),
        "legend order should contain the same number of searches"
    );
    let mut legends = BTreeMap::new();

    for ((shift, search), legend_order) in
        searches.into_iter().zip(legend_order)
    {
        // check `n`
        assert_eq!(
            search.n, n,
            "latency_plot: value of n in search doesn't match the provided"
        );
        let mut exp_data = db.find(search)?;
        match exp_data.len() {
            0 => {
                eprintln!(
                    "missing data for {} f = {}",
                    PlotFmt::protocol_name(search.protocol),
                    search.f
                );
                continue;
            }
            1 => (),
            _ => {
                let matches: Vec<_> = exp_data
                    .into_iter()
                    .map(|(timestamp, _, _)| {
                        timestamp.path().display().to_string()
                    })
                    .collect();
                panic!("found more than 1 matching experiment for this search criteria: search {:?} | matches {:?}", search, matches);
            }
        };
        let (_, _, exp_data) = exp_data.pop().unwrap();

        // compute y: avg latencies sorted by region name
        let mut from_err = Vec::new();
        let mut to_err = Vec::new();
        let mut per_region_latency: Vec<_> = exp_data
            .client_latency
            .iter()
            .map(|(region, histogram)| {
                // compute average latency
                let avg = histogram.mean(latency_precision).round() as u64;

                // maybe create error bar
                let error_bar = if let ErrorBar::With(percentile) = error_bar {
                    let percentile = histogram
                        .percentile(percentile, latency_precision)
                        .round();
                    percentile as u64 - avg
                } else {
                    0
                };
                // this represents the top of the bar
                from_err.push(0);
                // this represents the height of the error bar starting at the
                // top of the bar
                to_err.push(error_bar);

                (region.clone(), avg)
            })
            .collect();

        // sort by region and get region latencies
        per_region_latency.sort();
        let (regions, mut y): (HashSet<_>, Vec<_>) =
            per_region_latency.into_iter().unzip();

        // compute the stddev between region latencies
        // let hist = fantoch::metrics::Histogram::from(y.clone());
        // let stddev = hist.stddev().value().round() as u64;
        let stddev = 0;
        // add stddev as an error bar
        from_err.push(0);
        to_err.push(stddev);

        // add global client latency to the 'average' group
        y.push(
            exp_data
                .global_client_latency
                .mean(latency_precision)
                .round() as u64,
        );
        println!(
            "{:<7} f = {} | {:?} | stddev = {}",
            PlotFmt::protocol_name(search.protocol),
            search.f,
            y,
            stddev,
        );

        // compute x: shift all values by `shift`
        let x: Vec<_> = x.iter().map(|&x| x + shift).collect();

        // plot it error bars:
        // - even if the error bar is not set, we have the stddev error bar from
        //   the 'average' group
        let kwargs = bar_style(py, search, &style_fun, BAR_WIDTH)?;
        pytry!(py, kwargs.set_item("yerr", (from_err, to_err)));

        let line = ax.bar(x, y, Some(kwargs))?;
        plotted += 1;

        // save line with its legend order
        legends.insert(
            legend_order,
            (line, PlotFmt::label(search.protocol, search.f)),
        );

        // save new result
        results.push((search, f(exp_data)));

        // update set of all regions
        all_regions.extend(regions);
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
    let mut labels: Vec<_> =
        regions.into_iter().map(PlotFmt::region_name).collect();
    labels.push("average");
    ax.set_xticklabels(labels, None)?;

    // set labels
    let ylabel = format!("latency ({})", latency_precision.name());
    ax.set_ylabel(&ylabel, None)?;

    // legend
    // HACK:
    let legend_column_spacing =
        if search_count == 7 { Some(1.25) } else { None };
    let x_bbox_to_anchor = Some(0.48);
    add_legend(
        plotted,
        Some(legends),
        x_bbox_to_anchor,
        None,
        legend_column_spacing,
        py,
        &ax,
    )?;

    // end plot
    end_plot(plotted > 0, output_dir, output_file, py, &plt, Some(fig))?;
    Ok(results)
}

// based on: https://github.com/jonhoo/thesis/blob/master/graphs/vote-memlimit-cdf.py
pub fn cdf_plot(
    searches: Vec<Search>,
    style_fun: Option<Box<dyn Fn(&Search) -> HashMap<Style, String>>>,
    latency_precision: LatencyPrecision,
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
        inner_cdf_plot(
            py,
            &ax,
            search,
            &style_fun,
            latency_precision,
            db,
            &mut plotted,
        )?;
    }

    // set cdf plot style
    inner_cdf_plot_style(py, &ax, None, latency_precision)?;

    // legend
    add_legend(plotted, None, None, None, None, py, &ax)?;

    // end plot
    end_plot(plotted > 0, output_dir, output_file, py, &plt, Some(fig))?;

    Ok(())
}

pub fn cdf_plot_split(
    top_searches: Vec<Search>,
    bottom_searches: Vec<Search>,
    x_range: Option<(f64, f64)>,
    style_fun: Option<Box<dyn Fn(&Search) -> HashMap<Style, String>>>,
    latency_precision: LatencyPrecision,
    output_dir: Option<&str>,
    output_file: &str,
    db: &ResultsDB,
) -> Result<(), Report> {
    // start python
    let gil = Python::acquire_gil();
    let py = gil.python();
    let plt = PyPlot::new(py)?;

    // start plot:
    // - adjust vertical space between the two plots
    let kwargs = pydict!(py, ("hspace", 0.2));
    let (fig, _) = start_plot(py, &plt, Some(kwargs))?;

    let mut previous_axis: Option<Axes<'_>> = None;
    let mut plotted = 0;

    for subplot in vec![2, 1] {
        // create subplot (shared axis with the previous subplot (if any))
        let kwargs = match previous_axis {
            None => None,
            Some(previous_axis) => {
                // use the bottom axis for both so that the plots have the same
                // scale
                Some(pydict!(py, ("sharex", previous_axis.ax())))
            }
        };
        let ax = plt.subplot(2, 1, subplot, kwargs)?;

        // keep track of the number of plotted instances
        let mut subfigure_plotted = 0;

        // compute which searches to use
        let searches = match subplot {
            1 => &top_searches,
            2 => &bottom_searches,
            _ => unreachable!("impossible subplot"),
        };
        for search in searches {
            inner_cdf_plot(
                py,
                &ax,
                *search,
                &style_fun,
                latency_precision,
                db,
                &mut subfigure_plotted,
            )?;
        }

        // set cdf plot style
        inner_cdf_plot_style(py, &ax, x_range, latency_precision)?;

        // additional style for subplot 1:
        // - hide x-axis
        // - legend
        match subplot {
            1 => {
                // hide x-axis
                ax.xaxis.set_visible(false)?;
                // specific pull-up for this kind of plot
                let y_bbox_to_anchor = Some(1.66);
                // legend
                add_legend(
                    subfigure_plotted,
                    None,
                    None,
                    y_bbox_to_anchor,
                    None,
                    py,
                    &ax,
                )?;
            }
            2 => {
                // nothing to do
            }
            _ => unreachable!("impossible subplot"),
        };

        // save axis
        previous_axis = Some(ax);

        // track global number of plotted
        plotted += subfigure_plotted;
    }

    // end plot
    end_plot(plotted > 0, output_dir, output_file, py, &plt, Some(fig))?;

    Ok(())
}

fn inner_cdf_plot_style(
    py: Python<'_>,
    ax: &Axes<'_>,
    x_range: Option<(f64, f64)>,
    latency_precision: LatencyPrecision,
) -> Result<(), Report> {
    // maybe set x limits
    if let Some((x_min, x_max)) = x_range {
        let kwargs = pydict!(py, ("xmin", x_min), ("xmax", x_max));
        ax.set_xlim(Some(kwargs))?;
    }

    // set y limits
    let kwargs = pydict!(py, ("ymin", 95.0), ("ymax", 99.99));
    ax.set_ylim(Some(kwargs))?;

    let yticks = vec![95.0, 97.0, 99.0, 99.99];
    ax.set_yticks(yticks.clone(), None)?;
    ax.set_yticklabels(yticks.clone(), None)?;

    // set log scale on x and y axis
    set_log_scale(py, ax, AxisToScale::X)?;

    // set labels
    let xlabel = format!("latency ({}) [log-scale]", latency_precision.name());
    ax.set_xlabel(&xlabel, None)?;
    ax.set_ylabel("percentiles", None)?;

    Ok(())
}

fn inner_cdf_plot(
    py: Python<'_>,
    ax: &Axes<'_>,
    search: Search,
    style_fun: &Option<Box<dyn Fn(&Search) -> HashMap<Style, String>>>,
    latency_precision: LatencyPrecision,
    db: &ResultsDB,
    plotted: &mut usize,
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
        _ => {
            let matches: Vec<_> = exp_data
                .into_iter()
                .map(|(timestamp, _, _)| timestamp.path().display().to_string())
                .collect();
            panic!("found more than 1 matching experiment for this search criteria: search {:?} | matches {:?}", search, matches);
        }
    };
    let (_, _, exp_data) = exp_data.pop().unwrap();

    // compute x: all values in the global histogram
    let x: Vec<_> = percentiles()
        .map(|percentile| {
            exp_data
                .global_client_latency
                .percentile(percentile, latency_precision)
                .round() as u64
        })
        .collect();

    // compute y: percentiles!
    let y: Vec<_> =
        percentiles().map(|percentile| percentile * 100.0).collect();

    println!(
        "{:<7} f = {} | c = {} | {:?}",
        PlotFmt::protocol_name(search.protocol),
        search.f,
        search
            .clients_per_region
            .expect("clients per region should be set"),
        x.iter()
            .zip(y.iter())
            .filter(|(_, percentile)| vec![95.0, 98.8, 99.9, 99.99]
                .contains(percentile))
            .collect::<Vec<_>>()
    );

    // plot it!
    let kwargs = line_style(py, search, style_fun)?;
    ax.plot(x, y, None, Some(kwargs))?;
    *plotted += 1;

    Ok(())
}

pub fn throughput_something_plot(
    searches: Vec<Search>,
    style_fun: Option<Box<dyn Fn(&Search) -> HashMap<Style, String>>>,
    latency_precision: LatencyPrecision,
    n: usize,
    clients_per_region: Vec<usize>,
    x_range: Option<(f64, f64)>,
    y_range: Option<(f64, f64)>,
    y_axis: ThroughputYAxis,
    output_dir: Option<&str>,
    output_file: &str,
    db: &ResultsDB,
) -> Result<Vec<(Search, usize)>, Report> {
    // start python
    let gil = Python::acquire_gil();
    let py = gil.python();
    let plt = PyPlot::new(py)?;

    // start plot
    let (fig, ax) = start_plot(py, &plt, None)?;

    // keep track of the number of plotted instances
    let mut plotted = 0;

    let max_throughputs = inner_throughput_something_plot(
        py,
        &ax,
        searches,
        &style_fun,
        latency_precision,
        n,
        clients_per_region,
        x_range,
        y_range,
        y_axis,
        db,
        &mut plotted,
    )?;

    // set log scale on y axis if y axis is latency
    let log_scale = match y_axis {
        ThroughputYAxis::Latency(_) => {
            set_log_scale(py, &ax, AxisToScale::Y)?;
            true
        }
        ThroughputYAxis::CPU => false,
    };

    // set labels
    ax.set_xlabel("throughput (K ops/s)", None)?;
    let mut y_label = y_axis.y_label(latency_precision);
    if log_scale {
        y_label = format!("{} [log-scale]", y_label);
    }
    ax.set_ylabel(&y_label, None)?;

    // legend
    add_legend(plotted, None, None, None, None, py, &ax)?;

    // end plot
    end_plot(plotted > 0, output_dir, output_file, py, &plt, Some(fig))?;

    Ok(max_throughputs)
}

pub fn inner_throughput_something_plot(
    py: Python<'_>,
    ax: &Axes<'_>,
    searches: Vec<Search>,
    style_fun: &Option<Box<dyn Fn(&Search) -> HashMap<Style, String>>>,
    latency_precision: LatencyPrecision,
    n: usize,
    clients_per_region: Vec<usize>,
    x_range: Option<(f64, f64)>,
    y_range: Option<(f64, f64)>,
    y_axis: ThroughputYAxis,
    db: &ResultsDB,
    plotted: &mut usize,
) -> Result<Vec<(Search, usize)>, Report> {
    let mut max_throughputs = Vec::with_capacity(searches.len());
    for mut search in searches {
        // check `n`
        assert_eq!(search.n, n, "inner_throughput_something_plot: value of n in search doesn't match the provided");

        // x and y values for each number of clients
        let mut values = Vec::with_capacity(clients_per_region.len());
        for &clients in clients_per_region.iter() {
            // refine search
            search.clients_per_region(clients);

            // execute search
            let mut exp_data = db.find(search)?;
            match exp_data.len() {
                0 => {
                    eprintln!(
                        "missing data for {} f = {}",
                        PlotFmt::protocol_name(search.protocol),
                        search.f
                    );
                    values.push((0f64, 0f64));
                    continue;
                }
                1 => (),
                _ => {
                    let matches: Vec<_> = exp_data
                        .into_iter()
                        .map(|(timestamp, _, _)| {
                            timestamp.path().display().to_string()
                        })
                        .collect();
                    panic!("found more than 1 matching experiment for this search criteria: search {:?} | matches {:?}", search, matches);
                }
            };
            let (_, _, exp_data) = exp_data.pop().unwrap();

            // compute x value (throughput)
            let x_value = exp_data.global_client_throughput;

            // compute y value (latency)
            let y_value = match y_axis {
                ThroughputYAxis::Latency(latency) => match latency {
                    LatencyMetric::Average => exp_data
                        .global_client_latency
                        .mean(latency_precision)
                        .round(),
                    LatencyMetric::Percentile(percentile) => exp_data
                        .global_client_latency
                        .percentile(percentile, latency_precision)
                        .round(),
                },
                ThroughputYAxis::CPU => {
                    let dstats = &exp_data.global_process_dstats;
                    let (cpu_usr, _) = dstats.cpu_usr_mad();
                    let (cpu_sys, _) = dstats.cpu_sys_mad();
                    let cpu = std::cmp::min(100, cpu_usr + cpu_sys);
                    cpu as f64
                }
            };

            values.push((x_value, y_value));
        }

        // compute x: compute throughput given average latency and number of
        // clients
        let mut max_throughput = 0;
        let (x, y): (Vec<_>, Vec<_>) = values
            .into_iter()
            .filter_map(|(throughput, avg_latency)| {
                if throughput == 0f64 {
                    assert_eq!(avg_latency, 0f64);
                    None
                } else {
                    // compute K ops
                    let x = throughput / 1000f64;

                    // update max throughput
                    max_throughput = std::cmp::max(max_throughput, x as usize);

                    // round y
                    let y = avg_latency.round() as usize;
                    Some((x, y))
                }
            })
            .unzip();

        println!(
            "{:<7} f = {}\n  throughput: {:?} | max = {}\n  latency:    {:?}",
            PlotFmt::protocol_name(search.protocol),
            search.f,
            x.iter().map(|v| v.round() as usize).collect::<Vec<_>>(),
            max_throughput,
            y
        );

        // save max throughput
        max_throughputs.push((search, max_throughput));

        // plot it! (if there's something to be plotted)
        if !x.is_empty() {
            let kwargs = line_style(py, search, &style_fun)?;
            ax.plot(x, y, None, Some(kwargs))?;
            *plotted += 1;
        }
    }

    // maybe set x limits
    if let Some((x_min, x_max)) = x_range {
        let kwargs = pydict!(py, ("xmin", x_min), ("xmax", x_max));
        ax.set_xlim(Some(kwargs))?;
    }

    // maybe set y limits
    if let Some((y_min, y_max)) = y_range {
        let kwargs = pydict!(py, ("ymin", y_min), ("ymax", y_max));
        ax.set_ylim(Some(kwargs))?;
    }

    Ok(max_throughputs)
}

pub fn heatmap_plot<F>(
    n: usize,
    protocols: Vec<(Protocol, usize)>,
    clients_per_region: Vec<usize>,
    key_gen: KeyGen,
    search_refine: F,
    leader: ProcessId,
    heatmap_metric: HeatmapMetric,
    output_dir: Option<&str>,
    output_file: &str,
    db: &ResultsDB,
) -> Result<(), Report>
where
    F: Fn(&mut Search, KeyGen),
{
    // start python
    let gil = Python::acquire_gil();
    let py = gil.python();
    let plt = PyPlot::new(py)?;

    // start plot
    let (fig, ax) = start_plot(py, &plt, None)?;

    let set_xlabels = true;
    let set_ylabels = true;
    let set_colorbar = false;
    inner_heatmap_plot(
        py,
        &fig,
        &ax,
        n,
        protocols,
        clients_per_region,
        key_gen,
        search_refine,
        leader,
        heatmap_metric,
        set_xlabels,
        set_ylabels,
        set_colorbar,
        db,
    )?;

    // end plot
    end_plot(true, output_dir, output_file, py, &plt, Some(fig))?;

    Ok(())
}

pub fn intra_machine_scalability_plot(
    searches: Vec<Search>,
    n: usize,
    cpus: Vec<usize>,
    db: &ResultsDB,
) -> Result<(), Report> {
    for mut search in searches {
        let mut ys = Vec::with_capacity(cpus.len());
        for cpus in cpus.iter() {
            // check `n`
            assert_eq!(search.n, n, "intra_machine_scalability_plot: value of n in search doesn't match the provided");

            // refine search
            search.cpus(*cpus);

            // execute search
            let exp_data = db.find(search)?;
            let max_throughput = exp_data
                .into_iter()
                .map(|(_, _, exp_data)| {
                    // compute throughput for each result matching this search
                    // (we can have several, with different number of clients)
                    let throughput = exp_data.global_client_throughput;
                    // compute K ops
                    (throughput / 1000f64) as u64
                })
                .max();

            if let Some(max_throughput) = max_throughput {
                ys.push(max_throughput)
            }
        }

        println!(
            "{}: {:?}",
            search.key_gen.expect("key gen should be set"),
            ys
        );
    }

    Ok(())
}

pub fn inter_machine_scalability_plot(
    searches: Vec<Search>,
    style_fun: Option<Box<dyn Fn(&Search) -> HashMap<Style, String>>>,
    n: usize,
    settings: Vec<(usize, usize, f64)>,
    y_range: Option<(f64, f64)>,
    output_dir: Option<&str>,
    output_file: &str,
    db: &ResultsDB,
) -> Result<(), Report> {
    const FULL_PER_GROUP_WIDTH: f64 = 10f64;
    const MAX_COMBINATIONS: usize = 4;
    // 80% of `FULL_PER_GROUP_WIDTH` when `MAX_COMBINATIONS` is reached
    const BAR_WIDTH: f64 = FULL_PER_GROUP_WIDTH * 0.8 / MAX_COMBINATIONS as f64;

    assert!(
        searches.len() == MAX_COMBINATIONS,
        "inter_machine_scalability_plot: expected same number of searches as the max number of combinations"
    );

    // compute x: one per setting
    let x: Vec<_> = (0..settings.len())
        .map(|i| i as f64 * FULL_PER_GROUP_WIDTH)
        .collect();

    // we need to shift all to the left by half of the number of searches
    let search_count = searches.len();
    let shift_left = search_count as f64 / 2f64;
    // we also need to shift half bar to the right
    let shift_right = 0.5;
    let searches = searches.into_iter().enumerate().map(|(index, search)| {
        // compute index according to shifts
        let base = index as f64 - shift_left + shift_right;
        // compute combination's shift
        let shift = base * BAR_WIDTH;
        (shift, search)
    });

    // start python
    let gil = Python::acquire_gil();
    let py = gil.python();
    let plt = PyPlot::new(py)?;

    // start plot
    let (fig, ax) = start_plot(py, &plt, None)?;

    // keep track of the number of plotted instances
    let mut plotted = 0;

    for (shift, mut search) in searches.clone() {
        let mut y = Vec::new();
        for (shard_count, keys_per_command, coefficient) in settings.clone() {
            // check `n`
            assert_eq!(search.n, n, "inter_machine_scalability_plot: value of n in search doesn't match the provided");

            // refine search
            let key_gen = KeyGen::Zipf {
                coefficient,
                total_keys_per_shard: 1_000_000,
            };
            search
                .shard_count(shard_count)
                .keys_per_command(keys_per_command)
                .key_gen(key_gen);

            // execute search
            let exp_data = db.find(search)?;
            let max_throughput = exp_data
                .into_iter()
                .map(|(_, _, exp_data)| {
                    // compute throughput for each result matching this search
                    // (we can have several, with different number of clients)
                    let throughput = exp_data.global_client_throughput;
                    // compute K ops
                    (throughput / 1000f64) as u64
                })
                .max();

            let max_throughput = max_throughput.unwrap_or_default();
            y.push(max_throughput);
        }

        // compute x: shift all values by `shift`
        let x: Vec<_> = x.iter().map(|&x| x + shift).collect();
        println!("x: {:?} | y: {:?}", x, y);

        // plot it
        let kwargs = bar_style(py, search, &style_fun, BAR_WIDTH)?;
        ax.bar(x, y, Some(kwargs))?;
        plotted += 1;
    }

    // set xticks
    ax.set_xticks(x, None)?;

    let mut shards = BTreeSet::new();
    let labels: Vec<_> = settings
        .into_iter()
        .map(|(shard_count, _, coefficient)| {
            shards.insert(shard_count);
            format!("zipf = {}", coefficient)
        })
        .collect();
    let fontdict = pydict!(py, ("fontsize", 7.5));
    let kwargs = pydict!(py, ("fontdict", fontdict));
    ax.set_xticklabels(labels, Some(kwargs))?;

    // check that the number of shards is 3
    assert!(shards.len() == 3, "unsupported number of shards");
    let kwargs = None;
    plt.text(0.0, -190.0, "2 shards", kwargs)?;
    plt.text(20.0, -190.0, "4 shards", kwargs)?;
    plt.text(40.0, -190.0, "6 shards", kwargs)?;

    // set labels
    let ylabel = String::from("max. throughput (K ops/s)");
    ax.set_ylabel(&ylabel, None)?;

    // maybe set y limits
    if let Some((y_min, y_max)) = y_range {
        let kwargs = pydict!(py, ("ymin", y_min), ("ymax", y_max));
        ax.set_ylim(Some(kwargs))?;
    }

    // legend
    add_legend(plotted, None, None, None, None, py, &ax)?;

    // end plot
    end_plot(plotted > 0, output_dir, output_file, py, &plt, Some(fig))?;
    Ok(())
}

pub fn batching_plot(
    searches: Vec<Search>,
    style_fun: Option<Box<dyn Fn(&Search) -> HashMap<Style, String>>>,
    n: usize,
    settings: Vec<(usize, usize)>,
    y_range: Option<(f64, f64)>,
    output_dir: Option<&str>,
    output_file: &str,
    db: &ResultsDB,
) -> Result<(), Report> {
    const FULL_PER_GROUP_WIDTH: f64 = 10f64;
    const MAX_COMBINATIONS: usize = 2;
    // 80% of `FULL_PER_GROUP_WIDTH` when `MAX_COMBINATIONS` is reached
    const BAR_WIDTH: f64 = FULL_PER_GROUP_WIDTH * 0.6 / MAX_COMBINATIONS as f64;

    assert!(
        searches.len() <= MAX_COMBINATIONS,
        "batching_plot: expected a number of searches smaller than the max number of combinations"
    );

    // compute x: one per setting
    let x: Vec<_> = (0..settings.len())
        .map(|i| i as f64 * FULL_PER_GROUP_WIDTH)
        .collect();

    // we need to shift all to the left by half of the number of searches
    let search_count = searches.len();
    let shift_left = search_count as f64 / 2f64;
    // we also need to shift half bar to the right
    let shift_right = 0.5;
    let searches = searches.into_iter().enumerate().map(|(index, search)| {
        // compute index according to shifts
        let base = index as f64 - shift_left + shift_right;
        // compute combination's shift
        let shift = base * BAR_WIDTH;
        (shift, search)
    });

    // start python
    let gil = Python::acquire_gil();
    let py = gil.python();
    let plt = PyPlot::new(py)?;

    // start plot
    let (fig, ax) = start_plot(py, &plt, None)?;

    // decrease height
    let (width, height) = FIGSIZE;
    fig.set_size_inches(width, height - 0.3)?;

    // keep track of the number of plotted instances
    let mut plotted = 0;

    for (shift, mut search) in searches.clone() {
        let mut y = Vec::new();
        for (batch_max_size, payload_size) in settings.clone() {
            // check `n`
            assert_eq!(search.n, n, "batching_plot: value of n in search doesn't match the provided");

            search
                .batch_max_size(batch_max_size)
                .payload_size(payload_size);

            // execute search
            let exp_data = db.find(search)?;
            let max_throughput = exp_data
                .into_iter()
                .map(|(_, _, exp_data)| {
                    // compute throughput for each result matching this search
                    // (we can have several, with different number of clients)
                    let throughput = exp_data.global_client_throughput;
                    // compute K ops
                    (throughput / 1000f64) as u64
                })
                .max();

            let max_throughput = max_throughput.unwrap_or_default();
            y.push(max_throughput);
        }

        // compute x: shift all values by `shift`
        let x: Vec<_> = x.iter().map(|&x| x + shift).collect();
        println!("x: {:?} | y: {:?}", x, y);

        // plot it
        let kwargs = bar_style(py, search, &style_fun, BAR_WIDTH)?;
        ax.bar(x, y, Some(kwargs))?;
        plotted += 1;
    }

    // set xticks
    ax.set_xticks(x, None)?;

    let mut payload_sizes = BTreeSet::new();
    let labels: Vec<_> = settings
        .into_iter()
        .map(|(max_batch_size, payload_size)| {
            payload_sizes.insert(payload_size);
            match max_batch_size {
                1 => "OFF",
                10000 => "ON",
                _ => panic!("unsupported max batch size: {}", max_batch_size),
            }
        })
        .collect();
    let fontdict = pydict!(py, ("fontsize", 7.5));
    let kwargs = pydict!(py, ("fontdict", fontdict));
    ax.set_xticklabels(labels, Some(kwargs))?;

    // check that the number of payloads is 3
    assert!(payload_sizes.len() == 3, "unsupported number of payloads");
    let kwargs = None;
    plt.text(0.0, -150.0, "256 bytes", kwargs)?;
    plt.text(20.0, -150.0, "1024 bytes", kwargs)?;
    plt.text(40.0, -150.0, "4096 bytes", kwargs)?;

    // set labels
    let ylabel = String::from("max. throughput (K ops/s)");
    ax.set_ylabel(&ylabel, None)?;

    // maybe set y limits
    if let Some((y_min, y_max)) = y_range {
        let kwargs = pydict!(py, ("ymin", y_min), ("ymax", y_max));
        ax.set_ylim(Some(kwargs))?;
    }

    // legend
    let y_bbox_to_anchor = Some(1.22);
    add_legend(plotted, None, None, y_bbox_to_anchor, None, py, &ax)?;

    // end plot
    end_plot(plotted > 0, output_dir, output_file, py, &plt, Some(fig))?;
    Ok(())
}

pub fn throughput_latency_plot_split<GInput, G, RInput, R>(
    n: usize,
    search_gen_inputs: Vec<GInput>,
    search_gen: G,
    clients_per_region: Vec<usize>,
    top_search_refine_input: RInput,
    bottom_search_refine_input: RInput,
    search_refine: R,
    style_fun: Option<Box<dyn Fn(&Search) -> HashMap<Style, String>>>,
    latency_precision: LatencyPrecision,
    x_range: Option<(f64, f64)>,
    y_range: Option<(f64, f64)>,
    y_log_scale: bool,
    x_bbox_to_anchor: Option<f64>,
    legend_column_spacing: Option<f64>,
    left_margin: Option<f64>,
    witdh_reduction: Option<f64>,
    output_dir: Option<&str>,
    output_file: &str,
    db: &ResultsDB,
) -> Result<(Vec<(Search, usize)>, Vec<(Search, usize)>), Report>
where
    GInput: Clone,
    G: Fn(GInput) -> Search,
    RInput: Clone,
    R: Fn(&mut Search, RInput),
{
    // start python
    let gil = Python::acquire_gil();
    let py = gil.python();
    let plt = PyPlot::new(py)?;

    // start plot:
    // - adjust horizontal space between the three plots
    let kwargs = pydict!(py, ("hspace", 0.2));
    if let Some(left_margin) = left_margin {
        pytry!(py, kwargs.set_item("left", left_margin));
    }
    let (fig, _) = start_plot(py, &plt, Some(kwargs))?;

    // increase height
    let (mut width, height) = FIGSIZE;
    if let Some(width_reduction) = witdh_reduction {
        width -= width_reduction;
    }
    fig.set_size_inches(width, height + 1.5)?;

    // keep track of the number of plotted instances
    let mut plotted = 0;
    let mut result = (Vec::new(), Vec::new());

    for (subplot, search_refine_input) in vec![
        (1, top_search_refine_input),
        (2, bottom_search_refine_input),
    ] {
        let ax = plt.subplot(2, 1, subplot, None)?;
        let searches: Vec<_> = search_gen_inputs
            .clone()
            .into_iter()
            .map(|search_gen_input| {
                let mut search = search_gen(search_gen_input);
                search_refine(&mut search, search_refine_input.clone());
                search
            })
            .collect();
        let max_throughputs = inner_throughput_something_plot(
            py,
            &ax,
            searches,
            &style_fun,
            latency_precision,
            n,
            clients_per_region.clone(),
            x_range,
            y_range,
            ThroughputYAxis::Latency(LatencyMetric::Average),
            db,
            &mut plotted,
        )?;

        match subplot {
            1 => result.0 = max_throughputs,
            2 => result.1 = max_throughputs,
            _ => unreachable!(),
        }

        // set legend and labels:
        // - set legend
        // - set xlabel if bottom
        // - set ylabel in both
        match subplot {
            1 => {
                // specific pull-up for this kind of plot
                let y_bbox_to_anchor = Some(1.46);
                // legend
                add_legend(
                    plotted,
                    None,
                    x_bbox_to_anchor,
                    y_bbox_to_anchor,
                    legend_column_spacing,
                    py,
                    &ax,
                )?;
            }
            2 => {
                ax.set_xlabel("throughput (K ops/s)", None)?;
            }
            _ => unreachable!("impossible subplot"),
        }

        let ylabel = if y_log_scale {
            // set log scale on y axis
            set_log_scale(py, &ax, AxisToScale::Y)?;
            format!("latency ({}) [log-scale]", latency_precision.name())
        } else {
            format!("latency ({})", latency_precision.name())
        };
        ax.set_ylabel(&ylabel, None)?;
    }

    // end plot
    end_plot(plotted > 0, output_dir, output_file, py, &plt, Some(fig))?;

    Ok(result)
}

pub fn heatmap_plot_split<F>(
    n: usize,
    protocols: Vec<(Protocol, usize)>,
    clients_per_region: Vec<usize>,
    key_gen: KeyGen,
    search_refine: F,
    leader: ProcessId,
    output_dir: Option<&str>,
    output_file: &str,
    db: &ResultsDB,
) -> Result<(), Report>
where
    F: Fn(&mut Search, KeyGen) + Clone,
{
    // start python
    let gil = Python::acquire_gil();
    let py = gil.python();
    let plt = PyPlot::new(py)?;

    // start plot:
    // - adjust top and bottom to have almost no margin
    // - increase left margin
    // - adjust horizontal space between the three plots
    let kwargs = pydict!(
        py,
        ("top", 0.90),
        ("bottom", 0.30),
        ("left", 0.18),
        ("wspace", 0.1)
    );
    let (fig, _) = start_plot(py, &plt, Some(kwargs))?;

    // increase height
    let (width, height) = FIGSIZE;
    fig.set_size_inches(width, height - 0.8)?;

    for (subplot, set_xlabels, set_ylabels, set_colorbar, heatmap_metric) in vec![
        (1, true, true, false, HeatmapMetric::CPU),
        (2, false, false, false, HeatmapMetric::NetRecv),
        (3, false, false, true, HeatmapMetric::NetSend),
    ] {
        let ax = plt.subplot(1, 3, subplot, None)?;
        inner_heatmap_plot(
            py,
            &fig,
            &ax,
            n,
            protocols.clone(),
            clients_per_region.clone(),
            key_gen,
            search_refine.clone(),
            leader,
            heatmap_metric,
            set_xlabels,
            set_ylabels,
            set_colorbar,
            db,
        )?;

        ax.set_title(&heatmap_metric.name())?;
    }

    // end plot
    end_plot(true, output_dir, output_file, py, &plt, Some(fig))?;

    Ok(())
}

// based on: https://matplotlib.org/3.1.1/gallery/images_contours_and_fields/image_annotated_heatmap.html#sphx-glr-gallery-images-contours-and-fields-image-annotated-heatmap-py
pub fn inner_heatmap_plot<F>(
    py: Python<'_>,
    fig: &Figure<'_>,
    ax: &Axes<'_>,
    n: usize,
    protocols: Vec<(Protocol, usize)>,
    clients_per_region: Vec<usize>,
    key_gen: KeyGen,
    search_refine: F,
    leader: ProcessId,
    heatmap_metric: HeatmapMetric,
    set_xlabels: bool,
    set_ylabels: bool,
    set_colorbar: bool,
    db: &ResultsDB,
) -> Result<(), Report>
where
    F: Fn(&mut Search, KeyGen),
{
    // data for all rows
    let mut rows = Vec::with_capacity(protocols.len());

    for (protocol, f) in protocols.iter() {
        // create search
        let mut search = Search::new(n, *f, *protocol);
        // refine search
        search_refine(&mut search, key_gen);

        // data for this row
        let mut row_data = Vec::with_capacity(clients_per_region.len());

        for clients in clients_per_region.iter() {
            // further refine search
            search.clients_per_region(*clients);

            // execute search
            let mut exp_data = db.find(search)?;
            match exp_data.len() {
                0 => {
                    eprintln!(
                        "missing data for {} f = {}",
                        PlotFmt::protocol_name(search.protocol),
                        search.f
                    );
                    row_data.push(0);
                    continue;
                }
                1 => (),
                _ => {
                    let matches: Vec<_> = exp_data
                        .into_iter()
                        .map(|(timestamp, _, _)| {
                            timestamp.path().display().to_string()
                        })
                        .collect();
                    panic!("found more than 1 matching experiment for this search criteria: search {:?} | matches {:?}", search, matches);
                }
            };
            let (_, _, exp_data) = exp_data.pop().unwrap();

            let dstats = match search.protocol {
                Protocol::FPaxos => {
                    // if fpaxos, use data from the leader
                    exp_data
                        .process_dstats
                        .get(&leader)
                        .expect("leader data should exist")
                }
                _ => {
                    // otherwise, use global data
                    &exp_data.global_process_dstats
                }
            };

            // get data
            let value = match heatmap_metric {
                HeatmapMetric::NetSend => dstats.net_send.mean(),
                HeatmapMetric::NetRecv => dstats.net_recv.mean(),
                HeatmapMetric::CPU => {
                    dstats.cpu_usr.mean() + dstats.cpu_sys.mean()
                }
            };
            let utilization = heatmap_metric.utilization(value);
            row_data.push(utilization);
        }

        println!(
            "{:<7} f = {} | {} -> {:?}",
            PlotFmt::protocol_name(search.protocol),
            search.f,
            heatmap_metric.name(),
            row_data,
        );

        // save row
        rows.push(row_data);
    }

    // list of colormaps: https://matplotlib.org/tutorials/colors/colormaps.html
    let kwargs = pydict!(py, ("cmap", "afmhot_r"), ("vmin", 0), ("vmax", 100));
    // plot the heatmap
    let im = ax.imshow(rows, Some(kwargs))?;

    // create colorbar
    if set_colorbar {
        let cbar_ax = fig.add_axes(vec![0.71, 0.22, 0.24, 0.05])?;
        let kwargs =
            pydict!(py, ("cax", cbar_ax.ax()), ("orientation", "horizontal"),);
        let cbar = fig.colorbar(im.im(), Some(kwargs))?;
        cbar.set_label("utilization (%)", None)?;
        cbar.set_ticks(vec![0, 25, 50, 75, 100], None)?;
        cbar.set_ticklabels(vec![0, 25, 50, 75, 100], None)?;
    }

    // set xticks
    let xticks: Vec<_> = (0..clients_per_region.len()).collect();
    ax.set_xticks(xticks, None)?;
    // set ylabels
    if set_xlabels {
        let kwargs = pydict!(
            py,
            ("rotation", 50),
            ("horizontalalignment", "right"),
            ("rotation_mode", "anchor")
        );
        ax.set_xticklabels(clients_per_region.clone(), Some(kwargs))?;
    } else {
        // hide xlabels
        let kwargs = pydict!(py, ("labelbottom", false));
        ax.tick_params(Some(kwargs))?;
    }

    // set yticks
    let yticks: Vec<_> = (0..protocols.len()).collect();
    ax.set_yticks(yticks, None)?;
    // set ylabels
    if set_ylabels {
        let ylabels: Vec<_> = protocols
            .clone()
            .into_iter()
            .map(|(protocol, f)| PlotFmt::label(protocol, f))
            .collect();
        ax.set_yticklabels(ylabels, None)?;
    } else {
        // hide ylabels
        let kwargs = pydict!(py, ("labelleft", false));
        ax.tick_params(Some(kwargs))?;
    }

    // // hide major ticks on the left
    // let kwargs = pydict!(py, ("which", "major"), ("left", false));
    // ax.tick_params(Some(kwargs))?;

    // create white grid
    let xticks: Vec<_> = (0..clients_per_region.len() + 1)
        .map(|tick| tick as f64 - 0.5)
        .collect();
    let kwargs = pydict!(py, ("minor", true));
    ax.set_xticks(xticks, Some(kwargs))?;
    let yticks: Vec<_> = (0..protocols.len() + 1)
        .map(|tick| tick as f64 - 0.5)
        .collect();
    let kwargs = pydict!(py, ("minor", true));
    ax.set_yticks(yticks, Some(kwargs))?;
    let kwargs = pydict!(
        py,
        // the following makes sure the grid is drawn on the minor ticks
        ("which", "minor"),
        ("color", "black"),
        ("linestyle", "-"),
        ("linewidth", 1.5)
    );
    ax.grid(Some(kwargs))?;

    // hide minor ticks
    let kwargs =
        pydict!(py, ("which", "minor"), ("bottom", false), ("left", false));
    ax.tick_params(Some(kwargs))?;

    Ok(())
}

pub fn dstat_table(
    searches: Vec<Search>,
    metrics_type: MetricsType,
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
    let col_labels = col_labels.into_iter().map(String::from).collect();
    let col_widths = vec![0.13, 0.13, 0.13, 0.20, 0.20, 0.20];

    // actual data
    let mut cells = Vec::with_capacity(searches.len());

    // protocol labels
    let mut row_labels = Vec::with_capacity(searches.len());

    let mut plotted = 0;
    for search in searches {
        let mut exp_data = db.find(search)?;
        match exp_data.len() {
            0 => {
                eprintln!(
                    "missing data for {} f = {}",
                    PlotFmt::protocol_name(search.protocol),
                    search.f
                );
                continue;
            }
            1 => (),
            _ => {
                let matches: Vec<_> = exp_data
                    .into_iter()
                    .map(|(timestamp, _, _)| {
                        timestamp.path().display().to_string()
                    })
                    .collect();
                panic!("found more than 1 matching experiment for this search criteria: search {:?} | matches {:?}", search, matches);
            }
        };
        let (_, _, exp_data) = exp_data.pop().unwrap();

        // select the correct dstats depending on the `MetricsType` chosen
        let dstats = match metrics_type {
            MetricsType::Process(process_id) => {
                match exp_data.process_dstats.get(&process_id) {
                    Some(dstats) => dstats,
                    None => {
                        panic!("didn't found dstat for process {}", process_id)
                    }
                }
            }
            MetricsType::ProcessGlobal => &exp_data.global_process_dstats,
            MetricsType::ClientGlobal => &exp_data.global_client_dstats,
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

        // create row label
        let row_label = format!(
            "{} f = {}",
            PlotFmt::protocol_name(search.protocol),
            search.f
        );
        row_labels.push(row_label);

        // mark that there's data to be plotted
        plotted += 1;
    }

    table(
        plotted,
        col_labels,
        col_widths,
        row_labels,
        cells,
        output_dir,
        output_file,
    )
}

pub fn process_metrics_table(
    searches: Vec<Search>,
    metrics_type: MetricsType,
    output_dir: Option<&str>,
    output_file: &str,
    db: &ResultsDB,
) -> Result<(), Report> {
    let col_labels = vec![
        "fast",
        "slow",
        "(%)",
        "gc",
        "wait delay (ms)",
        "exec delay (ms)",
        // "chains",
        "deps size"
        // "out",
        // "in",
    ];
    let col_labels = col_labels.into_iter().map(String::from).collect();
    let col_widths = vec![0.11, 0.11, 0.07, 0.11, 0.24, 0.20, 0.24];

    // actual data
    let mut cells = Vec::with_capacity(searches.len());

    // protocol labels
    let mut row_labels = Vec::with_capacity(searches.len());

    let mut plotted = 0;
    for search in searches {
        let mut exp_data = db.find(search)?;
        match exp_data.len() {
            0 => {
                eprintln!(
                    "missing data for {} f = {}",
                    PlotFmt::protocol_name(search.protocol),
                    search.f
                );
                continue;
            }
            1 => (),
            _ => {
                // TODO if more than one match *and* the number of clients was
                // not set, then have an entry for each of the entries found
                let matches: Vec<_> = exp_data
                    .into_iter()
                    .map(|(timestamp, _, _)| {
                        timestamp.path().display().to_string()
                    })
                    .collect();
                panic!("found more than 1 matching experiment for this search criteria: search {:?} | matches {:?}", search, matches);
            }
        };
        let (_, _, exp_data) = exp_data.pop().unwrap();

        // select the correct metrics depending on the `MetricsType` chosen
        let (protocol_metrics, executor_metrics) = match metrics_type {
            MetricsType::Process(_) => panic!("unsupported metrics type Process in process_metrics_table"),
            MetricsType::ProcessGlobal => (&exp_data.global_protocol_metrics, &exp_data.global_executor_metrics),
            MetricsType::ClientGlobal => panic!("unsupported metrics type ClientGlobal in process_metrics_table"),
        };

        let fmt = |n: u64| {
            if n > 1_000_000 {
                format!("{}M", n / 1_000_000)
            } else if n > 1_000 {
                format!("{}K", n / 1_000)
            } else {
                format!("{}", n)
            }
        };

        // fetch all cell data
        let fast_path = protocol_metrics
            .get_aggregated(ProtocolMetricsKind::FastPath)
            .cloned()
            .unwrap_or_default();
        let slow_path = protocol_metrics
            .get_aggregated(ProtocolMetricsKind::SlowPath)
            .cloned()
            .unwrap_or_default();
        let fp_rate = (fast_path * 100) as f64 / (fast_path + slow_path) as f64;
        let fast_path = Some(fmt(fast_path));
        let slow_path = Some(fmt(slow_path));
        let fp_rate = Some(format!("{:.1}", fp_rate));
        let gced = protocol_metrics
            .get_aggregated(ProtocolMetricsKind::Stable)
            .map(|gced| fmt(*gced));
        let wait_condition_delay = protocol_metrics
            .get_collected(ProtocolMetricsKind::WaitConditionDelay)
            .map(|delay| {
                format!(
                    "{} ± {} [{}]",
                    delay.mean().value().round(),
                    delay.stddev().value().round(),
                    delay.max().value().round()
                )
            });
        let execution_delay = executor_metrics
            .get_collected(ExecutorMetricsKind::ExecutionDelay)
            .map(|delay| {
                format!(
                    "{} ± {}",
                    delay.mean().value().round(),
                    delay.stddev().value().round()
                )
            });
        // let chain_size = executor_metrics
        //     .get_collected(ExecutorMetricsKind::ChainSize)
        //     .map(|chain_size| {
        //         format!(
        //             "{} ± {} [{}]",
        //             chain_size.mean().round(),
        //             chain_size.stddev().round(),
        //             chain_size.max().value().round()
        //         )
        //     });
        let deps_size = protocol_metrics
            .get_collected(ProtocolMetricsKind::CommittedDepsLen)
            .map(|deps_size| {
                format!(
                    "{} ± {} [{}]",
                    deps_size.mean().value().round(),
                    deps_size.stddev().value().round(),
                    deps_size.max().value().round()
                )
            });
        // let out_requests = executor_metrics
        //     .get_aggregated(ExecutorMetricsKind::OutRequests)
        //     .map(|out_requests| fmt(*out_requests));
        // let in_requests = executor_metrics
        //     .get_aggregated(ExecutorMetricsKind::InRequests)
        //     .map(|in_requests| fmt(*in_requests));
        // create cell
        let cell = vec![
            fast_path,
            slow_path,
            fp_rate,
            gced,
            wait_condition_delay,
            execution_delay,
            // chain_size,
            deps_size,
            /* out_requests,
             * in_requests, */
        ];
        // format cell
        let fmt_cell_data =
            |data: Option<String>| data.unwrap_or(String::from("NA"));
        let cell: Vec<_> = cell.into_iter().map(fmt_cell_data).collect();

        // save cell
        cells.push(cell);

        // create row label
        let row_label = format!(
            "{} f = {}",
            PlotFmt::protocol_name(search.protocol),
            search.f
        );
        row_labels.push(row_label);

        // mark that there's data to be plotted
        plotted += 1;
    }

    table(
        plotted,
        col_labels,
        col_widths,
        row_labels,
        cells,
        output_dir,
        output_file,
    )
}

fn table(
    plotted: usize,
    col_labels: Vec<String>,
    col_widths: Vec<f64>,
    row_labels: Vec<String>,
    cells: Vec<Vec<String>>,
    output_dir: Option<&str>,
    output_file: &str,
) -> Result<(), Report> {
    if plotted > 0 {
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
        end_plot(plotted > 0, output_dir, output_file, py, &plt, None)?;
    }
    Ok(())
}

// percentiles of interest
fn percentiles() -> impl Iterator<Item = f64> {
    (10..=60)
        .step_by(10)
        .chain((65..=95).step_by(5))
        .map(|percentile| percentile as f64 / 100f64)
        .chain(vec![
            0.96, 0.97, 0.98, 0.984, 0.988, 0.992, 0.996, 0.999, 0.9999,
        ])
}

// https://matplotlib.org/3.3.1/api/_as_gen/matplotlib.pyplot.subplots_adjust.html?highlight=subplots_adjust#matplotlib.pyplot.subplots_adjust
pub fn start_plot<'a>(
    py: Python<'a>,
    plt: &'a PyPlot<'a>,
    kwargs: Option<&PyDict>,
) -> Result<(Figure<'a>, Axes<'a>), Report> {
    let (fig, ax) = plt.subplots(None)?;
    // create empty arguments if no arguments were provided
    let kwargs = if let Some(kwargs) = kwargs {
        kwargs
    } else {
        pyo3::types::PyDict::new(py)
    };
    // set adjust top and adjust bottom defaults if not set
    if !pytry!(py, kwargs.contains("top")) {
        pytry!(py, kwargs.set_item("top", ADJUST_TOP));
    }
    if !pytry!(py, kwargs.contains("bottom")) {
        pytry!(py, kwargs.set_item("bottom", ADJUST_BOTTOM));
    }
    fig.subplots_adjust(Some(kwargs))?;

    Ok((fig, ax))
}

pub fn end_plot(
    something_plotted: bool,
    output_dir: Option<&str>,
    output_file: &str,
    py: Python<'_>,
    plt: &PyPlot<'_>,
    fig: Option<Figure<'_>>,
) -> Result<(), Report> {
    if !something_plotted {
        // if nothing was plotted, just close the current figure
        return plt.close(None);
    };

    // maybe save `output_file` in `output_dir` (if one was set)
    let output_file = if let Some(output_dir) = output_dir {
        // make sure `output_dir` exists
        std::fs::create_dir_all(&output_dir).wrap_err("create plot dir")?;
        format!("{}/{}", output_dir, output_file)
    } else {
        output_file.to_string()
    };

    // reduce right margin
    let kwargs = pydict!(py, ("right", 0.95));
    plt.subplots_adjust(Some(kwargs))?;

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

/// `legends` is a mapping from legend order to a pair of the matplotlib object
/// and its legend.
pub fn add_legend(
    plotted: usize,
    legends: Option<BTreeMap<usize, (&PyAny, String)>>,
    x_bbox_to_anchor: Option<f64>,
    y_bbox_to_anchor: Option<f64>,
    column_spacing: Option<f64>,
    py: Python<'_>,
    ax: &Axes<'_>,
) -> Result<(), Report> {
    if plotted == 0 {
        return Ok(());
    }
    // default values for `y_bbox_to_anchor`
    let one_row = 1.19;
    let two_rows = 1.3;

    let (legend_ncol, y_bbox_to_anchor_default) = match plotted {
        0 => (0, 0.0),
        1 => (1, one_row),
        2 => (2, one_row),
        3 => (3, one_row),
        4 => (2, two_rows),
        5 => (3, two_rows),
        6 => (3, two_rows),
        7 => (4, two_rows),
        8 => (4, two_rows),
        9 => (5, two_rows),
        10 => (5, two_rows),
        11 => (6, two_rows),
        12 => (6, two_rows),
        _ => panic!(
            "add_legend: unsupported number of plotted instances: {}",
            plotted
        ),
    };

    // legend position: use default values if x or y not set
    let x_bbox_to_anchor_default = 0.5;
    let x_bbox_to_anchor = x_bbox_to_anchor.unwrap_or(x_bbox_to_anchor_default);
    let y_bbox_to_anchor = y_bbox_to_anchor.unwrap_or(y_bbox_to_anchor_default);

    // add legend
    let kwargs = pydict!(
        py,
        ("loc", "upper center"),
        // pull legend up
        ("bbox_to_anchor", (x_bbox_to_anchor, y_bbox_to_anchor)),
        // remove box around legend:
        ("edgecolor", "white"),
        ("ncol", legend_ncol),
    );
    // set spacing between columns if it was defined
    if let Some(column_spacing) = column_spacing {
        kwargs.set_item("columnspacing", column_spacing)?;
    }
    let legends = legends.map(|map| map.into_iter().map(|(_, l)| l).collect());
    ax.legend(legends, Some(kwargs))?;

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
                n if n <= 1000000 => {
                    (n as f64 / 10000f64).round() as u64 * 10000
                }
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
        ("markersize", 4.8),
        ("linestyle", linestyle),
        ("linewidth", linewidth),
    );
    Ok(kwargs)
}
