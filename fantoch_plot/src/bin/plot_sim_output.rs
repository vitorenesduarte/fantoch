use color_eyre::eyre::WrapErr;
use color_eyre::Report;
use fantoch_plot::plot::pyplot::PyPlot;
use pyo3::prelude::*;
use std::collections::{BTreeMap, HashMap};
use std::fmt;

// file with the output of simulation
const SIM_OUTPUT: &str = "sim.out";

// folder where all plots will be stored
const PLOT_DIR: Option<&str> = Some("plots");

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
enum LastLine {
    None,
    PoolSize,
    Conflicts,
    Result,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
struct Config {
    pool_size: usize,
    conflicts: usize,
    protocol: String,
    n: usize,
    f: usize,
    c: usize,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
struct Histogram {
    avg: usize,
    p99: usize,
    p99_9: usize,
}

#[derive(Clone, Debug, Default)]
struct Data {
    wait_condition_delay: Option<Histogram>,
    commit_latency: Option<Histogram>,
    execution_latency: Option<Histogram>,
    execution_delay: Option<Histogram>,
    fast_path_rate: Option<f64>,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum PlotType {
    WaitConditionDelay,
    CommitLatency,
    ExecutionLatency,
    ExecutionDelay,
}

impl PlotType {
    fn title(&self) -> &str {
        match self {
            PlotType::WaitConditionDelay => "Wait Condition Delay",
            PlotType::CommitLatency => "Commit Latency",
            PlotType::ExecutionLatency => "Execution Latency",
            PlotType::ExecutionDelay => "Execution Delay",
        }
    }
}

impl fmt::Debug for PlotType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlotType::WaitConditionDelay => write!(f, "wait_condition_delay"),
            PlotType::CommitLatency => write!(f, "commit_latency"),
            PlotType::ExecutionLatency => write!(f, "execution_latency"),
            PlotType::ExecutionDelay => write!(f, "execution_delay"),
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum MetricType {
    Avg,
    P99,
    P99_9,
}

impl fmt::Debug for MetricType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetricType::Avg => write!(f, "avg"),
            MetricType::P99 => write!(f, "p99"),
            MetricType::P99_9 => write!(f, "p99.9"),
        }
    }
}

fn main() -> Result<(), Report> {
    let sim_out = std::fs::read_to_string(SIM_OUTPUT)
        .wrap_err("error when reading the simulation output file")?;

    let mut last_line = LastLine::None;
    let mut current_pool_size = None;
    let mut current_conflicts = None;
    let mut all_data = HashMap::new();

    for line in sim_out.lines() {
        let parts: Vec<_> = line.split(":").collect();
        assert_eq!(parts.len(), 2);

        match last_line {
            // this line should be a `PoolSize`
            LastLine::None => {
                parse_pool_size(parts, &mut current_pool_size, &mut last_line)
            }
            // this line should be a `Conflicts`
            LastLine::PoolSize => {
                parse_conflicts(parts, &mut current_conflicts, &mut last_line)
            }
            // this line should be a `Result`
            LastLine::Conflicts => parse_result(
                parts,
                &mut current_pool_size,
                &mut current_conflicts,
                &mut all_data,
                &mut last_line,
            ),
            // this line could be a new `PoolSize`, `Conflicts`, or another
            // `Result`
            LastLine::Result => match parts[0] {
                "POOL_SIZE" => parse_pool_size(
                    parts,
                    &mut current_pool_size,
                    &mut last_line,
                ),
                "CONFLICTS" => parse_conflicts(
                    parts,
                    &mut current_conflicts,
                    &mut last_line,
                ),
                _ => parse_result(
                    parts,
                    &mut current_pool_size,
                    &mut current_conflicts,
                    &mut all_data,
                    &mut last_line,
                ),
            },
        }
    }

    plot_data(all_data)
}

fn plot_data(all_data: HashMap<Config, Data>) -> Result<(), Report> {
    let plot_types = vec![
        PlotType::WaitConditionDelay,
        PlotType::CommitLatency,
        PlotType::ExecutionLatency,
        PlotType::ExecutionDelay,
    ];
    let metric_types =
        vec![MetricType::Avg, MetricType::P99, MetricType::P99_9];
    let pool_sizes = vec![100, 50, 10, 1];
    let conflicts = vec![0, 1, 2, 5, 10, 20];
    let protocol = String::from("Caesar");
    let n = 5;
    let f = 2;
    let cs = vec![128, 256, 512];

    for plot_type in plot_types.clone() {
        for metric_type in metric_types.clone() {
            for pool_size in pool_sizes.clone() {
                plot(
                    plot_type,
                    metric_type,
                    pool_size,
                    conflicts.clone(),
                    protocol.clone(),
                    n,
                    f,
                    cs.clone(),
                    &all_data,
                )?;
            }
        }
    }

    Ok(())
}

fn plot(
    plot_type: PlotType,
    metric_type: MetricType,
    pool_size: usize,
    conflicts: Vec<usize>,
    protocol: String,
    n: usize,
    f: usize,
    cs: Vec<usize>,
    all_data: &HashMap<Config, Data>,
) -> Result<(), Report> {
    let data: Vec<_> = cs
        .into_iter()
        .map(|c| {
            let values: Vec<_> = conflicts
                .clone()
                .into_iter()
                .map(|conflicts| {
                    let config = Config {
                        pool_size,
                        conflicts,
                        protocol: protocol.clone(),
                        n,
                        f,
                        c,
                    };
                    if let Some(value) = get_plot_value(
                        plot_type,
                        metric_type,
                        &config,
                        &all_data,
                    ) {
                        value
                    } else {
                        // there can only be no value if the plot type is wait
                        // condition
                        assert_eq!(plot_type, PlotType::WaitConditionDelay);
                        0
                    }
                })
                .collect();
            (c, values)
        })
        .collect();
    let title = format!("{} (pool size = {:?})", plot_type.title(), pool_size);
    let output_file =
        format!("{}_{:?}_{:?}.pdf", pool_size, metric_type, plot_type);
    latency_plot(title, metric_type, conflicts, data, PLOT_DIR, &output_file)
}

fn latency_plot(
    title: String,
    metric_type: MetricType,
    conflicts: Vec<usize>,
    data: Vec<(usize, Vec<usize>)>,
    output_dir: Option<&str>,
    output_file: &str,
) -> Result<(), Report> {
    const FULL_REGION_WIDTH: f64 = 10f64;
    const MAX_COMBINATIONS: usize = 3;
    // 80% of `FULL_REGION_WIDTH` when `MAX_COMBINATIONS` is reached
    const BAR_WIDTH: f64 = FULL_REGION_WIDTH * 0.8 / MAX_COMBINATIONS as f64;

    assert_eq!(data.len(), 3);

    // compute x: one per region
    let x: Vec<_> = (0..conflicts.len())
        .map(|i| i as f64 * FULL_REGION_WIDTH)
        .collect();

    // we need to shift all to the left by half of the number of combinations
    let cs_count = data.len();
    let shift_left = cs_count as f64 / 2f64;
    // we also need to shift half bar to the right
    let shift_right = 0.5;
    let data = data.into_iter().enumerate().map(|(index, c)| {
        // compute index according to shifts
        let base = index as f64 - shift_left + shift_right;
        // compute combination's shift
        let shift = base * BAR_WIDTH;
        (shift, c)
    });

    // start python
    let gil = Python::acquire_gil();
    let py = gil.python();
    let plt = PyPlot::new(py)?;

    // start plot
    let (fig, ax) = fantoch_plot::start_plot(py, &plt, None)?;

    // keep track of the number of plotted instances
    let mut plotted = 0;
    let mut legends = BTreeMap::new();

    for (legend_order, (shift, (c, y))) in data.into_iter().enumerate() {
        // compute x: shift all values by `shift`
        let x: Vec<_> = x.iter().map(|&x| x + shift).collect();

        // bar style
        let kwargs = fantoch_plot::pydict!(
            py,
            // ("label", label),
            ("width", BAR_WIDTH),
            ("edgecolor", "black"),
            ("linewidth", 1),
            /* ("color", color),
             * ("hatch", hatch), */
        );
        let line = ax.bar(x, y, Some(kwargs))?;
        plotted += 1;

        // save line with its legend order
        legends.insert(legend_order, (line, format!("clients = {}", c)));
    }

    // set xticks
    ax.set_xticks(x, None)?;

    let labels: Vec<_> = conflicts
        .into_iter()
        .map(|conflict| format!("{}%", conflict))
        .collect();
    ax.set_xticklabels(labels, None)?;

    // set labels
    let xlabel = "conflict rate";
    ax.set_xlabel(xlabel, None)?;
    let ylabel = format!("{:?} latency (ms)", metric_type);
    ax.set_ylabel(&ylabel, None)?;

    // set title
    ax.set_title(&title)?;

    // legend
    fantoch_plot::add_legend(plotted, Some(legends), None, None, py, &ax)?;

    // end plot
    fantoch_plot::end_plot(
        plotted > 0,
        output_dir,
        output_file,
        py,
        &plt,
        Some(fig),
    )?;
    Ok(())
}

fn get_plot_value(
    plot_type: PlotType,
    metric_type: MetricType,
    config: &Config,
    all_data: &HashMap<Config, Data>,
) -> Option<usize> {
    let data = all_data.get(config).expect("config should exist");
    let histogram = match plot_type {
        PlotType::WaitConditionDelay => &data.wait_condition_delay,
        PlotType::CommitLatency => &data.commit_latency,
        PlotType::ExecutionLatency => &data.execution_latency,
        PlotType::ExecutionDelay => &data.execution_delay,
    };
    histogram.as_ref().map(|histogram| match metric_type {
        MetricType::Avg => histogram.avg,
        MetricType::P99 => histogram.p99,
        MetricType::P99_9 => histogram.p99_9,
    })
}

fn parse_pool_size(
    parts: Vec<&str>,
    current_pool_size: &mut Option<usize>,
    last_line: &mut LastLine,
) {
    assert_eq!(parts[0], "POOL_SIZE");
    let pool_size = parse_usize(parts[1], "pool size");
    *current_pool_size = Some(pool_size);

    // update last line
    *last_line = LastLine::PoolSize;
}

fn parse_conflicts(
    parts: Vec<&str>,
    current_conflicts: &mut Option<usize>,
    last_line: &mut LastLine,
) {
    assert_eq!(parts[0], "CONFLICTS");
    let conflicts = parse_usize(parts[1], "conflicts");
    *current_conflicts = Some(conflicts);

    // update last line
    *last_line = LastLine::Conflicts;
}

fn parse_result(
    parts: Vec<&str>,
    current_pool_size: &mut Option<usize>,
    current_conflicts: &mut Option<usize>,
    all_data: &mut HashMap<Config, Data>,
    last_line: &mut LastLine,
) {
    // get header and entry
    let header = parts[0];
    let entry = parts[1];

    // parse the header
    let header_parts: Vec<_> = header.split("|").collect();
    let config = header_parts[0];
    let entry_type = header_parts[1];
    // parse config
    let config = parse_config(config, current_pool_size, current_conflicts);

    // parse result entry
    parse_result_entry(entry_type, entry, config, all_data);

    // update last line
    *last_line = LastLine::Result;
}

fn parse_usize(to_parse: &str, what: &str) -> usize {
    match to_parse.trim().parse() {
        Ok(result) => result,
        Err(e) => {
            panic!("error parsing {}: {:?}", what, e);
        }
    }
}

fn parse_config(
    config: &str,
    current_pool_size: &mut Option<usize>,
    current_conflicts: &mut Option<usize>,
) -> Config {
    let parts: Vec<_> = config.split_whitespace().collect();

    // get pool size and conflicts
    let pool_size = current_pool_size
        .as_ref()
        .cloned()
        .expect("pool size should have been set");
    let conflicts = current_conflicts
        .as_ref()
        .cloned()
        .expect("conflicts should have been set");

    // parse protocol
    let protocol = parts[0].to_string();

    // parse n
    assert_eq!(parts[1], "n");
    assert_eq!(parts[2], "=");
    let n = parse_usize(parts[3], "n");

    // parse f
    assert_eq!(parts[4], "f");
    assert_eq!(parts[5], "=");
    let f = parse_usize(parts[6], "f");

    // parse c
    assert_eq!(parts[7], "c");
    assert_eq!(parts[8], "=");
    let c = parse_usize(parts[9], "c");

    Config {
        pool_size,
        conflicts,
        protocol,
        n,
        f,
        c,
    }
}

fn parse_result_entry(
    entry_type: &str,
    entry: &str,
    config: Config,
    all_data: &mut HashMap<Config, Data>,
) {
    let mut data = all_data.entry(config).or_default();
    match entry_type.trim() {
        "wait condition delay" => {
            assert!(data.wait_condition_delay.is_none());
            if let Some(histogram) = parse_histogram(entry) {
                data.wait_condition_delay = Some(histogram);
            }
        }
        "commit latency" => {
            assert!(data.commit_latency.is_none());
            let histogram = parse_histogram(entry)
                .expect("commit latency histogram must exist");
            data.commit_latency = Some(histogram);
        }
        "execution latency" => {
            assert!(data.execution_latency.is_none());
            let histogram = parse_histogram(entry)
                .expect("execution latency histogram must exist");
            data.execution_latency = Some(histogram);
        }
        "execution delay" => {
            assert!(data.execution_delay.is_none());
            let histogram = parse_histogram(entry)
                .expect("execution delay histogram must exist");
            data.execution_delay = Some(histogram);
        }
        "fast path rate" => {
            assert!(data.fast_path_rate.is_none());
            let fast_path_rate = entry
                .trim()
                .parse()
                .expect("fast path rate should be a float");
            data.fast_path_rate = Some(fast_path_rate);
        }
        entry_type => {
            panic!("unsupported entry type: {:?}", entry_type);
        }
    }
}

fn parse_histogram(histogram: &str) -> Option<Histogram> {
    let parts: Vec<_> = histogram.split_whitespace().collect();
    if parts[0] == "(empty)" {
        return None;
    }

    let parse_histogram_entry = |entry_type: &str, entry: &str| {
        let parts: Vec<_> = entry.split("=").collect();
        assert_eq!(parts[0], entry_type);
        parse_usize(parts[1], entry_type)
    };

    // parse avg, p99 and p99.9
    let avg = parse_histogram_entry("avg", parts[0]);
    let p99 = parse_histogram_entry("p99", parts[3]);
    let p99_9 = parse_histogram_entry("p99.9", parts[4]);
    let histogram = Histogram { avg, p99, p99_9 };
    Some(histogram)
}
