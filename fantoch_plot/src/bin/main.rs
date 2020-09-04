use color_eyre::eyre::WrapErr;
use color_eyre::Report;
use fantoch::client::KeyGen;
use fantoch::planet::{Planet, Region};
use fantoch_exp::Protocol;
use fantoch_plot::{
    ErrorBar, ExperimentData, LatencyMetric, MetricsType, PlotFmt, ResultsDB,
    Search, Style, ThroughputYAxis,
};
use std::collections::HashMap;

// folder where all results are stored
const RESULTS_DIR: &str = "../results";
// folder where all plots will be stored
const PLOT_DIR: Option<&str> = Some("plots");

// if true, dstats per process will be generated
const ALL_DSTATS: bool = false;

fn main() -> Result<(), Report> {
    // set global style
    fantoch_plot::set_global_style()?;

    partial_replication()?;
    // multi_key()?;
    // single_key()?;
    // show_distance_matrix();
    Ok(())
}

#[allow(dead_code)]
fn partial_replication() -> Result<(), Report> {
    // fixed parameters
    let n = 3;
    let mut key_gens = Vec::new();
    for coefficient in vec![0.5, 0.6] {
        key_gens.push(KeyGen::Zipf {
            coefficient,
            keys_per_shard: 1_000_000,
        });
    }
    let payload_size = 0;
    // let protocols = vec![Protocol::AtlasLocked];
    let protocols = vec![Protocol::AtlasLocked, Protocol::NewtAtomic];

    let shard_combinations = vec![
        // shard_count, shards_per_command
        // (1, 1),
        // (2, 1),
        (2, 2),
        // (3, 1),
        (3, 2),
        // (4, 1),
        (4, 2),
        // (5, 1),
        (5, 2),
        /*
        (2, 2),
        (3, 1),
        (3, 2),
        (5, 1),
        (6, 1),
        (6, 2),
        */
    ];

    // load results
    let db = ResultsDB::load(RESULTS_DIR).wrap_err("load results")?;

    let clients_per_region = vec![
        1024 / 4,
        1024 / 2,
        1024,
        1024 * 2,
        1024 * 4,
        1024 * 8,
        1024 * 12,
        1024 * 16,
        1024 * 20,
        1024 * 24,
        1024 * 32,
        1024 * 36,
        1024 * 40,
        1024 * 48,
        1024 * 56,
        1024 * 64,
        1024 * 96,
        1024 * 128,
        1024 * 160,
        1024 * 192,
        1024 * 224,
        1024 * 256,
        1024 * 272,
    ];

    for key_gen in key_gens {
        // generate all-combo throughput-something plot
        for y_axis in vec![
            ThroughputYAxis::Latency(LatencyMetric::Average),
            ThroughputYAxis::Latency(LatencyMetric::Percentile(0.99)),
            ThroughputYAxis::Latency(LatencyMetric::Percentile(0.999)),
            ThroughputYAxis::CPU,
        ] {
            let path =
                format!("throughput_{}_n{}_{}.pdf", y_axis.name(), n, key_gen,);
            // create searches
            let searches = shard_combinations
                .clone()
                .into_iter()
                .flat_map(|(shard_count, keys_per_command)| {
                    protocol_combinations(n, protocols.clone()).into_iter().map(
                        move |(protocol, f)| {
                            let mut search = Search::new(n, f, protocol);
                            search
                                .shard_count(shard_count)
                                .key_gen(key_gen)
                                .keys_per_command(keys_per_command)
                                .payload_size(payload_size);
                            search
                        },
                    )
                })
                .collect();

            let style_fun: Option<
                Box<dyn Fn(&Search) -> HashMap<Style, String>>,
            > = Some(Box::new(|search| {
                // create styles
                let mut styles = HashMap::new();
                styles.insert((1, 1), ("#111111", "s"));
                styles.insert((2, 1), ("#218c74", "s"));
                styles.insert((2, 2), ("#218c74", "+"));
                styles.insert((3, 1), ("#bdc3c7", "s"));
                styles.insert((3, 2), ("#bdc3c7", "+"));
                styles.insert((4, 1), ("#ffa726", "s"));
                styles.insert((4, 2), ("#ffa726", "+"));
                styles.insert((5, 1), ("#227093", "s"));
                styles.insert((5, 2), ("#227093", "+"));
                styles.insert((6, 1), ("#1abc9c", "s"));
                styles.insert((6, 2), ("#1abc9c", "+"));

                // get config of this search
                let shard_count = search.shard_count.unwrap();
                let keys_per_command = search.keys_per_command.unwrap();

                // find color and marker for this search
                let (color, marker) = if let Some(entry) =
                    styles.get(&(shard_count, keys_per_command))
                {
                    entry
                } else {
                    panic!(
                        "unsupported shards config pair: {:?}",
                        (shard_count, keys_per_command)
                    );
                };

                // set all styles for this search
                let mut style = HashMap::new();
                style.insert(
                    Style::Label,
                    format!(
                        "{} #{}",
                        PlotFmt::protocol_name(search.protocol),
                        shard_count
                    ),
                );
                style.insert(Style::Color, color.to_string());
                style.insert(Style::Marker, marker.to_string());
                style
            }));
            fantoch_plot::throughput_something_plot(
                searches,
                style_fun,
                n,
                clients_per_region.clone(),
                y_axis,
                PLOT_DIR,
                &path,
                &db,
            )?;
        }

        for (shard_count, keys_per_command) in shard_combinations.clone() {
            // generate throughput-something plot
            for y_axis in vec![
                ThroughputYAxis::Latency(LatencyMetric::Average),
                ThroughputYAxis::Latency(LatencyMetric::Percentile(0.99)),
                ThroughputYAxis::Latency(LatencyMetric::Percentile(0.999)),
                ThroughputYAxis::CPU,
            ] {
                let path = format!(
                    "throughput_{}_n{}_s{}_k{}_{}.pdf",
                    y_axis.name(),
                    n,
                    shard_count,
                    keys_per_command,
                    key_gen,
                );
                // create searches
                let searches = protocol_combinations(n, protocols.clone())
                    .into_iter()
                    .map(|(protocol, f)| {
                        let mut search = Search::new(n, f, protocol);
                        search
                            .shard_count(shard_count)
                            .key_gen(key_gen)
                            .keys_per_command(keys_per_command)
                            .payload_size(payload_size);
                        search
                    })
                    .collect();
                let style_fun = None;
                fantoch_plot::throughput_something_plot(
                    searches,
                    style_fun,
                    n,
                    clients_per_region.clone(),
                    y_axis,
                    PLOT_DIR,
                    &path,
                    &db,
                )?;
            }

            // generate dstat, latency and cdf plots
            for clients_per_region in clients_per_region.clone() {
                println!(
                    "n = {} | s = {} | k = {} | {} | c = {}",
                    n,
                    shard_count,
                    keys_per_command,
                    key_gen,
                    clients_per_region,
                );

                // create searches
                let searches: Vec<_> =
                    protocol_combinations(n, protocols.clone())
                        .into_iter()
                        .map(move |(protocol, f)| {
                            let mut search = Search::new(n, f, protocol);
                            search
                                .clients_per_region(clients_per_region)
                                .shard_count(shard_count)
                                .key_gen(key_gen)
                                .keys_per_command(keys_per_command)
                                .payload_size(payload_size);
                            search
                        })
                        .collect();

                // generate dstat table
                for metrics_type in dstat_combinations(shard_count, n) {
                    let path = format!(
                        "dstat_{}_n{}_s{}_k{}_{}_c{}.pdf",
                        metrics_type.name(),
                        n,
                        shard_count,
                        keys_per_command,
                        key_gen,
                        clients_per_region,
                    );
                    fantoch_plot::dstat_table(
                        searches.clone(),
                        metrics_type,
                        PLOT_DIR,
                        &path,
                        &db,
                    )?;
                }

                // generate process metrics table
                for metrics_type in process_metrics_combinations(shard_count, n)
                {
                    let path = format!(
                        "metrics_{}_n{}_s{}_k{}_{}_c{}.pdf",
                        metrics_type.name(),
                        n,
                        shard_count,
                        keys_per_command,
                        key_gen,
                        clients_per_region,
                    );
                    fantoch_plot::process_metrics_table(
                        searches.clone(),
                        metrics_type,
                        PLOT_DIR,
                        &path,
                        &db,
                    )?;
                }

                // generate latency plot
                let mut shown = false;
                for error_bar in vec![
                    ErrorBar::Without,
                    ErrorBar::With(0.99),
                    ErrorBar::With(0.999),
                ] {
                    let path = format!(
                        "latency{}_n{}_s{}_k{}_{}_c{}.pdf",
                        error_bar.name(),
                        n,
                        shard_count,
                        keys_per_command,
                        key_gen,
                        clients_per_region,
                    );
                    let style_fun = None;
                    let results = fantoch_plot::latency_plot(
                        searches.clone(),
                        style_fun,
                        n,
                        error_bar,
                        PLOT_DIR,
                        &path,
                        &db,
                        fmt_exp_data,
                    )?;

                    if !shown {
                        // only show results once
                        for (search, histogram_fmt) in results {
                            println!(
                                "{:<7} f = {} | {}",
                                PlotFmt::protocol_name(search.protocol),
                                search.f,
                                histogram_fmt,
                            );
                        }
                        shown = true;
                    }
                }

                // generate cdf plot
                let path = format!(
                    "cdf_n{}_s{}_k{}_{}_c{}.pdf",
                    n,
                    shard_count,
                    keys_per_command,
                    key_gen,
                    clients_per_region
                );
                let style_fun = None;
                fantoch_plot::cdf_plot(
                    searches.clone(),
                    style_fun,
                    PLOT_DIR,
                    &path,
                    &db,
                )?;
            }
        }
    }

    Ok(())
}

#[allow(dead_code)]
fn multi_key() -> Result<(), Report> {
    // fixed parameters
    let shard_count = 1;
    let n = 5;
    let payload_size = 0;
    let protocols = vec![
        Protocol::NewtAtomic,
        Protocol::NewtLocked,
        // Protocol::NewtFineLocked,
    ];

    // load results
    let db = ResultsDB::load(RESULTS_DIR).wrap_err("load results")?;

    let clients_per_region = vec![256, 1024, 1024 * 4, 1024 * 8, 1024 * 16];

    for keys_per_shard in vec![8, 64] {
        for zipf_coefficient in vec![1.0] {
            // create key generator
            let key_gen = KeyGen::Zipf {
                coefficient: zipf_coefficient,
                keys_per_shard: 1_000_000,
            };

            // generate throughput-something plot
            for y_axis in vec![
                ThroughputYAxis::Latency(LatencyMetric::Average),
                ThroughputYAxis::Latency(LatencyMetric::Percentile(0.99)),
                ThroughputYAxis::Latency(LatencyMetric::Percentile(0.999)),
                ThroughputYAxis::CPU,
            ] {
                let path = format!(
                    "throughput_{}_n{}_k{}_{}.pdf",
                    y_axis.name(),
                    n,
                    keys_per_shard,
                    key_gen,
                );
                // create searches
                let searches = protocol_combinations(n, protocols.clone())
                    .into_iter()
                    .map(|(protocol, f)| {
                        let mut search = Search::new(n, f, protocol);
                        search
                            .keys_per_command(keys_per_shard)
                            .key_gen(key_gen)
                            .payload_size(payload_size);
                        search
                    })
                    .collect();
                let style_fun = None;
                fantoch_plot::throughput_something_plot(
                    searches,
                    style_fun,
                    n,
                    clients_per_region.clone(),
                    y_axis,
                    PLOT_DIR,
                    &path,
                    &db,
                )?;
            }

            // generate dstat, latency and cdf plots
            for clients_per_region in clients_per_region.clone() {
                println!(
                    "n = {} | k = {} | {} | c = {}",
                    n, keys_per_shard, key_gen, clients_per_region,
                );

                // create searches
                let searches: Vec<_> =
                    protocol_combinations(n, protocols.clone())
                        .into_iter()
                        .map(move |(protocol, f)| {
                            let mut search = Search::new(n, f, protocol);
                            search
                                .clients_per_region(clients_per_region)
                                .key_gen(key_gen)
                                .keys_per_command(keys_per_shard)
                                .payload_size(payload_size);
                            search
                        })
                        .collect();

                // generate dstat table
                for metrics_type in dstat_combinations(shard_count, n) {
                    let path = format!(
                        "dstat_{}_n{}_k{}_{}_c{}.pdf",
                        metrics_type.name(),
                        n,
                        keys_per_shard,
                        key_gen,
                        clients_per_region,
                    );
                    fantoch_plot::dstat_table(
                        searches.clone(),
                        metrics_type,
                        PLOT_DIR,
                        &path,
                        &db,
                    )?;
                }

                // generate process metrics table
                for metrics_type in process_metrics_combinations(shard_count, n)
                {
                    let path = format!(
                        "metrics_{}_n{}_k{}_{}_c{}.pdf",
                        metrics_type.name(),
                        n,
                        keys_per_shard,
                        key_gen,
                        clients_per_region,
                    );
                    fantoch_plot::process_metrics_table(
                        searches.clone(),
                        metrics_type,
                        PLOT_DIR,
                        &path,
                        &db,
                    )?;
                }

                // generate latency plot
                let mut shown = false;
                for error_bar in vec![
                    ErrorBar::Without,
                    ErrorBar::With(0.99),
                    ErrorBar::With(0.999),
                ] {
                    let path = format!(
                        "latency{}_n{}_k{}_{}_c{}.pdf",
                        error_bar.name(),
                        n,
                        keys_per_shard,
                        key_gen,
                        clients_per_region,
                    );
                    let style_fun = None;
                    let results = fantoch_plot::latency_plot(
                        searches.clone(),
                        style_fun,
                        n,
                        error_bar,
                        PLOT_DIR,
                        &path,
                        &db,
                        fmt_exp_data,
                    )?;

                    if !shown {
                        // only show results once
                        for (search, histogram_fmt) in results {
                            println!(
                                "{:<7} f = {} | {}",
                                PlotFmt::protocol_name(search.protocol),
                                search.f,
                                histogram_fmt,
                            );
                        }
                        shown = true;
                    }
                }

                // generate cdf plot
                let path = format!(
                    "cdf_n{}_k{}_{}_c{}.pdf",
                    n, keys_per_shard, key_gen, clients_per_region
                );
                let style_fun = None;
                fantoch_plot::cdf_plot(
                    searches.clone(),
                    style_fun,
                    PLOT_DIR,
                    &path,
                    &db,
                )?;

                if n > 3 {
                    // generate cdf plot with subplots
                    let path = format!(
                        "cdf_one_per_f_n{}_k{}_{}_c{}.pdf",
                        n, keys_per_shard, key_gen, clients_per_region,
                    );
                    let style_fun = None;
                    fantoch_plot::cdf_plot_per_f(
                        searches.clone(),
                        style_fun,
                        PLOT_DIR,
                        &path,
                        &db,
                    )?;
                }
            }
        }
    }

    Ok(())
}

#[allow(dead_code)]
fn single_key() -> Result<(), Report> {
    // fixed parameters
    let key_gen = KeyGen::ConflictRate { conflict_rate: 10 };
    let payload_size = 4096;
    let protocols = vec![
        Protocol::NewtAtomic,
        Protocol::AtlasLocked,
        Protocol::FPaxos,
    ];

    // load results
    let db = ResultsDB::load(RESULTS_DIR).wrap_err("load results")?;

    for n in vec![3, 5] {
        // generate throughput-latency plot
        let clients_per_region = vec![
            32,
            512,
            1024,
            1024 * 2,
            1024 * 4,
            1024 * 8,
            1024 * 16,
            1024 * 32,
        ];

        // generate throughput-something plot
        for y_axis in vec![
            ThroughputYAxis::Latency(LatencyMetric::Average),
            ThroughputYAxis::Latency(LatencyMetric::Percentile(0.99)),
            ThroughputYAxis::Latency(LatencyMetric::Percentile(0.999)),
            ThroughputYAxis::CPU,
        ] {
            let path = format!("throughput_{}_n{}.pdf", y_axis.name(), n,);
            // create searches
            let searches = protocol_combinations(n, protocols.clone())
                .into_iter()
                .map(|(protocol, f)| {
                    let mut search = Search::new(n, f, protocol);
                    search.key_gen(key_gen).payload_size(payload_size);
                    search
                })
                .collect();
            let style_fun = None;
            fantoch_plot::throughput_something_plot(
                searches,
                style_fun,
                n,
                clients_per_region.clone(),
                y_axis,
                PLOT_DIR,
                &path,
                &db,
            )?;
        }

        // generate latency plots
        for clients_per_region in vec![
            4,
            8,
            16,
            32,
            64,
            128,
            256,
            512,
            1024,
            1024 * 2,
            1024 * 4,
            1024 * 8,
            1024 * 16,
            1024 * 32,
        ] {
            println!("n = {} | c = {}", n, clients_per_region);

            // create searches
            let searches: Vec<_> = protocol_combinations(n, protocols.clone())
                .into_iter()
                .map(|(protocol, f)| {
                    let mut search = Search::new(n, f, protocol);
                    search
                        .clients_per_region(clients_per_region)
                        .key_gen(key_gen)
                        .payload_size(payload_size);
                    search
                })
                .collect();

            // generate latency plot
            let mut shown = false;
            for error_bar in vec![
                ErrorBar::Without,
                ErrorBar::With(0.99),
                ErrorBar::With(0.999),
            ] {
                let path = format!(
                    "latency{}_n{}_c{}.pdf",
                    error_bar.name(),
                    n,
                    clients_per_region,
                );
                let style_fun = None;
                let results = fantoch_plot::latency_plot(
                    searches.clone(),
                    style_fun,
                    n,
                    error_bar,
                    PLOT_DIR,
                    &path,
                    &db,
                    fmt_exp_data,
                )?;

                if !shown {
                    // only show results once
                    for (search, histogram_fmt) in results {
                        println!(
                            "{:<7} f = {} | {}",
                            PlotFmt::protocol_name(search.protocol),
                            search.f,
                            histogram_fmt,
                        );
                    }
                    shown = true;
                }
            }

            // generate cdf plot
            let path = format!("cdf_n{}_c{}.pdf", n, clients_per_region);
            let style_fun = None;
            fantoch_plot::cdf_plot(
                searches.clone(),
                style_fun,
                PLOT_DIR,
                &path,
                &db,
            )?;

            if n > 3 {
                // generate cdf plot with subplots
                let path =
                    format!("cdf_one_per_f_n{}_c{}.pdf", n, clients_per_region);
                let style_fun = None;
                fantoch_plot::cdf_plot_per_f(
                    searches.clone(),
                    style_fun,
                    PLOT_DIR,
                    &path,
                    &db,
                )?;
            }
        }
    }

    Ok(())
}

#[allow(dead_code)]
fn show_distance_matrix() {
    // show distance matrix
    let planet = Planet::from("../latency_aws/");
    let regions = vec![
        Region::new("eu-west-1"),
        Region::new("us-west-1"),
        Region::new("ap-southeast-1"),
        Region::new("ca-central-1"),
        Region::new("sa-east-1"),
    ];
    println!("{}", planet.distance_matrix(regions).unwrap());
}

fn protocol_combinations(
    n: usize,
    mut protocols: Vec<Protocol>,
) -> Vec<(Protocol, usize)> {
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

fn dstat_combinations(shard_count: usize, n: usize) -> Vec<MetricsType> {
    let global_metrics =
        vec![MetricsType::ProcessGlobal, MetricsType::ClientGlobal];
    if ALL_DSTATS {
        fantoch::util::all_process_ids(shard_count, n)
            .map(|(process_id, _)| MetricsType::Process(process_id))
            .chain(global_metrics)
            .collect()
    } else {
        global_metrics
    }
}

fn process_metrics_combinations(
    _shard_count: usize,
    _n: usize,
) -> Vec<MetricsType> {
    vec![MetricsType::ProcessGlobal]
}

fn fmt_exp_data(exp_data: &ExperimentData) -> String {
    format!("{:?}", exp_data.global_client_latency)
}
