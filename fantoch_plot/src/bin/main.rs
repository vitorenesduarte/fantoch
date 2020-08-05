use color_eyre::eyre::WrapErr;
use color_eyre::Report;
use fantoch::client::{KeyGen, ShardGen};
use fantoch::planet::{Planet, Region};
use fantoch_exp::Protocol;
use fantoch_plot::{
    DstatType, ErrorBar, ExperimentData, LatencyMetric, PlotFmt, ResultsDB,
    Search, Style,
};
use std::collections::HashMap;

// folder where all results are stored
const RESULTS_DIR: &str = "../partial_replication";
// folder where all plots will be stored
const PLOT_DIR: Option<&str> = Some("plots");

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
    let keys_per_shard = 1;
    /*
    let zipf_key_count = 1_000_000;
    let zipf_coefficient = 1.0;
    let key_gen = KeyGen::Zipf {
        coefficient: zipf_coefficient,
        key_count: zipf_key_count,
    };
    */
    let key_gens = vec![
        KeyGen::ConflictRate { conflict_rate: 1 },
        KeyGen::ConflictRate { conflict_rate: 0 },
    ];
    let payload_size = 0;
    let protocols = vec![Protocol::NewtAtomic, Protocol::AtlasLocked];

    let shard_combinations = vec![
        // shards_per_command, shard_count
        (1, 1),
        (1, 2),
        /*
        (2, 2),
        (1, 3),
        (2, 3),
        (1, 4),
        (1, 5),
        (1, 6),
        */
    ];

    // load results
    let db = ResultsDB::load(RESULTS_DIR).wrap_err("load results")?;

    let clients_per_region = vec![
        1024 * 4,
        1024 * 8,
        1024 * 16,
        1024 * 24,
        1024 * 32,
        1024 * 36,
        1024 * 40,
        1024 * 48,
        1024 * 56,
        1024 * 64,
        1024 * 80,
        1024 * 96,
        1024 * 112,
        1024 * 128,
        1024 * 144,
        1024 * 160,
        1024 * 176,
        1024 * 192,
        1024 * 208,
        1024 * 224,
        1024 * 240,
        1024 * 256,
        1024 * 272,
    ];

    for key_gen in key_gens {
        // generate all-combo throughput-latency plot
        for latency_metric in vec![
            LatencyMetric::Average,
            LatencyMetric::Percentile(0.99),
            LatencyMetric::Percentile(0.999),
        ] {
            let path = format!(
                "throughput_latency_n{}_{}{}.pdf",
                n,
                key_gen,
                latency_metric.to_file_suffix(),
            );
            // create searches
            let searches = shard_combinations
                .clone()
                .into_iter()
                .flat_map(|(shards_per_command, shard_count)| {
                    let shard_gen = ShardGen::Random { shard_count };
                    protocol_combinations(n, protocols.clone()).into_iter().map(
                        move |(protocol, f)| {
                            let mut search = Search::new(n, f, protocol);
                            search
                                .shards_per_command(shards_per_command)
                                .shard_gen(shard_gen)
                                .keys_per_shard(keys_per_shard)
                                .key_gen(key_gen)
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
                styles.insert((1, 1), ("#1abc9c", "s"));
                styles.insert((1, 2), ("#218c74", "D"));
                styles.insert((2, 2), ("#227093", "."));
                styles.insert((1, 3), ("#bdc3c7", "+"));
                styles.insert((2, 3), ("#34495e", "x"));
                styles.insert((1, 4), ("#ffa726", "v"));
                styles.insert((1, 5), ("#227093", "."));
                styles.insert((1, 6), ("#34495e", "x"));

                // get shards config of this search
                let shards_per_command = search.shards_per_command.unwrap();
                let ShardGen::Random { shard_count } =
                    search.shard_gen.unwrap();

                // find color and marker for this search
                let (color, marker) = if let Some(entry) =
                    styles.get(&(shards_per_command, shard_count))
                {
                    entry
                } else {
                    panic!(
                        "unsupported shards config pair: {:?}",
                        (shards_per_command, shard_count)
                    );
                };

                // set all styles for this search
                let mut style = HashMap::new();
                style.insert(
                    Style::Label,
                    format!("t = {} | s = {}", shard_count, shards_per_command),
                );
                style.insert(Style::Color, color.to_string());
                style.insert(Style::Marker, marker.to_string());
                style
            }));
            fantoch_plot::throughput_latency_plot(
                searches,
                style_fun,
                n,
                clients_per_region.clone(),
                latency_metric,
                PLOT_DIR,
                &path,
                &db,
            )?;
        }

        for (shards_per_command, shard_count) in shard_combinations.clone() {
            let shard_gen = ShardGen::Random { shard_count };

            // generate throughput-latency plot
            for latency_metric in vec![
                LatencyMetric::Average,
                LatencyMetric::Percentile(0.99),
                LatencyMetric::Percentile(0.999),
            ] {
                let path = format!(
                    "throughput_latency_n{}_ts{}_s{}_{}{}.pdf",
                    n,
                    shard_count,
                    shards_per_command,
                    key_gen,
                    latency_metric.to_file_suffix(),
                );
                // create searches
                let searches = protocol_combinations(n, protocols.clone())
                    .into_iter()
                    .map(|(protocol, f)| {
                        let mut search = Search::new(n, f, protocol);
                        search
                            .shards_per_command(shards_per_command)
                            .shard_gen(shard_gen)
                            .keys_per_shard(keys_per_shard)
                            .key_gen(key_gen)
                            .payload_size(payload_size);
                        search
                    })
                    .collect();
                let style_fun = None;
                fantoch_plot::throughput_latency_plot(
                    searches,
                    style_fun,
                    n,
                    clients_per_region.clone(),
                    latency_metric,
                    PLOT_DIR,
                    &path,
                    &db,
                )?;
            }

            // generate dstat, latency and cdf plots
            for clients_per_region in clients_per_region.clone() {
                println!(
                    "n = {} | ts = {} | s = {} | {} | c = {}",
                    n,
                    shard_count,
                    shards_per_command,
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
                                .shards_per_command(shards_per_command)
                                .shard_gen(shard_gen)
                                .keys_per_shard(keys_per_shard)
                                .key_gen(key_gen)
                                .payload_size(payload_size);
                            search
                        })
                        .collect();

                // generate dstat table
                for dstat_type in dstat_combinations(shard_count, n) {
                    let path = format!(
                        "dstat_n{}_ts{}_s{}_c{}_{}_{}.pdf",
                        n,
                        shard_count,
                        shards_per_command,
                        clients_per_region,
                        key_gen,
                        dstat_type.name(),
                    );
                    fantoch_plot::dstat_table(
                        searches.clone(),
                        dstat_type,
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
                        "latency_n{}_ts{}_s{}_{}_c{}{}.pdf",
                        n,
                        shard_count,
                        shards_per_command,
                        key_gen,
                        clients_per_region,
                        error_bar.to_file_suffix(),
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
                    "cdf_n{}_ts{}_s{}_{}_c{}.pdf",
                    n,
                    shard_count,
                    shards_per_command,
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
    let zipf_key_count = 1_000_000;
    // let key_gen = KeyGen::ConflictRate { conflict_rate: 10 };
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
                key_count: zipf_key_count,
            };

            // generate throughput-latency plot
            for latency_metric in vec![
                LatencyMetric::Average,
                LatencyMetric::Percentile(0.99),
                LatencyMetric::Percentile(0.999),
            ] {
                let path = format!(
                    "throughput_latency_n{}_k{}_{}{}.pdf",
                    n,
                    keys_per_shard,
                    key_gen,
                    latency_metric.to_file_suffix(),
                );
                // create searches
                let searches = protocol_combinations(n, protocols.clone())
                    .into_iter()
                    .map(|(protocol, f)| {
                        let mut search = Search::new(n, f, protocol);
                        search
                            .keys_per_shard(keys_per_shard)
                            .key_gen(key_gen)
                            .payload_size(payload_size);
                        search
                    })
                    .collect();
                let style_fun = None;
                fantoch_plot::throughput_latency_plot(
                    searches,
                    style_fun,
                    n,
                    clients_per_region.clone(),
                    latency_metric,
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
                                .keys_per_shard(keys_per_shard)
                                .payload_size(payload_size);
                            search
                        })
                        .collect();

                // generate dstat table
                for dstat_type in dstat_combinations(shard_count, n) {
                    let path = format!(
                        "dstat_n{}_k{}_c{}_{}_{}.pdf",
                        n,
                        keys_per_shard,
                        clients_per_region,
                        key_gen,
                        dstat_type.name(),
                    );
                    fantoch_plot::dstat_table(
                        searches.clone(),
                        dstat_type,
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
                        "latency_n{}_k{}_{}_c{}{}.pdf",
                        n,
                        keys_per_shard,
                        key_gen,
                        clients_per_region,
                        error_bar.to_file_suffix()
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

        for latency_metric in vec![
            LatencyMetric::Average,
            LatencyMetric::Percentile(0.99),
            LatencyMetric::Percentile(0.999),
        ] {
            let path = format!(
                "throughput_latency_n{}{}.pdf",
                n,
                latency_metric.to_file_suffix()
            );
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
            fantoch_plot::throughput_latency_plot(
                searches,
                style_fun,
                n,
                clients_per_region.clone(),
                latency_metric,
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
                    "latency_n{}_c{}{}.pdf",
                    n,
                    clients_per_region,
                    error_bar.to_file_suffix()
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

fn dstat_combinations(shard_count: usize, n: usize) -> Vec<DstatType> {
    let global_dstats = vec![DstatType::ProcessGlobal, DstatType::ClientGlobal];
    fantoch::util::all_process_ids(shard_count, n)
        .map(|(process_id, _)| DstatType::Process(process_id))
        .chain(global_dstats)
        .collect()
}

fn fmt_exp_data(exp_data: &ExperimentData) -> String {
    format!("{:?}", exp_data.global_client_latency)
}
