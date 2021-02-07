use color_eyre::eyre::WrapErr;
use color_eyre::Report;
use fantoch::client::KeyGen;
use fantoch::planet::{Planet, Region};
use fantoch_exp::Protocol;
use fantoch_plot::{
    ErrorBar, ExperimentData, HeatmapMetric, LatencyMetric, LatencyPrecision,
    MetricsType, PlotFmt, ResultsDB, Search, Style, ThroughputYAxis,
};
use std::collections::HashMap;

// folder where all plots will be stored
const PLOT_DIR: Option<&str> = Some("plots");

// if true, dstats per process will be generated
const ALL_DSTATS: bool = true;

fn main() -> Result<(), Report> {
    // set global style
    fantoch_plot::set_global_style()?;

    // partial_replication_all()?;
    // multi_key()?;
    // single_key_all()?;
    eurosys()?;
    Ok(())
}

#[allow(dead_code)]
fn eurosys() -> Result<(), Report> {
    // fairness_plot()?;
    // tail_latency_plot()?;
    // increasing_load_plot()?;
    batching_plot()?;
    // scalability_plot()?;
    // partial_replication_plot()?;
    Ok(())
}

#[allow(dead_code)]
fn fairness_plot() -> Result<(), Report> {
    println!(">>>>>>>> FAIRNESS <<<<<<<<");
    // let results_dir =
    //     "/home/vitor.enes/eurosys_results/results_fairness_and_tail_latency";
    let results_dir = "../results_fairness_and_tail_latency";
    // fixed parameters
    let key_gen = KeyGen::ConflictPool {
        conflict_rate: 2,
        pool_size: 1,
    };
    let payload_size = 100;
    let protocols = vec![
        (Protocol::NewtAtomic, 1),
        (Protocol::AtlasLocked, 1),
        (Protocol::FPaxos, 1),
        (Protocol::NewtAtomic, 2),
        (Protocol::AtlasLocked, 2),
        (Protocol::FPaxos, 2),
        (Protocol::CaesarLocked, 2),
    ];
    let legend_order = vec![0, 2, 4, 1, 3, 5, 6];
    let n = 5;
    let clients_per_region = 512;
    let error_bar = ErrorBar::Without;

    // load results
    let db = ResultsDB::load(results_dir).wrap_err("load results")?;

    // create searches
    let searches: Vec<_> = protocols
        .into_iter()
        .map(|(protocol, f)| {
            let mut search = Search::new(n, f, protocol);
            match protocol {
                Protocol::FPaxos => {
                    // if fpaxos, don't filter by key gen as contention does not
                    // affect the results
                }
                Protocol::AtlasLocked
                | Protocol::NewtAtomic
                | Protocol::CaesarLocked => {
                    search.key_gen(key_gen);
                }
                _ => {
                    panic!("unsupported protocol: {:?}", protocol);
                }
            }
            // filter by clients per region and payload size in all protocols
            search
                .clients_per_region(clients_per_region)
                .payload_size(payload_size);
            search
        })
        .collect();

    // generate latency plot
    let path = String::from("plot_fairness.pdf");
    let style_fun = None;
    let latency_precision = LatencyPrecision::Millis;
    let results = fantoch_plot::latency_plot(
        searches,
        Some(legend_order),
        style_fun,
        latency_precision,
        n,
        error_bar,
        PLOT_DIR,
        &path,
        &db,
        fmt_exp_data,
    )?;
    for (search, histogram_fmt) in results {
        println!(
            "{:<7} f = {} | {}",
            PlotFmt::protocol_name(search.protocol),
            search.f,
            histogram_fmt,
        );
    }
    Ok(())
}

#[allow(dead_code)]
fn tail_latency_plot() -> Result<(), Report> {
    println!(">>>>>>>> TAIL LATENCY <<<<<<<<");
    let results_dir = "../results_fairness_and_tail_latency";
    // fixed parameters
    let key_gen = KeyGen::ConflictPool {
        conflict_rate: 2,
        pool_size: 1,
    };
    let payload_size = 100;
    let protocols = vec![
        (Protocol::NewtAtomic, 1),
        (Protocol::NewtAtomic, 2),
        (Protocol::AtlasLocked, 1),
        (Protocol::AtlasLocked, 2),
        (Protocol::CaesarLocked, 2),
        // (Protocol::FPaxos, 1),
        (Protocol::EPaxosLocked, 2),
    ];
    let n = 5;
    let clients_per_region_top = 256;
    let clients_per_region_bottom = 512;

    // load results
    let db = ResultsDB::load(results_dir).wrap_err("load results")?;

    // create searches
    let create_searches = |clients_per_region| {
        protocols
            .clone()
            .into_iter()
            .map(|(protocol, f)| {
                let mut search = Search::new(n, f, protocol);
                search
                    .key_gen(key_gen)
                    .payload_size(payload_size)
                    .clients_per_region(clients_per_region);
                search
            })
            .collect()
    };
    let top_searches = create_searches(clients_per_region_top);
    let bottom_searches = create_searches(clients_per_region_bottom);
    let x_range = Some((100.0, 15_000.0));

    // generate cdf plot
    let path = String::from("plot_tail_latency.pdf");
    let style_fun = None;
    let latency_precision = LatencyPrecision::Millis;
    fantoch_plot::cdf_plot_split(
        top_searches,
        bottom_searches,
        x_range,
        style_fun,
        latency_precision,
        PLOT_DIR,
        &path,
        &db,
    )?;

    Ok(())
}

#[allow(dead_code)]
fn increasing_load_plot() -> Result<(), Report> {
    println!(">>>>>>>> INCREASING LOAD <<<<<<<<");
    let results_dir =
        "/home/vitor.enes/eurosys_results/results_increasing_load";

    // fixed parameters
    let top_key_gen = KeyGen::ConflictPool {
        conflict_rate: 2,
        pool_size: 1,
    };
    let bottom_key_gen = KeyGen::ConflictPool {
        conflict_rate: 10,
        pool_size: 1,
    };
    let payload_size = 4096;
    let batch_max_size = 1;
    let n = 5;
    let leader = 1;

    // generate throughput-latency plot
    let clients_per_region = vec![
        32,
        512,
        1024,
        1024 * 2,
        1024 * 4,
        1024 * 8,
        1024 * 16,
        1024 * 20,
    ];
    // let clients_per_region = vec![
    //     32,
    //     1024,
    //     1024 * 4,
    //     1024 * 16,
    //     1024 * 24,
    //     1024 * 32,
    //     1024 * 40,
    //     1024 * 48,
    //     1024 * 56,
    //     1024 * 60,
    //     1024 * 64,
    // ];

    // load results
    let db = ResultsDB::load(results_dir).wrap_err("load results")?;

    let search_refine = |search: &mut Search, key_gen: KeyGen| {
        match search.protocol {
            Protocol::FPaxos => {
                // if fpaxos, don't filter by key gen as
                // contention does not affect the results
            }
            Protocol::AtlasLocked
            | Protocol::NewtAtomic
            | Protocol::EPaxosLocked
            | Protocol::CaesarLocked
            | Protocol::Basic => {
                search.key_gen(key_gen);
            }
            _ => {
                panic!("unsupported protocol: {:?}", search.protocol);
            }
        }
        // filter by payload size and batch max size in all protocols
        search
            .payload_size(payload_size)
            .batch_max_size(batch_max_size);
    };

    let protocols = vec![
        (Protocol::NewtAtomic, 1),
        (Protocol::NewtAtomic, 2),
        (Protocol::AtlasLocked, 1),
        (Protocol::AtlasLocked, 2),
        (Protocol::FPaxos, 1),
        (Protocol::FPaxos, 2),
        (Protocol::CaesarLocked, 2),
        /*
        (Protocol::Basic, 1),
        (Protocol::EPaxosLocked, 2),
        */
    ];

    let path = format!("plot_increasing_load_heatmap_{}.pdf", top_key_gen);
    fantoch_plot::heatmap_plot_split(
        n,
        protocols.clone(),
        clients_per_region.clone(),
        top_key_gen,
        search_refine,
        leader,
        PLOT_DIR,
        &path,
        &db,
    )?;

    let path = format!("plot_increasing_load_heatmap_{}.pdf", bottom_key_gen);
    fantoch_plot::heatmap_plot_split(
        n,
        protocols.clone(),
        clients_per_region.clone(),
        bottom_key_gen,
        search_refine,
        leader,
        PLOT_DIR,
        &path,
        &db,
    )?;

    let search_gen = |(protocol, f)| Search::new(n, f, protocol);
    let style_fun = None;
    let latency_precision = LatencyPrecision::Millis;
    let x_range = None;
    let y_range = Some((100.0, 1500.0));
    let y_log_scale = true;
    let x_bbox_to_anchor = None;
    let left_margin = None;
    let width_reduction = None;
    let path = String::from("plot_increasing_load.pdf");
    fantoch_plot::throughput_latency_plot_split(
        n,
        protocols.clone(),
        search_gen,
        clients_per_region.clone(),
        top_key_gen,
        bottom_key_gen,
        search_refine,
        style_fun,
        latency_precision,
        x_range,
        y_range,
        y_log_scale,
        x_bbox_to_anchor,
        left_margin,
        width_reduction,
        PLOT_DIR,
        &path,
        &db,
    )?;

    Ok(())
}

#[allow(dead_code)]
fn batching_plot() -> Result<(), Report> {
    println!(">>>>>>>> BATCHING <<<<<<<<");
    let results_dir = "../results_increasing_load";

    // fixed parameters
    let key_gen = KeyGen::ConflictPool {
        conflict_rate: 2,
        pool_size: 1,
    };
    let empty_key_gen = KeyGen::ConflictPool {
        conflict_rate: 0,
        pool_size: 1,
    };

    let n = 5;
    let protocols = vec![
        (Protocol::NewtAtomic, 1),
        // (Protocol::NewtAtomic, 2),
        (Protocol::FPaxos, 1),
        // (Protocol::FPaxos, 2),
    ];
    let search_gen = |(protocol, f)| Search::new(n, f, protocol);

    let settings = vec![
        // (batch_max_size, payload_size)
        (1, 256),
        (10000, 256),
        (1, 1024),
        (10000, 1024),
        (1, 4096),
        (10000, 4096),
    ];
    let n = 5;
    let leader = 1;

    // generate throughput-latency plot
    let clients_per_region = vec![
        32,
        512,
        1024,
        1024 * 2,
        1024 * 4,
        1024 * 8,
        1024 * 16,
        1024 * 20,
        1024 * 24,
        1024 * 32,
        1024 * 36,
        1024 * 40,
        1024 * 44,
        1024 * 48,
        1024 * 52,
        1024 * 56,
        1024 * 60,
        1024 * 64,
    ];

    // load results
    let db = ResultsDB::load(results_dir).wrap_err("load results")?;

    for (batch_max_size, payload_size) in settings.clone() {
        let search_refine = |search: &mut Search, key_gen: KeyGen| {
            // filter by key gen payload size and batch max size in all
            // protocols
            search
                .key_gen(key_gen)
                .payload_size(payload_size)
                .batch_max_size(batch_max_size);
        };

        let path = format!(
            "plot_batching_heatmap_{}_{}.pdf",
            batch_max_size, payload_size
        );
        fantoch_plot::heatmap_plot_split(
            n,
            protocols.clone(),
            clients_per_region.clone(),
            key_gen,
            search_refine,
            leader,
            PLOT_DIR,
            &path,
            &db,
        )?;

        let style_fun = None;
        let latency_precision = LatencyPrecision::Millis;
        let x_range = None;
        let y_range = Some((100.0, 2000.0));
        let y_log_scale = true;
        let x_bbox_to_anchor = None;
        let left_margin = None;
        let width_reduction = None;
        let path =
            format!("plot_batching_{}_{}.pdf", batch_max_size, payload_size);
        let (max_throughputs, _) = fantoch_plot::throughput_latency_plot_split(
            n,
            protocols.clone(),
            search_gen,
            clients_per_region.clone(),
            key_gen,
            empty_key_gen,
            search_refine,
            style_fun,
            latency_precision,
            x_range,
            y_range,
            y_log_scale,
            x_bbox_to_anchor,
            left_margin,
            width_reduction,
            PLOT_DIR,
            &path,
            &db,
        )?;
        for (search, max_throughput) in max_throughputs {
            let name = match search.protocol {
                Protocol::FPaxos => "fpaxos",
                Protocol::NewtAtomic => "newt  ",
                _ => unreachable!(),
            };
            println!(
                "R {} f = {} bms = {:<5} ps = {:<4}: {}",
                name,
                search.f,
                search.batch_max_size.unwrap(),
                search.payload_size.unwrap(),
                max_throughput
            );
        }
    }

    // create searches
    let searches: Vec<_> = protocols
        .into_iter()
        .map(|search_gen_input| search_gen(search_gen_input))
        .collect();
    let style_fun = None;
    let path = format!("plot_batching.pdf");
    let y_range = Some((0.0, 800.0));
    fantoch_plot::batching_plot(
        searches, style_fun, n, settings, y_range, PLOT_DIR, &path, &db,
    )?;

    Ok(())
}

#[allow(dead_code)]
fn scalability_plot() -> Result<(), Report> {
    let results_dir = "../results_scalability";
    // fixed parameters
    let shard_count = 1;
    let n = 3;
    let f = 1;
    let payload_size = 100;
    let keys_per_command = 1;
    let protocol = Protocol::NewtAtomic;

    let coefficients = vec![
        0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0, 2.5, 3.0, 3.5, 4.0, 5.0, 6.0,
        7.0, 8.0, 9.0, 10.0,
    ];
    let cpus = vec![2, 4, 6, 8, 12];

    // load results
    let db = ResultsDB::load(results_dir).wrap_err("load results")?;

    // create searches
    let searches: Vec<_> = coefficients
        .into_iter()
        .map(|coefficient| {
            // create key gen
            let key_gen = KeyGen::Zipf {
                total_keys_per_shard: 1_000_000,
                coefficient,
            };
            let mut search = Search::new(n, f, protocol);
            search
                .shard_count(shard_count)
                .key_gen(key_gen)
                .keys_per_command(keys_per_command)
                .payload_size(payload_size);
            search
        })
        .collect();
    fantoch_plot::intra_machine_scalability_plot(searches, n, cpus, &db)?;

    Ok(())
}

#[allow(dead_code)]
fn partial_replication_plot() -> Result<(), Report> {
    println!(">>>>>>>> PARTIAL REPLICATION <<<<<<<<");
    let results_dir =
        "/home/vitor.enes/eurosys_results/results_partial_replication";
    // fixed parameters
    let top_coefficient = 0.5;
    let bottom_coefficient = 0.7;
    let payload_size = 100;
    let n = 3;
    let f = 1;

    // generate throughput-latency plot
    let clients_per_region = vec![
        256,
        1024,
        1024 * 2,
        1024 * 3,
        1024 * 4,
        1024 * 5,
        1024 * 6,
        1024 * 8,
        1024 * 10,
        1024 * 12,
        1024 * 16,
        1024 * 20,
        1024 * 22,
        1024 * 24,
        1024 * 32,
        1024 * 34,
        1024 * 36,
        1024 * 40,
        1024 * 44,
        1024 * 48,
        1024 * 64,
        1024 * 72,
        1024 * 80,
        1024 * 96,
        1024 * 104,
        1024 * 112,
        1024 * 128,
        1024 * 136,
        1024 * 144,
    ];

    let protocols = vec![
        (Protocol::NewtAtomic, 0),
        (Protocol::AtlasLocked, 100),
        (Protocol::AtlasLocked, 95),
        (Protocol::AtlasLocked, 50),
    ];

    let search_gen = |(protocol, read_only_percentage)| {
        let mut search = Search::new(n, f, protocol);
        search.read_only_percentage(read_only_percentage);
        search
    };

    let style_fun = |search: &Search| {
        let mut style = HashMap::new();
        match search.protocol {
            Protocol::NewtAtomic => {
                style.insert(
                    Style::Label,
                    format!("{}", PlotFmt::protocol_name(search.protocol)),
                );
            }
            Protocol::AtlasLocked => {
                let ro = search
                    .read_only_percentage
                    .expect("read-only percentage should be set in search");
                style.insert(Style::Label, format!("Janus* w = {}%", 100 - ro));

                let (protocol, f) = match ro {
                    100 => (Protocol::Basic, 2),
                    95 => (Protocol::NewtLocked, 1),
                    50 => (Protocol::NewtLocked, 2),
                    _ => panic!("unsupported read-only percentage: {:?}", ro),
                };
                style.insert(Style::Color, PlotFmt::color(protocol, f));
                style.insert(Style::Marker, PlotFmt::marker(protocol, f));
                style.insert(Style::Hatch, PlotFmt::hatch(protocol, f));
            }
            _ => panic!("unsupported protocol: {:?}", search.protocol),
        }
        style
    };

    // load results
    let db = ResultsDB::load(results_dir).wrap_err("load results")?;

    for (shard_count, keys_per_command, x_range) in vec![
        (1, 2, Some((0.0, 400.0))),
        (2, 2, Some((0.0, 400.0))),
        (4, 2, Some((0.0, 700.0))),
        (6, 2, Some((0.0, 1000.0))),
    ] {
        let search_refine = |search: &mut Search, coefficient: f64| {
            let key_gen = KeyGen::Zipf {
                coefficient,
                total_keys_per_shard: 1_000_000,
            };
            search
                .key_gen(key_gen)
                .shard_count(shard_count)
                .keys_per_command(keys_per_command)
                .payload_size(payload_size);
        };

        let latency_precision = LatencyPrecision::Millis;
        let y_range = Some((140.0, 310.0));
        let y_log_scale = false;
        let x_bbox_to_anchor = Some(0.45);
        let left_margin = Some(0.15);
        let width_reduction = Some(1.75);
        let path = format!(
            "plot_partial_replication_{}_k{}.pdf",
            shard_count, keys_per_command
        );
        let style_fun: Option<Box<dyn Fn(&Search) -> HashMap<Style, String>>> =
            Some(Box::new(style_fun));
        fantoch_plot::throughput_latency_plot_split(
            n,
            protocols.clone(),
            search_gen,
            clients_per_region.clone(),
            top_coefficient,
            bottom_coefficient,
            search_refine,
            style_fun,
            latency_precision,
            x_range,
            y_range,
            y_log_scale,
            x_bbox_to_anchor,
            left_margin,
            width_reduction,
            PLOT_DIR,
            &path,
            &db,
        )?;
    }

    // create searches
    let searches: Vec<_> = protocols
        .into_iter()
        .map(|search_gen_input| search_gen(search_gen_input))
        .collect();
    let style_fun: Option<Box<dyn Fn(&Search) -> HashMap<Style, String>>> =
        Some(Box::new(style_fun));
    let settings = vec![
        (2, 2, top_coefficient),
        (2, 2, bottom_coefficient),
        (4, 2, top_coefficient),
        (4, 2, bottom_coefficient),
        (6, 2, top_coefficient),
        (6, 2, bottom_coefficient),
    ];
    let y_range = Some((0.0, 1000.0));
    let path = format!("plot_partial_replication.pdf");
    fantoch_plot::inter_machine_scalability_plot(
        searches, style_fun, n, settings, y_range, PLOT_DIR, &path, &db,
    )?;
    Ok(())
}

#[allow(dead_code)]
fn partial_replication_all() -> Result<(), Report> {
    let results_dir = "../results_partial_replication";
    // fixed parameters
    let n = 3;
    let mut key_gens = Vec::new();
    // for coefficient in vec![0.5, 1.0] {
    for (coefficient, x_range, y_range) in vec![
        (0.5, Some((0.0, 700.0)), Some((150.0, 400.0))),
        (0.7, Some((0.0, 700.0)), Some((150.0, 400.0))),
        (1.0, None, None),
    ] {
        let key_gen = KeyGen::Zipf {
            coefficient,
            total_keys_per_shard: 1_000_000,
        };
        key_gens.push((key_gen, x_range, y_range));
    }
    let payload_size = 100;
    let protocols = vec![
        Protocol::AtlasLocked,
        Protocol::NewtLocked,
        Protocol::NewtAtomic,
    ];
    let latency_precision = LatencyPrecision::Millis;

    let shard_combinations = vec![
        // shard_count, shards_per_command
        (1, 1),
        (1, 2),
        (2, 2),
        (4, 2),
        (6, 2),
        // (8, 2),
    ];

    // load results
    let db = ResultsDB::load(results_dir).wrap_err("load results")?;

    let clients_per_region = vec![
        256,
        1024,
        1024 * 2,
        1024 * 3,
        1024 * 4,
        1024 * 5,
        1024 * 6,
        1024 * 8,
        1024 * 10,
        1024 * 12,
        1024 * 16,
        1024 * 20,
        1024 * 22,
        1024 * 24,
        1024 * 32,
        1024 * 34,
        1024 * 36,
        1024 * 40,
        1024 * 44,
        1024 * 48,
        1024 * 64,
        1024 * 72,
        1024 * 80,
        1024 * 96,
        1024 * 104,
        1024 * 112,
        1024 * 128,
        1024 * 136,
        1024 * 144,
    ];

    let search_refine = |search: &mut Search, read_only_percentage: usize| {
        match search.protocol {
            Protocol::NewtAtomic => {
                // if newt atomic, don't filter by read-only percentage as reads
                // are not treated in any special way there, and thus, it does
                // not affect the results
            }
            Protocol::AtlasLocked | Protocol::NewtLocked => {
                search.read_only_percentage(read_only_percentage);
            }
            _ => {
                panic!("unsupported protocol: {:?}", search.protocol);
            }
        }
    };

    // for read_only_percentage in vec![100, 95, 50] {
    for read_only_percentage in vec![0, 100, 95, 50] {
        for (key_gen, x_range, y_range) in key_gens.clone() {
            // generate all-combo throughput-something plot
            for y_axis in vec![
                ThroughputYAxis::Latency(LatencyMetric::Average),
                ThroughputYAxis::Latency(LatencyMetric::Percentile(0.99)),
                ThroughputYAxis::Latency(LatencyMetric::Percentile(0.999)),
                ThroughputYAxis::CPU,
            ] {
                let path = format!(
                    "throughput_{}_n{}_{}_r{}.pdf",
                    y_axis.name(),
                    n,
                    key_gen,
                    read_only_percentage
                );
                // create searches
                let searches = shard_combinations
                    .clone()
                    .into_iter()
                    .flat_map(|(shard_count, keys_per_command)| {
                        protocol_combinations(n, protocols.clone())
                            .into_iter()
                            .map(move |(protocol, f)| {
                                let mut search = Search::new(n, f, protocol);
                                search
                                    .shard_count(shard_count)
                                    .key_gen(key_gen)
                                    .keys_per_command(keys_per_command)
                                    .payload_size(payload_size);
                                search_refine(
                                    &mut search,
                                    read_only_percentage,
                                );
                                search
                            })
                    })
                    .collect();

                let style_fun: Option<
                    Box<dyn Fn(&Search) -> HashMap<Style, String>>,
                > = Some(Box::new(|search| {
                    // create styles
                    let mut styles = HashMap::new();
                    styles.insert((1, 1), ("#444444", "s"));
                    styles.insert((1, 2), ("#111111", "+"));
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
                    latency_precision,
                    n,
                    clients_per_region.clone(),
                    x_range,
                    y_range,
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
                        "throughput_{}_n{}_s{}_k{}_{}_r{}.pdf",
                        y_axis.name(),
                        n,
                        shard_count,
                        keys_per_command,
                        key_gen,
                        read_only_percentage,
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
                            search_refine(&mut search, read_only_percentage);
                            search
                        })
                        .collect();
                    let style_fun = None;
                    let max_throughputs =
                        fantoch_plot::throughput_something_plot(
                            searches,
                            style_fun,
                            latency_precision,
                            n,
                            clients_per_region.clone(),
                            x_range,
                            y_range,
                            y_axis,
                            PLOT_DIR,
                            &path,
                            &db,
                        )?;
                    println!(
                        "n = {} | s = {} | k = {} | {} | read_only = {} | max. tputs: {:?}",
                        n,
                        shard_count,
                        keys_per_command,
                        key_gen,
                        read_only_percentage,
                        max_throughputs
                    );
                }

                // generate dstat, latency and cdf plots
                for clients_per_region in clients_per_region.clone() {
                    println!(
                        "n = {} | s = {} | k = {} | {} | read_only = {} | c = {}",
                        n,
                        shard_count,
                        keys_per_command,
                        key_gen,
                        read_only_percentage,
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
                                search_refine(
                                    &mut search,
                                    read_only_percentage,
                                );
                                search
                            })
                            .collect();

                    // generate dstat table
                    for metrics_type in dstat_combinations(shard_count, n) {
                        let path = format!(
                            "dstat_{}_n{}_s{}_k{}_{}_r{}_c{}.pdf",
                            metrics_type.name(),
                            n,
                            shard_count,
                            keys_per_command,
                            key_gen,
                            read_only_percentage,
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
                    for metrics_type in
                        process_metrics_combinations(shard_count, n)
                    {
                        let path = format!(
                            "metrics_{}_n{}_s{}_k{}_{}_r{}_c{}.pdf",
                            metrics_type.name(),
                            n,
                            shard_count,
                            keys_per_command,
                            key_gen,
                            read_only_percentage,
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
                            "latency{}_n{}_s{}_k{}_{}_r{}_c{}.pdf",
                            error_bar.name(),
                            n,
                            shard_count,
                            keys_per_command,
                            key_gen,
                            read_only_percentage,
                            clients_per_region,
                        );
                        let legend_order = None;
                        let style_fun = None;
                        let results = fantoch_plot::latency_plot(
                            searches.clone(),
                            legend_order,
                            style_fun,
                            latency_precision,
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
                        "cdf_n{}_s{}_k{}_{}_r{}_c{}.pdf",
                        n,
                        shard_count,
                        keys_per_command,
                        key_gen,
                        read_only_percentage,
                        clients_per_region
                    );
                    let style_fun = None;
                    fantoch_plot::cdf_plot(
                        searches.clone(),
                        style_fun,
                        latency_precision,
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
fn multi_key_all() -> Result<(), Report> {
    let results_dir = "../results_multi_key";
    // fixed parameters
    let shard_count = 1;
    let n = 3;
    let payload_size = 100;
    let protocols = vec![Protocol::NewtAtomic];
    let latency_precision = LatencyPrecision::Micros;

    // load results
    let db = ResultsDB::load(results_dir).wrap_err("load results")?;

    let clients_per_region = vec![
        64, 128, 256, 512, 768, 1024, 1280, 1536, 2048, 2560, 3072, 3584, 4096,
    ];

    // for cpus in vec![2, 4, 6, 8, 12] {
    for cpus in vec![2] {
        for keys_per_shard in vec![1] {
            for zipf_coefficient in vec![
                0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0, 2.5, 3.0, 3.5, 4.0, 5.0,
                6.0, 7.0, 8.0, 9.0, 10.0,
            ] {
                // create key generator
                let key_gen = KeyGen::Zipf {
                    coefficient: zipf_coefficient,
                    total_keys_per_shard: 1_000_000,
                };

                // generate throughput-something plot
                for y_axis in vec![
                    ThroughputYAxis::Latency(LatencyMetric::Average),
                    ThroughputYAxis::Latency(LatencyMetric::Percentile(0.99)),
                    ThroughputYAxis::Latency(LatencyMetric::Percentile(0.999)),
                    ThroughputYAxis::CPU,
                ] {
                    let path = format!(
                        "throughput_{}_cpus{}_n{}_k{}_{}.pdf",
                        y_axis.name(),
                        cpus,
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
                                .payload_size(payload_size)
                                .cpus(cpus);
                            search
                        })
                        .collect();
                    let style_fun = None;
                    let x_range = None;
                    let y_range = None;
                    fantoch_plot::throughput_something_plot(
                        searches,
                        style_fun,
                        latency_precision,
                        n,
                        clients_per_region.clone(),
                        x_range,
                        y_range,
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
                                    .payload_size(payload_size)
                                    .cpus(cpus);
                                search
                            })
                            .collect();

                    // generate dstat table
                    for metrics_type in dstat_combinations(shard_count, n) {
                        let path = format!(
                            "dstat_{}_cpus{}_n{}_k{}_{}_c{}.pdf",
                            metrics_type.name(),
                            cpus,
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
                    for metrics_type in
                        process_metrics_combinations(shard_count, n)
                    {
                        let path = format!(
                            "metrics_{}_cpus{}_n{}_k{}_{}_c{}.pdf",
                            metrics_type.name(),
                            cpus,
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
                            "latency{}_cpus{}_n{}_k{}_{}_c{}.pdf",
                            error_bar.name(),
                            cpus,
                            n,
                            keys_per_shard,
                            key_gen,
                            clients_per_region,
                        );
                        let legend_order = None;
                        let style_fun = None;
                        let results = fantoch_plot::latency_plot(
                            searches.clone(),
                            legend_order,
                            style_fun,
                            latency_precision,
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
                        "cdf_cpus{}_n{}_k{}_{}_c{}.pdf",
                        cpus, n, keys_per_shard, key_gen, clients_per_region
                    );
                    let style_fun = None;
                    fantoch_plot::cdf_plot(
                        searches.clone(),
                        style_fun,
                        latency_precision,
                        PLOT_DIR,
                        &path,
                        &db,
                    )?;

                    if n > 3 {
                        // generate cdf plot with subplots
                        let path = format!(
                            "cdf_split_cpus{}_n{}_k{}_{}_c{}.pdf",
                            cpus,
                            n,
                            keys_per_shard,
                            key_gen,
                            clients_per_region,
                        );
                        let (top_searches, bottom_searches): (Vec<_>, Vec<_>) =
                            searches
                                .into_iter()
                                .partition(|search| search.f == 1);
                        let x_range = None;
                        let style_fun = None;
                        fantoch_plot::cdf_plot_split(
                            top_searches,
                            bottom_searches,
                            x_range,
                            style_fun,
                            latency_precision,
                            PLOT_DIR,
                            &path,
                            &db,
                        )?;
                    }
                }
            }
        }
    }

    Ok(())
}

#[allow(dead_code)]
fn single_key_all() -> Result<(), Report> {
    let results_dir =
        "/home/vitor.enes/eurosys_results/results_increasing_load";
    // fixed parameters
    let shard_count = 1;
    let key_gens = vec![
        KeyGen::ConflictPool {
            conflict_rate: 2,
            pool_size: 1,
        },
        // KeyGen::ConflictPool {
        //     conflict_rate: 10,
        //     pool_size: 1,
        // },
    ];
    let payload_size = 4096;
    let batch_max_size = 1;
    let protocols = vec![
        Protocol::NewtAtomic,
        Protocol::AtlasLocked,
        Protocol::EPaxosLocked,
        Protocol::FPaxos,
    ];
    let leader = 1;
    let latency_precision = LatencyPrecision::Millis;

    // generate throughput-latency plot
    let clients_per_region = vec![
        32,
        256,
        512,
        1024,
        1024 * 2,
        1024 * 4,
        1024 * 8,
        1024 * 12,
        1024 * 16,
        1024 * 20,
        1024 * 24,
        1024 * 32,
        1024 * 40,
        1024 * 48,
        1024 * 56,
        1024 * 60,
        1024 * 64,
    ];

    // load results
    let db = ResultsDB::load(results_dir).wrap_err("load results")?;

    for n in vec![5] {
        for key_gen in key_gens.clone() {
            let search_refine = |search: &mut Search, key_gen: KeyGen| {
                match search.protocol {
                    Protocol::FPaxos => {
                        // if fpaxos, don't filter by key gen as
                        // contention does not affect the results
                    }
                    Protocol::AtlasLocked
                    | Protocol::NewtAtomic
                    | Protocol::EPaxosLocked
                    | Protocol::CaesarLocked => {
                        search.key_gen(key_gen);
                    }
                    _ => {
                        panic!("unsupported protocol: {:?}", search.protocol);
                    }
                }
                // filter by payload size in all protocols
                search
                    .payload_size(payload_size)
                    .batch_max_size(batch_max_size);
            };

            // create searches
            let searches: Vec<_> = protocol_combinations(n, protocols.clone())
                .into_iter()
                .map(|(protocol, f)| {
                    let mut search = Search::new(n, f, protocol);
                    search_refine(&mut search, key_gen);
                    search
                })
                .collect();

            // generate all-combo throughput-something plot
            for y_axis in vec![
                ThroughputYAxis::Latency(LatencyMetric::Average),
                ThroughputYAxis::Latency(LatencyMetric::Percentile(0.99)),
                ThroughputYAxis::Latency(LatencyMetric::Percentile(0.999)),
                ThroughputYAxis::CPU,
            ] {
                let path = format!(
                    "throughput_{}_n{}_{}.pdf",
                    y_axis.name(),
                    n,
                    key_gen
                );

                let style_fun = None;
                let x_range = None;
                let y_range = None;
                fantoch_plot::throughput_something_plot(
                    searches.clone(),
                    style_fun,
                    latency_precision,
                    n,
                    clients_per_region.clone(),
                    x_range,
                    y_range,
                    y_axis,
                    PLOT_DIR,
                    &path,
                    &db,
                )?;
            }

            for heatmap_metric in vec![
                HeatmapMetric::CPU,
                HeatmapMetric::NetSend,
                HeatmapMetric::NetRecv,
            ] {
                let path = format!(
                    "heatmap_{}_n{}_{}.pdf",
                    heatmap_metric.name(),
                    n,
                    key_gen
                );

                fantoch_plot::heatmap_plot(
                    n,
                    protocol_combinations(n, protocols.clone()),
                    clients_per_region.clone(),
                    key_gen,
                    search_refine,
                    leader,
                    heatmap_metric,
                    PLOT_DIR,
                    &path,
                    &db,
                )?;
            }

            // generate dstat, latency and cdf plots
            for clients_per_region in clients_per_region.clone() {
                println!(
                    "n = {} | {} | c = {}",
                    n, key_gen, clients_per_region
                );

                // create searches
                let searches: Vec<_> =
                    protocol_combinations(n, protocols.clone())
                        .into_iter()
                        .map(|(protocol, f)| {
                            let mut search = Search::new(n, f, protocol);
                            match protocol {
                                Protocol::FPaxos => {
                                    // if fpaxos, don't filter by key gen as
                                    // contention does not affect the results
                                }
                                Protocol::AtlasLocked
                                | Protocol::NewtAtomic
                                | Protocol::EPaxosLocked
                                | Protocol::CaesarLocked => {
                                    search.key_gen(key_gen);
                                }
                                _ => {
                                    panic!(
                                        "unsupported protocol: {:?}",
                                        protocol
                                    );
                                }
                            }
                            // filter by clients per region and payload size in
                            // all protocols
                            search
                                .clients_per_region(clients_per_region)
                                .payload_size(payload_size)
                                .batch_max_size(batch_max_size);
                            search
                        })
                        .collect();

                // generate dstat table
                for metrics_type in dstat_combinations(shard_count, n) {
                    let path = format!(
                        "dstat_{}_n{}_{}_c{}.pdf",
                        metrics_type.name(),
                        n,
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
                        "metrics_{}_n{}_{}_c{}.pdf",
                        metrics_type.name(),
                        n,
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
                /*
                let mut shown = false;
                for error_bar in vec![
                    ErrorBar::Without,
                    ErrorBar::With(0.99),
                    ErrorBar::With(0.999),
                ] {
                    let path = format!(
                        "latency{}_n{}_{}_c{}.pdf",
                        error_bar.name(),
                        n,
                        key_gen,
                        clients_per_region,
                    );
                    let legend_order = None;
                    let style_fun = None;
                    let results = fantoch_plot::latency_plot(
                        searches.clone(),
                        legend_order,
                        style_fun,
                        latency_precision,
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
                */

                // create searches without fpaxos for cdf plots
                let protocols = vec![
                    Protocol::NewtAtomic,
                    Protocol::AtlasLocked,
                    Protocol::EPaxosLocked,
                ];
                let searches: Vec<_> =
                    protocol_combinations(n, protocols.clone())
                        .into_iter()
                        .map(|(protocol, f)| {
                            let mut search = Search::new(n, f, protocol);
                            search
                                .key_gen(key_gen)
                                .clients_per_region(clients_per_region)
                                .payload_size(payload_size)
                                .batch_max_size(batch_max_size);
                            search
                        })
                        .collect();

                // generate cdf plot
                let path = format!(
                    "cdf_n{}_{}_c{}.pdf",
                    n, key_gen, clients_per_region
                );
                let style_fun = None;
                fantoch_plot::cdf_plot(
                    searches.clone(),
                    style_fun,
                    latency_precision,
                    PLOT_DIR,
                    &path,
                    &db,
                )?;

                if n > 3 {
                    // generate cdf plot with subplots
                    let path = format!(
                        "cdf_one_per_f_n{}_{}_c{}.pdf",
                        n, key_gen, clients_per_region
                    );
                    let (top_searches, bottom_searches): (Vec<_>, Vec<_>) =
                        searches.into_iter().partition(|search| search.f == 1);
                    let x_range = None;
                    let style_fun = None;
                    fantoch_plot::cdf_plot_split(
                        top_searches,
                        bottom_searches,
                        x_range,
                        style_fun,
                        latency_precision,
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
    protocols: Vec<Protocol>,
) -> Vec<(Protocol, usize)> {
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
