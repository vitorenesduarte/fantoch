use color_eyre::eyre::WrapErr;
use color_eyre::Report;
use fantoch::client::KeyGen;
use fantoch::planet::{Planet, Region};
use fantoch_exp::Protocol;
use fantoch_plot::{
    ErrorBar, ExperimentData, HeatmapMetric, LatencyMetric, LatencyPrecision,
    MetricsType, PlotFmt, ResultsDB, Search, Style, ThroughputYAxis,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;

// latency dir
// const LATENCY_AWS: &str = "../latency_aws/2021_02_13";
const LATENCY_AWS: &str = "../latency_aws/2020_06_05";

// folder where all plots will be stored
const PLOT_DIR: Option<&str> = Some("plots");

// if true, dstats per process will be generated
const ALL_DSTATS: bool = true;

fn main() -> Result<(), Report> {
    // set global style
    let newsgott = true;
    fantoch_plot::set_global_style(newsgott)?;

    // partial_replication_all()?;
    // multi_key()?;
    // single_key_all()?;
    show_distance_matrix();
    thesis()?;
    Ok(())
}

#[allow(dead_code)]
fn thesis() -> Result<(), Report> {
    eurosys()?;
    fast_path_plot()?;
    increasing_sites_plot()?;
    nfr_plot()?;
    recovery_plot()?;
    Ok(())
}

#[allow(dead_code)]
fn eurosys() -> Result<(), Report> {
    fairness_plot()?;
    tail_latency_plot()?;
    increasing_load_plot()?;
    batching_plot()?;
    partial_replication_plot()?;
    Ok(())
}

#[derive(Default)]
struct RecoveryData {
    taiwan: Vec<u64>,
    finland: Vec<u64>,
    south_carolina: Vec<u64>,
    total: Vec<u64>,
}

#[allow(dead_code)]
fn recovery_plot() -> Result<(), Report> {
    println!(">>>>>>>> RECOVERY <<<<<<<<");

    let atlas_data = recovery_data("eurosys20_data/recovery/atlas.dat")?;
    let fpaxos_data = recovery_data("eurosys20_data/recovery/fpaxos.dat")?;
    let taiwan = (atlas_data.taiwan, fpaxos_data.taiwan);
    let finland = (atlas_data.finland, fpaxos_data.finland);
    let south_carolina =
        (atlas_data.south_carolina, fpaxos_data.south_carolina);
    let total = (atlas_data.total, fpaxos_data.total);

    let path = String::from("plot_recovery.pdf");
    fantoch_plot::recovery_plot(
        taiwan,
        finland,
        south_carolina,
        total,
        PLOT_DIR,
        &path,
    )?;

    Ok(())
}

fn recovery_data(path: &str) -> Result<RecoveryData, Report> {
    #[derive(Debug, Deserialize)]
    struct Record {
        #[allow(dead_code)]
        time: u64,
        taiwan: u64,
        finland: u64,
        south_carolina: u64,
        total: u64,
    }

    let file = File::open(path)?;
    let buf = BufReader::new(file);
    let mut rdr = csv::ReaderBuilder::new().delimiter(b' ').from_reader(buf);
    let mut recovery_data = RecoveryData::default();

    for result in rdr.deserialize() {
        let record: Record = result?;
        recovery_data.taiwan.push(record.taiwan);
        recovery_data.finland.push(record.finland);
        recovery_data.south_carolina.push(record.south_carolina);
        recovery_data.total.push(record.total);
    }

    Ok(recovery_data)
}

#[allow(dead_code)]
fn nfr_plot() -> Result<(), Report> {
    println!(">>>>>>>> NFR <<<<<<<<");
    let results_dir = "/home/mari/thesis_results/results_nfr";
    // fixed parameters
    let key_gen = KeyGen::Zipf {
        total_keys_per_shard: 1_000_000,
        coefficient: 0.99,
    };
    let payload_size = 100;
    let protocols = vec![
        (Protocol::TempoAtomic, Some(1)),
        (Protocol::AtlasLocked, Some(1)),
        (Protocol::TempoAtomic, Some(2)),
        (Protocol::AtlasLocked, Some(2)),
        (Protocol::EPaxosLocked, None),
    ];
    let legend_order = vec![0, 2, 1, 3, 4];
    let ns = vec![7];
    let read_only_percentages = vec![20, 50, 80, 100];
    let clients_per_region = 256;

    // load results
    let db = ResultsDB::load(results_dir).wrap_err("load results")?;

    for n in ns {
        // generate plot
        let path = format!("plot_nfr_n{}.pdf", n);
        let style_fun = None;
        let latency_precision = LatencyPrecision::Millis;
        fantoch_plot::nfr_plot(
            n,
            read_only_percentages.clone(),
            protocols.clone(),
            key_gen,
            clients_per_region,
            payload_size,
            Some(legend_order.clone()),
            style_fun,
            latency_precision,
            PLOT_DIR,
            &path,
            &db,
        )?;
    }
    Ok(())
}

#[allow(dead_code)]
fn fast_path_plot() -> Result<(), Report> {
    println!(">>>>>>>> FAST PATH <<<<<<<<");
    //FIXMe:
    let results_dir = "/home/mari/ola";
    // fixed parameters
    let conflict_rates = vec![0, 5, 10, 20, 40, 60, 80, 100];
    let payload_size = 100;
    let batch_max_size = 1;
    let clients_per_region = vec![1, 8];

    let search_refine = |search: &mut Search,
                         clients_per_region: usize,
                         conflict_rate: usize| {
        let key_gen = KeyGen::ConflictPool {
            conflict_rate,
            pool_size: 1,
        };
        search
            .clients_per_region(clients_per_region)
            .key_gen(key_gen)
            .payload_size(payload_size)
            .batch_max_size(batch_max_size);
    };

    // tuple with protocol and f
    let protocols_n5 = vec![
        (Protocol::TempoAtomic, 2),
        (Protocol::AtlasLocked, 2),
        (Protocol::EPaxosLocked, 2),
    ];

    let protocols_n7 = vec![
        (Protocol::TempoAtomic, 2),
        (Protocol::TempoAtomic, 3),
        (Protocol::AtlasLocked, 2),
        (Protocol::AtlasLocked, 3),
        (Protocol::EPaxosLocked, 3),
    ];

    // change basic to worst case
    let style_fun = |search: &Search| {
        let mut style = HashMap::new();
        if search.protocol == Protocol::Basic {
            style.insert(Style::Label, "worst-case".to_string());
        }
        style
    };

    // load results
    let db = ResultsDB::load(results_dir).wrap_err("load results")?;

    // generate fast path plots
    for (n, protocols) in vec![(5, protocols_n5), (7, protocols_n7)] {
        // create searches
        let searches: Vec<_> = protocols
            .into_iter()
            .map(|(protocol, f)| Search::new(n, f, protocol))
            .collect();

        for clients_per_region in clients_per_region.clone() {
            let path =
                format!("plot_fast_path_n{}_c{}.pdf", n, clients_per_region);
            fantoch_plot::fast_path_plot(
                searches.clone(),
                clients_per_region,
                conflict_rates.clone(),
                search_refine,
                Some(Box::new(style_fun)),
                PLOT_DIR,
                &path,
                &db,
            )?;
        }
    }

    Ok(())
}

#[allow(dead_code)]
fn increasing_sites_plot() -> Result<(), Report> {
    println!(">>>>>>>> INCREASING SITES <<<<<<<<");
    let results_dir =
        "/home/mari/thesis_results/results_increasing_sites";
    // fixed parameters
    let key_gen = KeyGen::ConflictPool {
        conflict_rate: 2,
        pool_size: 1,
    };
    let payload_size = 100;
    let protocols = vec![
        (Protocol::TempoAtomic, Some(1)),
        (Protocol::AtlasLocked, Some(1)),
        (Protocol::FPaxos, Some(1)),
        (Protocol::TempoAtomic, Some(2)),
        (Protocol::AtlasLocked, Some(2)),
        (Protocol::FPaxos, Some(2)),
        (Protocol::EPaxosLocked, None),
    ];
    let legend_order = vec![0, 2, 4, 1, 3, 5, 6];
    let ns = vec![3, 5, 7, 9, 11];
    let clients_per_region = 256;
    let error_bar = ErrorBar::Without;

    // load results
    let db = ResultsDB::load(results_dir).wrap_err("load results")?;

    // generate plot
    let path = String::from("plot_increasing_sites.pdf");
    let style_fun = None;
    let latency_precision = LatencyPrecision::Millis;
    fantoch_plot::increasing_sites_plot(
        ns,
        protocols,
        key_gen,
        clients_per_region,
        payload_size,
        Some(legend_order),
        style_fun,
        latency_precision,
        error_bar,
        PLOT_DIR,
        &path,
        &db,
    )?;
    Ok(())
}

#[allow(dead_code)]
fn fairness_plot() -> Result<(), Report> {
    println!(">>>>>>>> FAIRNESS <<<<<<<<");
    //FIXME:
    let results_dir =
        "/home/mari/eurosys_results/results_fairness_and_tail_latency";
    // fixed parameters
    let key_gen = KeyGen::ConflictPool {
        conflict_rate: 2,
        pool_size: 1,
    };
    let payload_size = 100;
    let protocols = vec![
        (Protocol::TempoAtomic, 1),
        (Protocol::AtlasLocked, 1),
        (Protocol::FPaxos, 1),
        (Protocol::TempoAtomic, 2),
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
                | Protocol::TempoAtomic
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
    let results = fantoch_plot::fairness_plot(
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
    //FIXME:
    let results_dir =
        "/home/mari/eurosys_results/results_fairness_and_tail_latency";
    // fixed parameters
    let key_gen = KeyGen::ConflictPool {
        conflict_rate: 2,
        pool_size: 1,
    };
    let payload_size = 100;
    let protocols = vec![
        (Protocol::TempoAtomic, 1),
        (Protocol::TempoAtomic, 2),
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
    let y_bbox_to_anchor = Some(1.56);
    // increase height
    let height_adjust = Some(1.5);
    fantoch_plot::cdf_plot_split(
        top_searches,
        bottom_searches,
        x_range,
        style_fun,
        latency_precision,
        y_bbox_to_anchor,
        height_adjust,
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
        "/home/mari/eurosys_results/results_fairness_and_tail_latency";

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

    // load results
    let db = ResultsDB::load(results_dir).wrap_err("load results")?;

    let search_refine = |search: &mut Search, key_gen: KeyGen| {
        match search.protocol {
            Protocol::FPaxos => {
                // if fpaxos, don't filter by key gen as
                // contention does not affect the results
            }
            Protocol::AtlasLocked
            | Protocol::TempoAtomic
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
        (Protocol::TempoAtomic, 1),
        (Protocol::TempoAtomic, 2),
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

    // adjust Caesar name to Caesar*
    let style_fun = |search: &Search| {
        let mut style = HashMap::new();
        if search.protocol == Protocol::CaesarLocked {
            style.insert(
                Style::Label,
                format!("{}*", PlotFmt::protocol_name(search.protocol)),
            );
        }
        style
    };

    let path = format!("plot_increasing_load_heatmap_{}.pdf", top_key_gen);
    fantoch_plot::heatmap_plot_split(
        n,
        protocols.clone(),
        clients_per_region.clone(),
        top_key_gen,
        search_refine,
        Some(Box::new(style_fun)),
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
        Some(Box::new(style_fun)),
        leader,
        PLOT_DIR,
        &path,
        &db,
    )?;

    let search_gen = |(protocol, f)| Search::new(n, f, protocol);
    let latency_precision = LatencyPrecision::Millis;
    let x_range = None;
    let y_range = Some((100.0, 1500.0));
    let y_axis = ThroughputYAxis::Latency(LatencyMetric::Average);
    let y_log_scale = true;
    let x_bbox_to_anchor = Some(0.46);
    let y_bbox_to_anchor = Some(1.42);
    let legend_column_spacing = Some(1.25);
    let left_margin = None;
    let width_adjust = None;
    let height_adjust = Some(1.0);
    let path = String::from("plot_increasing_load.pdf");
    fantoch_plot::throughput_something_plot_split(
        n,
        protocols,
        search_gen,
        clients_per_region,
        top_key_gen,
        bottom_key_gen,
        search_refine,
        Some(Box::new(style_fun)),
        latency_precision,
        x_range,
        y_range,
        y_axis,
        y_log_scale,
        x_bbox_to_anchor,
        y_bbox_to_anchor,
        legend_column_spacing,
        left_margin,
        width_adjust,
        height_adjust,
        PLOT_DIR,
        &path,
        &db,
    )?;

    Ok(())
}

#[allow(dead_code)]
fn batching_plot() -> Result<(), Report> {
    println!(">>>>>>>> BATCHING <<<<<<<<");
    let results_dir = "/home/mari/eurosys_results/results_batching";

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
    let tempo = (Protocol::TempoAtomic, 1);
    let fpaxos = (Protocol::FPaxos, 1);
    let protocols = vec![tempo, fpaxos];
    let tempo_batch_max_size = 10000;
    let fpaxos_batch_max_size = 10000;
    let search_gen = |(protocol, f)| Search::new(n, f, protocol);

    let settings = vec![
        // (batching, payload_size)
        (false, 256),
        (true, 256),
        (false, 1024),
        (true, 1024),
        (false, 4096),
        (true, 4096),
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
        1024 * 28,
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

    for (batching, payload_size) in settings.clone() {
        let search_refine = |search: &mut Search, key_gen: KeyGen| {
            // filter by key gen payload size and batch max size in all
            // protocols
            search.key_gen(key_gen).payload_size(payload_size);
            // set batch max size if batching
            let batch_max_size = if batching {
                match search.protocol {
                    Protocol::TempoAtomic => tempo_batch_max_size,
                    Protocol::FPaxos => fpaxos_batch_max_size,
                    _ => panic!("unsupported protocol: {:?}", search.protocol),
                }
            } else {
                1
            };
            search.batch_max_size(batch_max_size);
        };

        let path =
            format!("plot_batching_heatmap_{}_{}.pdf", batching, payload_size);
        let style_fun = None;
        fantoch_plot::heatmap_plot_split(
            n,
            protocols.clone(),
            clients_per_region.clone(),
            key_gen,
            search_refine,
            style_fun,
            leader,
            PLOT_DIR,
            &path,
            &db,
        )?;

        let style_fun = None;
        let latency_precision = LatencyPrecision::Millis;
        let x_range = None;
        let y_range = Some((100.0, 2000.0));
        let y_axis = ThroughputYAxis::Latency(LatencyMetric::Average);
        let y_log_scale = true;
        let x_bbox_to_anchor = None;
        let y_bbox_to_anchor = None;
        let legend_column_spacing = None;
        let left_margin = None;
        let width_adjust = None;
        let height_adjust = None;
        let path = format!("plot_batching_{}_{}.pdf", batching, payload_size);
        let (max_throughputs, _) =
            fantoch_plot::throughput_something_plot_split(
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
                y_axis,
                y_log_scale,
                x_bbox_to_anchor,
                y_bbox_to_anchor,
                legend_column_spacing,
                left_margin,
                width_adjust,
                height_adjust,
                PLOT_DIR,
                &path,
                &db,
            )?;
        for (search, max_throughput) in max_throughputs {
            let name = match search.protocol {
                Protocol::FPaxos => "fpaxos",
                Protocol::TempoAtomic => "tempo ",
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
    let searches: Vec<_> = vec![
        (tempo, tempo_batch_max_size),
        (fpaxos, fpaxos_batch_max_size),
    ]
    .into_iter()
    .map(|(search_gen_input, batch_max_size)| {
        (search_gen(search_gen_input), batch_max_size)
    })
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
    let protocol = Protocol::TempoAtomic;

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
        "/home/mari/eurosys_results/results_partial_replication";
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
        1024 * 52,
        1024 * 64,
        1024 * 72,
        1024 * 80,
        1024 * 88,
        1024 * 96,
        1024 * 104,
        1024 * 112,
        1024 * 128,
        1024 * 136,
        1024 * 144,
    ];

    let protocols = vec![
        (Protocol::TempoAtomic, 0),
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
            Protocol::TempoAtomic => {
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
                    95 => (Protocol::TempoLocked, 1),
                    50 => (Protocol::TempoLocked, 2),
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

    for (y_axis, y_range) in vec![
        (
            ThroughputYAxis::Latency(LatencyMetric::Average),
            Some((150.0, 310.0)),
        ),
        (
            ThroughputYAxis::Latency(LatencyMetric::Percentile(0.99)),
            Some((150.0, 810.0)),
        ),
        (
            ThroughputYAxis::Latency(LatencyMetric::Percentile(0.999)),
            Some((150.0, 810.0)),
        ),
    ] {
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
            let y_log_scale = false;
            let x_bbox_to_anchor = Some(0.45);
            let y_bbox_to_anchor = None;
            let legend_column_spacing = None;
            let left_margin = Some(0.15);
            let width_adjust = Some(-1.75);
            let height_adjust = None;
            let path = format!(
                "plot_partial_replication_{}_{}_k{}.pdf",
                y_axis.name(),
                shard_count,
                keys_per_command
            );
            let style_fun: Option<
                Box<dyn Fn(&Search) -> HashMap<Style, String>>,
            > = Some(Box::new(style_fun));
            fantoch_plot::throughput_something_plot_split(
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
                y_axis,
                y_log_scale,
                x_bbox_to_anchor,
                y_bbox_to_anchor,
                legend_column_spacing,
                left_margin,
                width_adjust,
                height_adjust,
                PLOT_DIR,
                &path,
                &db,
            )?;
        }
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
    for (coefficient, x_range, y_range) in vec![
        (0.5, Some((0.0, 700.0)), Some((150.0, 400.0))),
        (0.7, Some((0.0, 700.0)), Some((150.0, 400.0))),
    ] {
        let key_gen = KeyGen::Zipf {
            coefficient,
            total_keys_per_shard: 1_000_000,
        };
        key_gens.push((key_gen, x_range, y_range));
    }
    let payload_size = 100;
    let protocols = vec![Protocol::AtlasLocked, Protocol::TempoAtomic];
    let latency_precision = LatencyPrecision::Millis;

    let shard_combinations = vec![
        // shard_count, shards_per_command
        // (1, 1),
        // (1, 2),
        (2, 2),
        (4, 2),
        (6, 2),
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
        1024 * 52,
        1024 * 64,
        1024 * 72,
        1024 * 80,
        1024 * 88,
        1024 * 96,
        1024 * 104,
        1024 * 112,
        1024 * 128,
        1024 * 136,
        1024 * 144,
    ];

    let search_refine = |search: &mut Search, read_only_percentage: usize| {
        match search.protocol {
            Protocol::TempoAtomic => {
                // if tempo atomic, don't filter by read-only percentage as
                // reads are not treated in any special way
                // there, and thus, it does not affect the
                // results
            }
            Protocol::AtlasLocked | Protocol::TempoLocked => {
                search.read_only_percentage(read_only_percentage);
            }
            _ => {
                panic!("unsupported protocol: {:?}", search.protocol);
            }
        }
    };

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
                        let results = fantoch_plot::fairness_plot(
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
    let protocols = vec![Protocol::TempoAtomic];
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
                        let results = fantoch_plot::fairness_plot(
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
                        let y_bbox_to_anchor = None;
                        let height_adjust = None;
                        fantoch_plot::cdf_plot_split(
                            top_searches,
                            bottom_searches,
                            x_range,
                            style_fun,
                            latency_precision,
                            y_bbox_to_anchor,
                            height_adjust,
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
    let results_dir = "../results_fast_path";
    // fixed parameters
    let shard_count = 1;
    let conflict_rates = vec![0, 5, 10, 20, 40, 60, 80, 100];
    let key_gens: Vec<_> = conflict_rates
        .into_iter()
        .map(|conflict_rate| KeyGen::ConflictPool {
            conflict_rate,
            pool_size: 1,
        })
        .collect();
    let batch_max_sizes = vec![1];
    let payload_sizes = vec![100];
    let protocols = vec![
        Protocol::TempoAtomic,
        Protocol::AtlasLocked,
        Protocol::EPaxosLocked,
        Protocol::FPaxos,
        Protocol::CaesarLocked,
    ];
    let leader = 1;
    let latency_precision = LatencyPrecision::Millis;

    // generate throughput-latency plot
    let clients_per_region = vec![
        1,  // 8
        16, // 32,
        64, // 128,
        256,
        // 512,
        1024,
        // 1024 * 2,
        // 1024 * 4,
        // 1024 * 6,
        // 1024 * 8,
        // 1024 * 12,
        // 1024 * 16,
        // 1024 * 20,
        // 1024 * 24,
        // 1024 * 28,
        // 1024 * 32,
        // 1024 * 40,
        // 1024 * 44,
        // 1024 * 48,
        // 1024 * 52,
        // 1024 * 56,
        // 1024 * 60,
        // 1024 * 64,
    ];
    let ns = vec![5, 7];

    // load results
    let db = ResultsDB::load(results_dir).wrap_err("load results")?;

    for n in ns {
        for key_gen in key_gens.clone() {
            for batch_max_size in batch_max_sizes.clone() {
                for payload_size in payload_sizes.clone() {
                    println!("--------------------------------------------");
                    println!(
                        "n = {} | {} | b = {} | p = {}",
                        n, key_gen, batch_max_size, payload_size,
                    );
                    let search_refine =
                        |search: &mut Search, key_gen: KeyGen| {
                            match search.protocol {
                                Protocol::FPaxos => {
                                    // if fpaxos, don't filter by key gen as
                                    // contention does not affect the results
                                }
                                Protocol::AtlasLocked
                                | Protocol::TempoAtomic
                                | Protocol::EPaxosLocked
                                | Protocol::CaesarLocked => {
                                    search.key_gen(key_gen);
                                }
                                _ => {
                                    panic!(
                                        "unsupported protocol: {:?}",
                                        search.protocol
                                    );
                                }
                            }
                            // filter by payload size in all protocols
                            search
                                .payload_size(payload_size)
                                .batch_max_size(batch_max_size);
                        };

                    // create searches
                    let searches: Vec<_> =
                        protocol_combinations(n, protocols.clone())
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
                        ThroughputYAxis::Latency(LatencyMetric::Percentile(
                            0.99,
                        )),
                        ThroughputYAxis::Latency(LatencyMetric::Percentile(
                            0.999,
                        )),
                        ThroughputYAxis::CPU,
                    ] {
                        let path = format!(
                            "throughput_{}_n{}_{}_b{}_p{}.pdf",
                            y_axis.name(),
                            n,
                            key_gen,
                            batch_max_size,
                            payload_size,
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
                            "heatmap_{}_n{}_{}_b{}_p{}.pdf",
                            heatmap_metric.name(),
                            n,
                            key_gen,
                            batch_max_size,
                            payload_size,
                        );

                        let style_fun = None;
                        fantoch_plot::heatmap_plot(
                            n,
                            protocol_combinations(n, protocols.clone()),
                            clients_per_region.clone(),
                            key_gen,
                            search_refine,
                            style_fun,
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
                            "n = {} | {} | b = {} | p = {} | c = {}",
                            n,
                            key_gen,
                            batch_max_size,
                            payload_size,
                            clients_per_region
                        );

                        // create searches
                        let searches: Vec<_> =
                            protocol_combinations(n, protocols.clone())
                                .into_iter()
                                .map(|(protocol, f)| {
                                    let mut search =
                                        Search::new(n, f, protocol);
                                    match protocol {
                                        Protocol::FPaxos => {
                                            // if fpaxos, don't filter by key
                                            // gen as
                                            // contention does not affect the
                                            // results
                                        }
                                        Protocol::AtlasLocked
                                        | Protocol::TempoAtomic
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
                                    // filter by clients per region and payload
                                    // size in
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
                                "dstat_{}_n{}_{}_b{}_p{}_c{}.pdf",
                                metrics_type.name(),
                                n,
                                key_gen,
                                batch_max_size,
                                payload_size,
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
                                "metrics_{}_n{}_{}_b{}_p{}_c{}.pdf",
                                metrics_type.name(),
                                n,
                                key_gen,
                                batch_max_size,
                                payload_size,
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
                            Protocol::TempoAtomic,
                            Protocol::AtlasLocked,
                            Protocol::EPaxosLocked,
                            Protocol::CaesarLocked,
                        ];
                        let searches: Vec<_> =
                            protocol_combinations(n, protocols.clone())
                                .into_iter()
                                .map(|(protocol, f)| {
                                    let mut search =
                                        Search::new(n, f, protocol);
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
                            "cdf_n{}_{}_b{}_p{}_c{}.pdf",
                            n,
                            key_gen,
                            batch_max_size,
                            payload_size,
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

                        if n == 5 {
                            // generate cdf plot with subplots
                            let path = format!(
                                "cdf_one_per_f_n{}_{}_b{}_p{}_c{}.pdf",
                                n,
                                key_gen,
                                batch_max_size,
                                payload_size,
                                clients_per_region
                            );
                            let (top_searches, bottom_searches): (
                                Vec<_>,
                                Vec<_>,
                            ) = searches
                                .into_iter()
                                .partition(|search| search.f == 1);
                            let x_range = None;
                            let style_fun = None;
                            let y_bbox_to_anchor = None;
                            let height_adjust = None;
                            fantoch_plot::cdf_plot_split(
                                top_searches,
                                bottom_searches,
                                x_range,
                                style_fun,
                                latency_precision,
                                y_bbox_to_anchor,
                                height_adjust,
                                PLOT_DIR,
                                &path,
                                &db,
                            )?;
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

#[allow(dead_code)]
fn show_distance_matrix() {
    // show distance matrix
    let planet = Planet::from(LATENCY_AWS);
    let regions = vec![
        Region::new("eu-west-1"),
        Region::new("us-west-1"),
        Region::new("ap-southeast-1"),
        Region::new("ca-central-1"),
        Region::new("sa-east-1"),
        Region::new("ap-east-1"),
        Region::new("us-east-1"),
        Region::new("ap-northeast-1"),
        Region::new("eu-north-1"),
        Region::new("ap-south-1"),
        Region::new("us-west-2"),
    ];
    println!("{}", planet.distance_matrix(regions).unwrap());
}

fn protocol_combinations(
    n: usize,
    protocols: Vec<Protocol>,
) -> Vec<(Protocol, usize)> {
    let max_f = n / 2;

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
