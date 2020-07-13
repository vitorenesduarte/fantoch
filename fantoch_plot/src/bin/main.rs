use color_eyre::eyre::WrapErr;
use color_eyre::Report;
use fantoch::client::KeyGen;
use fantoch::metrics::Histogram;
use fantoch::planet::{Planet, Region};
use fantoch_exp::Protocol;
use fantoch_plot::{
    ErrorBar, ExperimentData, LatencyMetric, PlotFmt, ResultsDB, Search,
};

// folder where all results are stored
const RESULTS_DIR: &str = "../results_multikey_maxtput";

fn main() -> Result<(), Report> {
    multi_key()?;
    // single_key()?;
    // show_distance_matrix();
    Ok(())
}

#[allow(dead_code)]
fn multi_key() -> Result<(), Report> {
    // set global style
    fantoch_plot::set_global_style()?;

    // fixed parameters
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
    let mut db = ResultsDB::load(RESULTS_DIR).wrap_err("load results")?;

    for keys_per_command in vec![8, 16, 32] {
        for zipf_coefficient in vec![1.0] {
            // create key generator
            let key_gen = KeyGen::Zipf {
                coefficient: zipf_coefficient,
                key_count: zipf_key_count,
            };

            // generate throughput-latency plot
            let clients_per_region =
                vec![256, 1024, 1024 * 4, 1024 * 8, 1024 * 16];

            for latency in vec![
                LatencyMetric::Average,
                LatencyMetric::Percentile(0.99),
                LatencyMetric::Percentile(0.999),
            ] {
                let suffix =
                    if let LatencyMetric::Percentile(percentile) = latency {
                        format!("_p{}", percentile * 100f64)
                    } else {
                        String::from("")
                    };
                let path = format!(
                    "throughput_latency_n{}_k{}_zipf{}{}.pdf",
                    n, keys_per_command, zipf_coefficient, suffix
                );
                // create searches
                let searches = protocol_combinations(n, protocols.clone())
                    .into_iter()
                    .map(|(protocol, f)| {
                        let mut search = Search::new(n, f, protocol);
                        search
                            .key_gen(key_gen)
                            .keys_per_command(keys_per_command)
                            .payload_size(payload_size);
                        search
                    })
                    .collect();
                fantoch_plot::throughput_latency_plot(
                    searches,
                    n,
                    clients_per_region.clone(),
                    latency,
                    &path,
                    &mut db,
                )?;
            }

            // generate latency plots
            for clients_per_region in
                vec![32, 256, 1024, 1024 * 4, 1024 * 8, 1024 * 16]
            {
                println!(
                    "n = {} | k = {} | c = {} | zipf = {}",
                    n, keys_per_command, clients_per_region, zipf_coefficient,
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
                                .keys_per_command(keys_per_command)
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
                    let suffix = if let ErrorBar::With(percentile) = error_bar {
                        format!("_p{}", percentile * 100f64)
                    } else {
                        String::from("")
                    };
                    let path = format!(
                        "latency_n{}_c{}_k{}_zipf{}{}.pdf",
                        n,
                        clients_per_region,
                        keys_per_command,
                        zipf_coefficient,
                        suffix
                    );
                    let results = fantoch_plot::latency_plot(
                        searches.clone(),
                        n,
                        error_bar,
                        &path,
                        &mut db,
                        extract_from_exp_data,
                    )?;

                    if !shown {
                        // only show results once
                        for (search, histogram) in results {
                            println!(
                                "{:<7} f = {} | {:?}",
                                PlotFmt::protocol_name(search.protocol),
                                search.f,
                                histogram
                            );
                        }
                        shown = true;
                    }
                }

                // generate cdf plot
                let path = format!(
                    "cdf_n{}_c{}_k{}_zipf{}.pdf",
                    n, clients_per_region, keys_per_command, zipf_coefficient,
                );
                fantoch_plot::cdf_plot(searches.clone(), &path, &mut db)?;

                if n > 3 {
                    // generate cdf plot with subplots
                    let path = format!(
                        "cdf_one_per_f_n{}_c{}_k{}_zipf{}.pdf",
                        n,
                        clients_per_region,
                        keys_per_command,
                        zipf_coefficient
                    );
                    fantoch_plot::cdf_plot_per_f(
                        searches.clone(),
                        &path,
                        &mut db,
                    )?;
                }
            }
        }
    }

    Ok(())
}

#[allow(dead_code)]
fn single_key() -> Result<(), Report> {
    // set global style
    fantoch_plot::set_global_style()?;

    // fixed parameters
    let key_gen = KeyGen::ConflictRate { conflict_rate: 10 };
    let payload_size = 4096;
    let protocols = vec![
        Protocol::NewtAtomic,
        Protocol::AtlasLocked,
        Protocol::FPaxos,
    ];

    // load results
    let mut db = ResultsDB::load(RESULTS_DIR).wrap_err("load results")?;

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

        for latency in vec![
            LatencyMetric::Average,
            LatencyMetric::Percentile(0.99),
            LatencyMetric::Percentile(0.999),
        ] {
            let suffix = if let LatencyMetric::Percentile(percentile) = latency
            {
                format!("_p{}", percentile * 100f64)
            } else {
                String::from("")
            };
            let path = format!("throughput_latency_n{}{}.pdf", n, suffix);
            // create searches
            let searches = protocol_combinations(n, protocols.clone())
                .into_iter()
                .map(|(protocol, f)| {
                    let mut search = Search::new(n, f, protocol);
                    search.key_gen(key_gen).payload_size(payload_size);
                    search
                })
                .collect();
            fantoch_plot::throughput_latency_plot(
                searches,
                n,
                clients_per_region.clone(),
                latency,
                &path,
                &mut db,
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
                let suffix = if let ErrorBar::With(percentile) = error_bar {
                    format!("_p{}", percentile * 100f64)
                } else {
                    String::from("")
                };
                let path = format!(
                    "latency_n{}_c{}{}.pdf",
                    n, clients_per_region, suffix
                );
                let results = fantoch_plot::latency_plot(
                    searches.clone(),
                    n,
                    error_bar,
                    &path,
                    &mut db,
                    extract_from_exp_data,
                )?;

                if !shown {
                    // only show results once
                    for (search, histogram) in results {
                        println!(
                            "{:<7} f = {} | {:?}",
                            PlotFmt::protocol_name(search.protocol),
                            search.f,
                            histogram
                        );
                    }
                    shown = true;
                }
            }

            // generate cdf plot
            let path = format!("cdf_n{}_c{}.pdf", n, clients_per_region);
            fantoch_plot::cdf_plot(searches.clone(), &path, &mut db)?;

            if n > 3 {
                // generate cdf plot with subplots
                let path =
                    format!("cdf_one_per_f_n{}_c{}.pdf", n, clients_per_region);
                fantoch_plot::cdf_plot_per_f(searches.clone(), &path, &mut db)?;
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

fn extract_from_exp_data(exp_data: &ExperimentData) -> Histogram {
    exp_data.global_client_latency.clone()
}
