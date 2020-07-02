use color_eyre::eyre::WrapErr;
use color_eyre::Report;
use fantoch::planet::{Planet, Region};
use fantoch_plot::{ErrorBar, Latency, PlotFmt, ResultsDB};

// folder where all results are stored
const RESULTS_DIR: &str = "../results";

fn main() -> Result<(), Report> {
    // set global style
    fantoch_plot::set_global_style()?;

    // fixed parameters
    let conflict_rate = 10;
    let payload_size = 4096;

    // load results
    let mut db = ResultsDB::load(RESULTS_DIR).wrap_err("load results")?;

    for n in vec![3, 5] {
        for clients_per_region in vec![
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
                let global_metrics = fantoch_plot::latency_plot(
                    n,
                    clients_per_region,
                    conflict_rate,
                    payload_size,
                    error_bar,
                    &path,
                    &mut db,
                )?;

                if !shown {
                    // only show global metrics once
                    for ((protocol, f), histogram) in global_metrics {
                        println!(
                            "{:<6} f = {} | {:?}",
                            PlotFmt::protocol_name(protocol),
                            f,
                            histogram
                        );
                    }
                    shown = true;
                }
            }

            // generate cdf plot
            let path = format!("cdf_n{}_c{}.pdf", n, clients_per_region);
            fantoch_plot::cdf_plot(
                n,
                clients_per_region,
                conflict_rate,
                payload_size,
                &path,
                &mut db,
            )?;

            if n != 3 {
                // generate cdf plot with subplots
                let path =
                    format!("cdf_one_per_f_n{}_c{}.pdf", n, clients_per_region);
                fantoch_plot::cdf_plots(
                    n,
                    clients_per_region,
                    conflict_rate,
                    payload_size,
                    &path,
                    &mut db,
                )?;
            }
        }

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
            Latency::Average,
            Latency::Percentile(0.99),
            Latency::Percentile(0.999),
        ] {
            let suffix = if let Latency::Percentile(percentile) = latency {
                format!("_p{}", percentile * 100f64)
            } else {
                String::from("")
            };
            let path = format!("throughput_latency_n{}{}.pdf", n, suffix);
            fantoch_plot::throughput_latency_plot(
                n,
                clients_per_region.clone(),
                conflict_rate,
                payload_size,
                latency,
                &path,
                &mut db,
            )?;
        }
    }

    // show distance matrix
    let planet = Planet::from("../latency_aws/");
    let regions = vec![
        Region::new("eu-west-1"),
        Region::new("us-west-1"),
        Region::new("ap-southeast-1"),
        Region::new("ca-central-1"),
        Region::new("sa-east-1"),
    ];
    println!("{}", planet.distance_matrix(regions.clone()).unwrap());

    Ok(())
}
