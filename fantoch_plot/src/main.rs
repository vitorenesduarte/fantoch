use color_eyre::eyre::WrapErr;
use color_eyre::Report;
use fantoch_plot::{ErrorBar, PlotFmt, ResultsDB};

// folder where all results are stored
const RESULTS_DIR: &str = "../results";

fn main() -> Result<(), Report> {
    let conflict_rate = 10;
    let payload_size = 4096;

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
        let path = format!("throughput_latency_n{}.pdf", n);
        fantoch_plot::throughput_latency_plot(
            n,
            clients_per_region,
            conflict_rate,
            payload_size,
            &path,
            &mut db,
        )?;
    }
    Ok(())
}
