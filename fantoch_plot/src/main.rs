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
        for clients in vec![8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096] {
            println!("n = {} | c = {}", n, clients);

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
                let path = format!("latency_n{}_c{}{}.pdf", n, clients, suffix);
                let global_metrics = fantoch_plot::latency_plot(
                    n,
                    clients,
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
            let path = format!("cdf_n{}_c{}.pdf", n, clients);
            fantoch_plot::cdf_plot(
                n,
                clients,
                conflict_rate,
                payload_size,
                &path,
                &mut db,
            )?;
        }
    }
    Ok(())
}

/*
let metrics =
    vec!["min", "max", "avg", "p95", "p99", "p99.9", "p99.99"];
let mut latencies_to_avg = HashMap::new();

// region latency should be something like:
// - "min=173   max=183   avg=178   p95=183   p99=183 p99.9=183
//   p99.99=183"
for (region, region_latency) in latencies {
    let region_latency = region_latency
        .strip_prefix("latency: ")
        .expect("client output should start with 'latency: '");
    let line = format!(
        "region = {:<14} | {}",
        region.name(),
        region_latency
    );
    append_to_output_log(&mut output_log, line).await?;

    for entry in region_latency.split_whitespace() {
        // entry should be something like:
        // - "min=78"
        let parts: Vec<_> = entry.split("=").collect();
        assert_eq!(parts.len(), 2);
        let metric = parts[0].to_string();
        let latency = parts[1].parse::<usize>()?;
        latencies_to_avg
            .entry(metric)
            .or_insert_with(Vec::new)
            .push(latency);
    }
}

let mut line =
    format!("n = {} AND c = {:<9} |", config.n(), clients);
for metric in metrics {
    let latencies = latencies_to_avg
        .remove(metric)
        .expect("metric should exist");
    // there should be as many latencies as regions
    assert_eq!(latencies.len(), machines.regions.len());
    let avg = latencies.into_iter().sum::<usize>()
        / machines.regions.len();
    line = format!("{} {}={:<6}", line, metric, avg);
}
append_to_output_log(&mut output_log, line).await?;
*/
