use color_eyre::eyre;
use color_eyre::Report;

// folder where all results are stored
const RESULTS_DIR: &str = "../results";

fn main() -> Result<(), Report> {
    if let Err(e) = fantoch_plot::single_plot() {
        eyre::bail!("{:?}", e);
    }

    let conflict_rate = 10;
    let payload_size = 4096;

    for n in vec![3, 5] {
        for clients in vec![128, 1024] {
            let path = format!("latency_n{}_c{}.pdf", n, clients);
            fantoch_plot::latency_plot(
                n,
                clients,
                conflict_rate,
                payload_size,
                &path,
                RESULTS_DIR,
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
