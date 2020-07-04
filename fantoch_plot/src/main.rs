mod lib;

use lib::Matplotlib;
use pyo3::prelude::*;

fn main() -> PyResult<()> {
    let gil = Python::acquire_gil();
    let py = gil.python();
    let plt = Matplotlib::new(py)?;

    let x = vec!["us-east-1", "ca-central-1", "eu-west-2"];
    let y = vec![10, 20, 30];
    plt.plot(x, y, "o-")?;
    plt.title("latency per region")?;
    plt.xlabel("regions")?;
    plt.ylabel("latency (ms)")?;

    let kwargs = &[("format", "pdf")];
    plt.savefig("plot.pdf", Some(kwargs), py)?;
    Ok(())

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
}
