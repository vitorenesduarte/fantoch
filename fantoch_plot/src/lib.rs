mod plot;
mod results_db;

use color_eyre::eyre::WrapErr;
use color_eyre::Report;
use fantoch_exp::Protocol;
use plot::Matplotlib;
use pyo3::prelude::*;
use results_db::ResultsDB;

pub fn latency_plot(
    n: usize,
    clients_per_region: usize,
    conflict_rate: usize,
    payload_size: usize,
    output_file: &str,
    results_dir: &str,
) -> Result<(), Report> {
    let db = ResultsDB::load(results_dir).wrap_err("load results")?;

    let protocols = vec![
        Protocol::NewtAtomic,
        Protocol::AtlasLocked,
        Protocol::FPaxos,
    ];
    let max_f = if n == 3 { 1 } else { 2 };

    // compute all protocol combinations
    let mut combinations = Vec::new();
    for protocol in protocols {
        for f in 1..=max_f {
            combinations.push((protocol, n, f));
        }
    }

    for (protocol, n, f) in combinations {
        let exp_data = db
            .search()
            .n(n)
            .f(f)
            .protocol(protocol)
            .clients_per_region(clients_per_region)
            .conflict_rate(conflict_rate)
            .payload_size(payload_size)
            .load()?;
        assert_eq!(exp_data.len(), 1);
    }
    Ok(())
}

pub fn single_plot() -> PyResult<()> {
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
}
