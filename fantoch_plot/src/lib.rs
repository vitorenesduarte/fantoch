mod plot;
mod results_db;

use color_eyre::eyre::WrapErr;
use color_eyre::Report;
use plot::Matplotlib;
use pyo3::prelude::*;
use results_db::ResultsDB;

pub fn plot(results_dir: &str) -> Result<(), Report> {
    let db = ResultsDB::load(results_dir).wrap_err("load results")?;

    for n in vec![3, 5] {
        let max_f = if n == 3 { 1 } else { 2 };
        for f in 1..=max_f {
            println!("n = {} | f = {}", n, f);
            for clients_per_region in
                vec![4, 8, 16, 32, 64, 128, 256, 512, 1024, 1024 * 2, 1024 * 4]
            {
                let protocols: Vec<_> = db
                    .search()
                    .n(n)
                    .f(f)
                    .clients_per_region(clients_per_region)
                    .find()
                    .map(|exp_config| exp_config.protocol)
                    .collect();
                println!("    c = {} | {:?}", clients_per_region, protocols);
            }
        }
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
