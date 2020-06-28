mod lib;

use lib::Matplotlib;
use pyo3::prelude::*;

fn main() -> PyResult<()> {
    let gil = Python::acquire_gil();
    let py = gil.python();
    let plt = Matplotlib::new(py)?;

    let x = vec!["us-east-1", "ca-central-1", "eu-west-2"];
    let y = vec![10, 20, 30];
    plt.subplot(2, 1, 1)?;
    plt.plot(x, y, "o-")?;
    plt.title("A tale of 2 subplots")?;
    plt.xlabel("regions")?;
    plt.ylabel("latency (ms)")?;

    let x = vec![8, 16, 32, 64, 128];
    let y = vec![5, 10, 18, 32, 40];
    plt.subplot(2, 1, 2)?;
    plt.plot(x, y, ".-")?;
    plt.xlabel("throughput (ops/s)")?;
    plt.ylabel("latency (ms)")?;

    let kwargs = &[("format", "pdf")];
    plt.savefig("plot.pdf", Some(kwargs), py)?;
    Ok(())
}
