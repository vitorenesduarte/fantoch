use pyo3::prelude::*;

fn main() -> PyResult<()> {
    let gil = Python::acquire_gil();
    let py = gil.python();
    let plt = PyModule::import(py, "matplotlib.pyplot")?;

    let x = vec!["us-east-1", "ca-central-1", "eu-west-2"];
    let y = vec![10, 20, 30];
    plt.call1("subplot", (2, 1, 1))?;
    plt.call1("plot", (x, y, "o-"))?;
    plt.call1("title", ("A tale of 2 subplots",))?;
    plt.call1("ylabel", ("regions",))?;
    plt.call1("ylabel", ("latency (ms)",))?;

    let x = vec![8, 16, 32, 64, 128];
    let y = vec![5, 10, 18, 32, 40];
    plt.call1("subplot", (2, 1, 2))?;
    plt.call1("plot", (x, y, ".-"))?;
    plt.call1("xlabel", ("throughput (ops/s)",))?;
    plt.call1("ylabel", ("latency (ms)",))?;

    plt.call1("savefig", ("plot.pdf",))?;
    Ok(())
}
