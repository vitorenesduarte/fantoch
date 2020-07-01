pub mod axes;
pub mod figure;

use axes::Axes;
use figure::Figure;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

#[macro_export]
macro_rules! pytry {
    ($py:expr, $e:expr) => {{
        match $e {
            Ok(v) => v,
            Err(e) => color_eyre::eyre::bail!("{:?}", e.print($py)),
        }
    }};
}

#[macro_export]
macro_rules! pydict {
    ($py:expr, $($tup:expr),*) => {{
        #[allow(unused_mut)]
        let mut dict = pyo3::types::PyDict::new($py);
        let mut res = Ok(dict);
        $(
            let (key, value) = $tup;
            if let Err(e) = dict.set_item(key, value) {
                res = Err(e);
            }
        )*
        res
    }};
    ($py:expr, $($tup:expr,)*) => {{
        $crate::pydict![$py, $($tup),*]
    }};
}

pub struct Matplotlib<'p> {
    plt: &'p PyModule,
}

impl<'p> Matplotlib<'p> {
    pub fn new(py: Python<'p>) -> PyResult<Self> {
        let plt = PyModule::import(py, "matplotlib.pyplot")?;
        Ok(Self { plt })
    }

    pub fn subplot(
        &self,
        nrows: usize,
        ncols: usize,
        index: usize,
    ) -> PyResult<()> {
        self.plt.call1("subplot", (nrows, ncols, index))?;
        Ok(())
    }

    pub fn subplots(&self) -> PyResult<(Figure, Axes)> {
        let result = self.plt.call0("subplots")?;
        let tuple = result.downcast::<PyTuple>()?;
        let fig = Figure::new(tuple.get_item(0));
        let ax = Axes::new(tuple.get_item(1));
        Ok((fig, ax))
    }

    // TODO maybe take an optional `Fmt` struct instead
    pub fn plot<X, Y>(&self, x: Vec<X>, y: Vec<Y>, fmt: &str) -> PyResult<()>
    where
        X: IntoPy<PyObject>,
        Y: IntoPy<PyObject>,
    {
        self.plt.call1("plot", (x, y, fmt))?;
        Ok(())
    }

    pub fn title(&self, title: &str) -> PyResult<()> {
        self.plt.call1("title", (title,))?;
        Ok(())
    }

    pub fn xlabel(&self, label: &str) -> PyResult<()> {
        self.plt.call1("xlabel", (label,))?;
        Ok(())
    }

    pub fn ylabel(&self, label: &str) -> PyResult<()> {
        self.plt.call1("ylabel", (label,))?;
        Ok(())
    }

    pub fn savefig(&self, path: &str, kwargs: Option<&PyDict>) -> PyResult<()> {
        self.plt.call("savefig", (path,), kwargs)?;
        Ok(())
    }

    pub fn close(&self, figure: Figure) -> PyResult<()> {
        self.plt.call1("close", (figure.fig(),))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn save_pdf_test() {
        let path = ".test.pdf";
        if let Err(e) = save_pdf(path) {
            panic!("error while saving pdf: {:?}", e);
        }

        // check that the file was indeed created
        assert_eq!(std::path::Path::new(path).is_file(), true);
    }

    fn save_pdf(path: &str) -> PyResult<()> {
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

        let kwargs = pydict!(py, ("format", "pdf"))?;
        plt.savefig(path, Some(kwargs))?;
        Ok(())
    }
}
