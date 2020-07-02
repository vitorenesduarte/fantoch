pub mod axes;
pub mod axis;
pub mod figure;
pub mod ticker;

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

pub struct PyPlot<'p> {
    plt: &'p PyModule,
}

impl<'p> PyPlot<'p> {
    pub fn new(py: Python<'p>) -> PyResult<Self> {
        let plt = PyModule::import(py, "matplotlib.pyplot")?;
        Ok(Self { plt })
    }

    // Get current figure.
    pub fn gcf(&self) -> PyResult<Figure> {
        let fig = self.plt.call0("gcf")?;
        Ok(Figure::new(fig))
    }

    pub fn subplot(
        &self,
        nrows: usize,
        ncols: usize,
        index: usize,
        kwargs: Option<&PyDict>,
    ) -> PyResult<Axes> {
        let result = self.plt.call("subplot", (nrows, ncols, index), kwargs)?;
        let ax = Axes::new(result)?;
        Ok(ax)
    }

    pub fn subplots(
        &self,
        kwargs: Option<&PyDict>,
    ) -> PyResult<(Figure, Axes)> {
        // check that `ncols` and `nrows` was not set
        if let Some(kwargs) = kwargs {
            assert_eq!(
                kwargs.get_item("ncols"),
                None,
                "ncols shouldn't be set here; use `PyPlot::subplot` instead"
            );
            assert_eq!(
                kwargs.get_item("nrows"),
                None,
                "nrows shouldn't be set here; use `PyPlot::subplot` instead"
            );
        }
        let result = self.plt.call("subplots", (), kwargs)?;
        let tuple = result.downcast::<PyTuple>()?;
        let fig = Figure::new(tuple.get_item(0));
        let ax = Axes::new(tuple.get_item(1))?;
        Ok((fig, ax))
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
        let plt = PyPlot::new(py)?;

        let x = vec!["us-east-1", "ca-central-1", "eu-west-2"];
        let y = vec![10, 20, 30];
        let (fig, ax) = plt.subplots(None)?;
        ax.plot(x, y, Some("o-"), None)?;
        ax.set_xlabel("regions")?;
        ax.set_ylabel("latency (ms)")?;

        let kwargs = pydict!(py, ("format", "pdf"))?;
        plt.savefig(path, Some(kwargs))?;
        plt.close(fig)?;
        Ok(())
    }
}
