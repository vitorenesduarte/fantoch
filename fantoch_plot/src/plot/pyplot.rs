use crate::plot::axes::Axes;
use crate::plot::figure::Figure;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

pub struct PyPlot<'p> {
    plt: &'p PyModule,
}

impl<'p> PyPlot<'p> {
    pub fn new(py: Python<'p>) -> PyResult<Self> {
        let plt = PyModule::import(py, "matplotlib.pyplot")?;
        Ok(Self { plt })
    }

    pub fn subplot(
        &self,
        nrows: usize,
        ncols: usize,
        index: usize,
        kwargs: Option<&PyDict>,
    ) -> PyResult<Axes<'_>> {
        let result = self.plt.call("subplot", (nrows, ncols, index), kwargs)?;
        let ax = Axes::new(result)?;
        Ok(ax)
    }

    pub fn subplots(
        &self,
        kwargs: Option<&PyDict>,
    ) -> PyResult<(Figure<'_>, Axes<'_>)> {
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

    pub fn close(&self, figure: Figure<'_>) -> PyResult<()> {
        self.plt.call1("close", (figure.fig(),))?;
        Ok(())
    }
}
