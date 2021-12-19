use crate::plot::axes::Axes;
use crate::plot::figure::Figure;
use crate::plot::table::Table;
use crate::pytry;
use color_eyre::Report;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use pyo3::PyNativeType;

pub struct PyPlot<'p> {
    plt: &'p PyModule,
}

impl<'p> PyPlot<'p> {
    pub fn new(py: Python<'p>) -> Result<Self, Report> {
        let plt = pytry!(py, PyModule::import(py, "matplotlib.pyplot"));
        Ok(Self { plt })
    }

    pub fn subplot<I>(
        &self,
        nrows: usize,
        ncols: usize,
        index: I,
        kwargs: Option<&PyDict>,
    ) -> Result<Axes<'_>, Report>
    where
        I: IntoPy<PyObject>,
    {
        let result = pytry!(
            self.py(),
            self.plt
                .getattr("subplot")?
                .call((nrows, ncols, index), kwargs)
        );
        let ax = Axes::new(result)?;
        Ok(ax)
    }

    pub fn subplots(
        &self,
        kwargs: Option<&PyDict>,
    ) -> Result<(Figure<'_>, Axes<'_>), Report> {
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
        let result =
            pytry!(self.py(), self.plt.getattr("subplots")?.call((), kwargs));
        let tuple = pytry!(self.py(), result.downcast::<PyTuple>());
        let fig =
            Figure::new(tuple.get_item(0).expect("subplots fig should be set"));
        let ax =
            Axes::new(tuple.get_item(1).expect("subplots ax should be set"))?;
        Ok((fig, ax))
    }

    pub fn subplots_adjust(
        &self,
        kwargs: Option<&PyDict>,
    ) -> Result<(), Report> {
        pytry!(
            self.py(),
            self.plt.getattr("subplots_adjust")?.call((), kwargs)
        );
        Ok(())
    }

    pub fn table(&self, kwargs: Option<&PyDict>) -> Result<Table<'_>, Report> {
        let result =
            pytry!(self.py(), self.plt.getattr("table")?.call((), kwargs));
        let table = Table::new(result);
        Ok(table)
    }

    pub fn axis(&self, option: &str) -> Result<(), Report> {
        pytry!(self.py(), self.plt.getattr("axis")?.call1((option,)));
        Ok(())
    }

    pub fn text(
        &self,
        x: f64,
        y: f64,
        text: &str,
        kwargs: Option<&PyDict>,
    ) -> Result<(), Report> {
        pytry!(
            self.py(),
            self.plt.getattr("text")?.call((x, y, text), kwargs)
        );
        Ok(())
    }

    pub fn savefig(
        &self,
        path: &str,
        kwargs: Option<&PyDict>,
    ) -> Result<(), Report> {
        pytry!(
            self.py(),
            self.plt.getattr("savefig")?.call((path,), kwargs)
        );
        Ok(())
    }

    pub fn close(&self, kwargs: Option<&PyDict>) -> Result<(), Report> {
        pytry!(self.py(), self.plt.getattr("close")?.call((), kwargs));
        Ok(())
    }

    pub fn tight_layout(&self) -> Result<(), Report> {
        pytry!(self.py(), self.plt.getattr("tight_layout")?.call0());
        Ok(())
    }

    fn py(&self) -> Python<'_> {
        self.plt.py()
    }
}
